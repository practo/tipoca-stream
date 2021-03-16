package main

import (
	"context"
	"time"

	"flag"
	"github.com/spf13/cobra"
	pflag "github.com/spf13/pflag"
	"github.com/spf13/viper"
	"math/rand"
	"net/http"

	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/practo/klog/v2"
	conf "github.com/practo/tipoca-stream/redshiftsink/cmd/redshiftbatcher/config"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshiftbatcher"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer/masker"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var rootCmd = &cobra.Command{
	Use:   "redshiftbatcher",
	Short: "Batches the data, uploads to s3 and signals the load of the batch.",
	Long:  "Consumes the Kafka Topics, trasnform them for redshfit, batches them and uploads to s3. Also signals the load of the batch on successful batch and upload operation..",
	Run:   run,
}

func init() {
	klog.InitFlags(nil)
	rootCmd.PersistentFlags().String("config", "./cmd/redshiftbatcher/config/config.yaml", "config file")
	pflag.CommandLine.AddGoFlag(flag.CommandLine.Lookup("v"))
}

func serveMetrics() {
	klog.V(2).Info("Starting prometheus metric endpoint")
	http.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":8787", nil)
}

func run(cmd *cobra.Command, args []string) {
	klog.Info("Starting the redshift batcher")
	go serveMetrics()

	config, err := conf.LoadConfig(cmd)
	if err != nil {
		klog.Errorf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	// maskConfig is central to all the topics in the redshiftsink resource
	// since the maskFile is at database level. Also centralizing
	// it to keep it clean and make the git pull be a centarl one time activity
	maskConfig, err := masker.NewMaskConfig(
		"/",
		viper.GetString("batcher.maskFile"),
		viper.GetString("batcher.maskFileVersion"),
		viper.GetString("gitAccessToken"),
	)
	if err != nil {
		klog.Errorf("Error loading mask config: %v", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	consumerGroups := make(map[string]kafka.ConsumerGroupInterface)
	var consumersReady []chan bool
	wg := &sync.WaitGroup{}
	for _, groupConfig := range config.ConsumerGroups {
		ready := make(chan bool)
		groupID := groupConfig.GroupID
		consumerGroup, err := kafka.NewConsumerGroup(
			groupConfig,
			redshiftbatcher.NewHandler(
				ready,
				ctx,
				groupID,
				config.Batcher,
				groupConfig.Kafka,
				groupConfig.Sarama,
				maskConfig,
				groupConfig.LoaderTopicPrefix,
			),
		)
		if err != nil {
			klog.Errorf("Error making kafka consumer group, exiting: %v\n", err)
			os.Exit(1)
		}
		consumersReady = append(consumersReady, ready)
		consumerGroups[groupID] = consumerGroup
		klog.V(2).Infof("Kafka client created for group: %s", groupID)
		manager := kafka.NewManager(
			consumerGroup,
			groupID,
			groupConfig.TopicRegexes,
		)
		wg.Add(1)
		go manager.SyncTopics(ctx, wg)
		wg.Add(1)
		go manager.Consume(ctx, wg)
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	ready := 0

	klog.V(2).Infof("ConsumerGroups: %v", len(consumersReady))
	for ready >= 0 {
		select {
		default:
		case <-sigterm:
			klog.V(2).Info("SIGTERM signal received")
			ready = -1
		}

		if ready == -1 || ready == len(consumersReady) {
			time.Sleep(3 * time.Second)
			continue
		}

		for _, channel := range consumersReady {
			select {
			case <-channel:
				ready += 1
				klog.V(2).Infof("ConsumerGroup #%d is up and running", ready)
			}
		}
	}

	klog.V(2).Info("Cancelled main context")
	cancel()

	klog.V(2).Info("Waiting for all goroutines to shutdown...")
	wg.Wait()

	var closeErr error
	for groupID, consumerGroup := range consumerGroups {
		klog.V(2).Infof("Closing consumerGroup: %s", groupID)
		closeErr = consumerGroup.Close()
		if closeErr != nil {
			klog.Errorf(
				"Error closing consumer group: %s, err: %v", groupID, err)
		}
	}
	if closeErr != nil {
		os.Exit(1)
	}

	klog.V(1).Info("Goodbye!")
}

func main() {
	rand.Seed(time.Now().UnixNano())

	rootCmd.Execute()
}
