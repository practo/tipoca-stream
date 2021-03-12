package main

import (
	"context"
	"time"

	"flag"
	"github.com/spf13/cobra"
	pflag "github.com/spf13/pflag"
	"math/rand"
	"net/http"

	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/practo/klog/v2"
	conf "github.com/practo/tipoca-stream/redshiftsink/cmd/redshiftloader/config"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshiftloader"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var rootCmd = &cobra.Command{
	Use:   "redshiftloader",
	Short: "Loads the uploaded batch of debezium events to redshift.",
	Long:  "Loads the uploaded batch of debezium events to redshift.",
	Run:   run,
}

func init() {
	klog.InitFlags(nil)
	rootCmd.PersistentFlags().String("config", "./cmd/redshiftloader/config/config.yaml", "config file")
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
	klog.Info("Starting the redshift loader")
	go serveMetrics()

	config, err := conf.LoadConfig(cmd)
	if err != nil {
		klog.Errorf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Defaults s3 credentials to same as provided for the loader s3sink
	if config.Redshift.S3AccessKeyId == "" {
		config.Redshift.S3AccessKeyId = config.S3Sink.AccessKeyId
	}
	if config.Redshift.S3SecretAccessKey == "" {
		config.Redshift.S3SecretAccessKey = config.S3Sink.SecretAccessKey
	}

	// Redshift connections is shared by all topics in all routines
	redshifter, err := redshift.NewRedshift(config.Redshift)
	if err != nil {
		klog.Fatalf("Error creating redshifter: %v\n", err)
	}

	schema := config.Redshift.Schema
	schemaExist, err := redshifter.SchemaExist(ctx, schema)
	if err != nil {
		klog.Fatalf("Error querying schema exists, err: %v\n", err)
	}
	if !schemaExist {
		err = redshifter.CreateSchema(ctx, schema)
		if err != nil {
			exist, err2 := redshifter.SchemaExist(ctx, schema)
			if err2 != nil {
				klog.Fatalf("Error checking schema exist, err: %v\n", err2)
			}
			if !exist {
				klog.Fatalf("Error creating schema, err: %v\n", err)
			}
		} else {
			klog.Infof("Created Redshift schema: %s", schema)
		}
	}

	consumerGroups := make(map[string]kafka.ConsumerGroupInterface)
	var consumersReady []chan bool
	wg := &sync.WaitGroup{}

	for _, groupConfig := range config.ConsumerGroups {
		ready := make(chan bool)
		groupID := groupConfig.GroupID
		consumerGroup, err := kafka.NewConsumerGroup(
			groupConfig,
			redshiftloader.NewHandler(
				ctx,
				ready,
				groupID,
				config.Loader,
				groupConfig.Sarama,
				redshifter,
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
			// cancel,
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

	klog.V(2).Info("Cancelling context to trigger graceful shutdown...")
	cancel()

	klog.V(2).Info("Waiting for waitgroups to shutdown...")
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
