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
	conf "github.com/practo/tipoca-stream/redshiftsink/cmd/redshiftbatcher/config"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/consumer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshiftbatcher"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var rootCmd = &cobra.Command{
	Use:   "redshiftbatcher",
	Short: "Batches the debezium data, uploads to s3 and signals the load of the batch.",
	Long:  "Consumes the Kafka Topics, trasnform them for redshfit, batches them and uploads to s3. Also signals the load of the batch on successful batch and upload operation..",
	Run:   run,
}

func init() {
	klog.InitFlags(nil)
	rootCmd.PersistentFlags().String("config", "./cmd/redshiftbatcher/config/config.yaml", "config file")
	pflag.CommandLine.AddGoFlag(flag.CommandLine.Lookup("v"))
}

func serveMetrics() {
	klog.Info("Starting prometheus metric endpoint")
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

	ready := make(chan bool)
	consumerGroup, err := consumer.NewConsumerGroup(
		config.Kafka, config.Sarama, redshiftbatcher.NewConsumer(ready),
	)
	if err != nil {
		klog.Errorf("Error creating kafka consumer group, exiting: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	klog.Info("Succesfully created kafka client")

	manager := consumer.NewManager(
		consumerGroup,
		config.Kafka.TopicRegexes,
		cancel,
		config.Reload,
	)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go manager.SyncTopics(ctx, 15, wg)
	wg.Add(1)
	go manager.Consume(ctx, wg)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		klog.Info("Context cancelled, bye bye!")
	case <-sigterm:
		klog.Info("Sigterm signal received")
		cancel()
	case <-ready:
		klog.Info("Consumer is up and running")
	}

	select {
	case <-ctx.Done():
		klog.Info("Context cancelled, bye bye!")
	case <-sigterm:
		klog.Info("Sigterm signal received")
		cancel()
	}

	// TODO: the processing batching function should signal back
	// It does not at present
	// https://github.com/practo/tipoca-stream/issues/18
	klog.Info("Waiting the batcher goroutines to gracefully shutdown")
	time.Sleep(5 * time.Second)

	wg.Wait()
	if err = consumerGroup.Close(); err != nil {
		klog.Errorf("Error closing group: %v", err)
		os.Exit(1)
	}

	klog.Info("Goodbye!")
}

// main/main.main()
// => consumer/manager.Consume() => consumer/consumer_group.Consume()
// => sarama/consumer_group.Consume() => redshfitbatcher/consumer.ConsumeClaim()
func main() {
	rand.Seed(time.Now().UnixNano())

	rootCmd.Execute()
}
