package main

import (
	"context"
	"time"

	"flag"
	"github.com/spf13/cobra"
	pflag "github.com/spf13/pflag"

	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/practo/klog/v2"
	conf "github.com/practo/tipoca-stream/kafka-go/cmd/redshiftloader/config"
	"github.com/practo/tipoca-stream/kafka-go/pkg/consumer"
	"github.com/practo/tipoca-stream/kafka-go/pkg/redshiftloader"
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

func run(cmd *cobra.Command, args []string) {
	klog.Info("Starting the redshift loader")

	config, err := conf.LoadConfig(cmd)
	if err != nil {
		klog.Errorf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	ready := make(chan bool)
	consumerGroup, err := consumer.NewConsumerGroup(
		config.Kafka, config.Sarama, redshiftloader.NewConsumer(ready),
	)
	if err != nil {
		klog.Errorf("Error creating kafka consumer group, exiting: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	klog.Info("Succesfully created kafka client")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	manager := consumer.NewManager(
		consumerGroup,
		config.Kafka.TopicRegexes,
		sigterm,
	)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go manager.SyncTopics(ctx, 15, wg)
	wg.Add(1)
	go manager.Consume(ctx, wg)

	<-ready
	klog.Info("Consumer is up and running")

	select {
	case <-ctx.Done():
		klog.Info("Context cancelled, bye bye!")
	case <-sigterm:
		klog.Info("Sigterm signal received")
	}
	cancel()

	// TODO: the processing batching function should signal back
	// It does not at present
	// https://github.com/practo/tipoca-stream/issues/18
	klog.Info("Waiting the batcher processees to gracefully shutdown")
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
	rootCmd.Execute()
}
