package main

import (
	"context"
	"time"

	"flag"
	"github.com/spf13/cobra"
	pflag "github.com/spf13/pflag"
	"math/rand"

	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/practo/klog/v2"
	conf "github.com/practo/tipoca-stream/redshiftsink/cmd/redshiftloader/config"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/consumer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshiftloader"
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

	ctx, cancel := context.WithCancel(context.Background())

	// Redshift connections is shared by all topics in all routines
	redshifter, err := redshift.NewRedshift(ctx, config.Redshift)
	if err != nil {
		klog.Fatalf("Error creating redshifter: %v\n", err)
	}

	var consumerGroups map[string]consumer.ConsumerGroupInterface
	ready := make(chan bool)
	wg := &sync.WaitGroup{}

	for _, groupConfig := range config.ConsumerGroups {
		consumerGroup, err := consumer.NewConsumerGroup(
			groupConfig,
			redshiftloader.NewConsumer(ready, redshifter),
		)
		if err != nil {
			klog.Errorf("Error making kafka consumer group, exiting: %v\n", err)
			os.Exit(1)
		}
		groupID := groupConfig.GroupID
		consumerGroups[groupID] = consumerGroup
		klog.Infof("Succesfully created kafka client for group: %s", groupID)
		manager := consumer.NewManager(
			consumerGroup,
			groupID,
			groupConfig.TopicRegexes,
			cancel,
			config.Reload,
		)
		wg.Add(1)
		go manager.SyncTopics(ctx, 15, wg)
		wg.Add(1)
		go manager.Consume(ctx, wg)
	}

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

	// TODO: the processing function should signal back
	// It does not at present
	// https://github.com/practo/tipoca-stream/issues/18
	klog.Info("Waiting the loader goroutines to gracefully shutdown")
	time.Sleep(10 * time.Second)

	wg.Wait()

	var closeErr error
	for groupID, consumerGroup := range consumerGroups {
		klog.Infof("Closing consumerGroup: %s", groupID)
		closeErr = consumerGroup.Close()
		if closeErr != nil {
			klog.Errorf(
				"Error closing consumer group: %s, err: %v", groupID, err)
		}
	}
	if closeErr != nil {
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
