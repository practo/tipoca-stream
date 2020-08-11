package main

import (
	"context"

	"flag"
	"github.com/spf13/cobra"
	pflag "github.com/spf13/pflag"

	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/practo/klog/v2"
	conf "github.com/practo/tipoca-stream/redshiftsink/cmd/redshiftbatcher/config"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/consumer"
)

var rootCmd = &cobra.Command{
	Use:   "redshiftbatcher",
	Short: "Batches the debezium data, uploads to s3 and signals the load of the batch.",
	Long:  "Consumes the Kafka Topics, trasnform them for redshfit, batches them and uploads to s3. Also signals the load of the batch on successful batch and upload operation..",
	Run:   run,
}

func init() {
	klog.InitFlags(nil)
	rootCmd.PersistentFlags().String("config", "config.yaml", "config file")
	pflag.CommandLine.AddGoFlag(flag.CommandLine.Lookup("v"))
}

func run(cmd *cobra.Command, args []string) {
	klog.Info("Starting the redshift batcher")

	config, err := conf.LoadConfig(cmd)
	if err != nil {
		klog.Errorf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	client, err := consumer.NewClient(config.Kafka, config.Sarama)
	if err != nil {
		klog.Errorf("Error creating kafka consumer client, exiting: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	klog.Info("Succesfully created kafka client")

	manager := consumer.NewManager(
		client,
		config.Kafka.TopicPrefixes,
	)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go manager.SyncTopics(ctx, 15, wg)
	wg.Add(1)
	go manager.Consume(ctx, wg)

	<-manager.Ready
	klog.Info("Consumer is up and running")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		klog.Info("Context cancelled, bye bye!")
	case <-sigterm:
		klog.Info("Sigterm signal received, Goodbye till will meet again!")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		klog.Errorf("Error closing client: %v", err)
		os.Exit(1)
	}
}

func main() {
	rootCmd.Execute()
}
