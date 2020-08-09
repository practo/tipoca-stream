package main

import (
	"context"
	"flag"
	pflag "github.com/spf13/pflag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftbatcher/pkg/consumer"
)

// Sarama configuration options
var (
	brokers        = ""
	version        = ""
	group          = ""
	topicPrefixes  = ""
	kafkaClient    = ""
	saramaAssignor = ""
	saramaOldest   = true
	saramaLog      = false
)

func init() {
	klog.InitFlags(nil)

	flag.StringVar(&brokers, "brokers", "", "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&group, "group", "", "Kafka consumer group definition")
	flag.StringVar(&version, "version", "2.1.1", "Kafka cluster version")
	flag.StringVar(&topicPrefixes, "topic-prefixes", "", "Kafka topics to be consumed, as a comma separated list")
	flag.StringVar(&kafkaClient, "kafka-client", "sarama", "Kafka client to use: kafka-go or sarama")

	// sarama specifc flags
	flag.StringVar(&saramaAssignor, "sarama-assignor", "range", "Consumer group partition assignment strategy (range, roundrobin, sticky)")
	flag.BoolVar(&saramaOldest, "sarama-oldest", true, "Kafka consumer consume initial offset from oldest")
	flag.BoolVar(&saramaLog, "sarama-log", false, "Enable or disable sarama client logging")

	flag.Parse()

	if len(brokers) == 0 {
		klog.Fatal("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}

	if len(topicPrefixes) == 0 {
		klog.Fatal("no topicPrefixes given to be consumed, please set the -topicPrefixes flag")
	}

	if len(group) == 0 {
		klog.Fatal("no Kafka consumer group defined, please set the -group flag")
	}

	if kafkaClient != consumer.KafkaGo && kafkaClient != consumer.Sarama {
		klog.Fatalf("supported kafka clients are: %s and %s\n",
			consumer.KafkaGo, consumer.Sarama)
	}

	pflag.CommandLine.AddGoFlag(flag.CommandLine.Lookup("v"))
}

func main() {
	klog.Info("Starting the redshift batcher")

	client, err := consumer.NewClient(
		kafkaClient, brokers, group, version,
		saramaLog, saramaAssignor, saramaOldest,
	)
	if err != nil {
		klog.Fatalf("Error creating kafka consumer client: %v\n", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	klog.Info("Succesfully created kafka client")

	manager := consumer.NewManager(client, topicPrefixes)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go manager.SyncTopics(ctx, 15, wg)
	wg.Add(1)
	go manager.Consume(ctx, wg)

	<-manager.Ready // Await till the consumer has been set up
	klog.Info("Consumer is up and running")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		klog.Info("Exiting. Context cancelled")
	case <-sigterm:
		klog.Info("Exiting. SIGTERM signal received")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}
