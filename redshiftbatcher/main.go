package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/practo/klog/v2"
	"github.com/spf13/pflag"
	"github.com/practo/tipoca-stream/redshiftbatcher/pkg/consumer"
)

// Sarama configuration options
var (
	brokers  = ""
	version  = ""
	group    = ""
	topics   = ""
	assignor = ""
	oldest   = true
	verbose  = false
)

func init() {
	klog.InitFlags(nil)

	flag.StringVar(&brokers, "brokers", "", "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&group, "group", "", "Kafka consumer group definition")
	flag.StringVar(&version, "version", "2.1.1", "Kafka cluster version")
	flag.StringVar(&topicPrefixes, "topicPrefixes", "", "Kafka topics to be consumed, as a comma separated list")
	flag.StringVar(&assignor, "assignor", "range", "Consumer group partition assignment strategy (range, roundrobin, sticky)")
	flag.BoolVar(&oldest, "oldest", true, "Kafka consumer consume initial offset from oldest")
	flag.BoolVar(&clientlog, "clientlog", false, "client logging")
	flag.Parse()

	if len(brokers) == 0 {
		klog.Fatal("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}

	if len(topics) == 0 {
		klog.Fatal("no topics given to be consumed, please set the -topics flag")
	}

	if len(group) == 0 {
		klog.Fatal("no Kafka consumer group defined, please set the -group flag")
	}

	pflag.CommandLine.AddGoFlag(flag.CommandLine.Lookup("v"))
}

func main() {
	klog.Info("Starting the redshift batcher")

	ctx, cancel := context.WithCancel(context.Background())

	client, err := NewClient(
		brokers, group, clientlog, version, assignor, oldest)
	if err != nil {
		klog.Fatal("Error creating kafka consumer client")
	}

	manager := NewManager(client, topicPrefixes)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go manager.RefreshTopics(ctx, 15, wg)
	wg.Add(1)
	go manager.Consume(ctx, wg)

	cancel()
	wg.Wait()

	// TODO: handle signal shutdowns
}
