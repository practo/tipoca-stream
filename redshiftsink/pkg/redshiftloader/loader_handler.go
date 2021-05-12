package redshiftloader

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/prometheus"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"github.com/spf13/viper"
	"sync"
	"time"
)

var (
	DefaultMaxWaitSeconds    int     = 1800
	DefaultMaxProcessingTime int32   = 600000
	MaxRunningLoaders        float64 = 10
	ThrottlingBudget         int     = 10
	FirstThrottlingBudget    int     = 120
)

type LoaderConfig struct {
	// Maximum size of a batch, on exceeding this batch is pushed
	// regarless of the wait time.
	// Deprecated: in favour of MaxBytesPerBatch
	MaxSize int `yaml:"maxSize,omitempty"`

	// MaxBytesPerBatch is the maximum bytes per batch. It is not the size
	// of kafka message but the size of all the messages that would be
	// loaded in the batch. Default is there
	// if the user has not specified a default will be applied.
	// If this is specified, maxSize specification is not considered.
	// Default would be specified after MaxSize is gone
	MaxBytesPerBatch *int64 `yaml:"maxBytesPerBatch,omitempty"`

	// MaxWaitSeconds after which the bash would be pushed regardless of its size.
	MaxWaitSeconds *int `yaml:"maxWaitSeconds,omitempty"`
}

// loaderHandler is the sarama consumer handler
// loaderHandler.ConsumeClaim() is called for every topic partition
type loaderHandler struct {
	ready chan bool
	ctx   context.Context

	consumerGroupID string

	maxSize int // Deprecated

	maxWaitSeconds   *int
	maxBytesPerBatch *int64

	saramaConfig  kafka.SaramaConfig
	redshifter    *redshift.Redshift
	redshiftGroup *string
	serializer    serializer.Serializer

	// prometheusClient is used to query the running loaders
	// so that concurrency of load can be maintained below a threshold
	prometheusClient prometheus.Client

	// loadRunning is used for two purpose
	// 1. to track total running loaders
	// 2. to allow more throttling seconds in case of first load
	loadRunning *sync.Map
}

func NewHandler(
	ctx context.Context,
	ready chan bool,
	consumerGroupID string,
	loaderConfig LoaderConfig,
	saramaConfig kafka.SaramaConfig,
	redshifter *redshift.Redshift,
	redshiftGroup *string,
) *loaderHandler {
	// apply defaults
	if loaderConfig.MaxWaitSeconds == nil {
		loaderConfig.MaxWaitSeconds = &DefaultMaxWaitSeconds
	}

	var err error
	var prometheusClient prometheus.Client
	prometheusURL := viper.GetString("prometheusURL")
	if prometheusURL != "" {
		prometheusClient, err = prometheus.NewClient(prometheusURL)
		if err != nil {
			klog.Fatalf("Error initializing prometheus client, err: %v", err)
		}
	}

	return &loaderHandler{
		ready: ready,
		ctx:   ctx,

		consumerGroupID: consumerGroupID,

		maxSize: loaderConfig.MaxSize, // Deprecated

		maxWaitSeconds:   loaderConfig.MaxWaitSeconds,
		maxBytesPerBatch: loaderConfig.MaxBytesPerBatch,

		saramaConfig:     saramaConfig,
		redshifter:       redshifter,
		redshiftGroup:    redshiftGroup,
		serializer:       serializer.NewSerializer(viper.GetString("schemaRegistryURL")),
		prometheusClient: prometheusClient,
		loadRunning:      new(sync.Map),
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *loaderHandler) Setup(sarama.ConsumerGroupSession) error {
	klog.V(1).Info("setting up handler")

	// Mark the consumer as ready
	select {
	case <-h.ready:
		return nil
	default:
	}
	close(h.ready)

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *loaderHandler) Cleanup(sarama.ConsumerGroupSession) error {
	klog.V(1).Info("cleaning up handler")
	return nil
}

func (h *loaderHandler) throttle(topic string) error {
	// never throttle if promtheus client is not set
	// this makes throttling using prometheus an addon feature
	if h.prometheusClient == nil {
		klog.V(2).Infof("%s: promtheus is not enabled, throttling feature disabled", topic)
		return nil
	}

	localLoadRunning := 0.0
	h.loadRunning.Range(func(key, value interface{}) bool {
		running := value.(bool)
		if running {
			localLoadRunning += 1
		}
		return true
	})
	klog.V(2).Infof("%s: running loaders(local): %v", topic, localLoadRunning)

	throttleBudget := FirstThrottlingBudget
	_, ok := h.loadRunning.Load(topic)
	if ok {
		throttleBudget = ThrottlingBudget
	}

	for i := 0; i < throttleBudget; i++ {
		runningLoaders, err := h.prometheusClient.Query("sum(rsk_loader_running > 0)")
		if err != nil {
			return err
		}
		klog.V(2).Infof("%s: running loaders(metric): %v", topic, runningLoaders)

		if (runningLoaders <= MaxRunningLoaders) && (localLoadRunning <= MaxRunningLoaders) {
			return nil
		}

		klog.V(2).Infof("%s: throttled for 15s", topic)
		time.Sleep(15 * time.Second)
	}

	klog.V(2).Infof("%s: throttle budget exhausted, go load!", topic)

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// ConsumeClaim is managed by the consumer.manager routine
func (h *loaderHandler) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {

	klog.V(1).Infof(
		"%s: consumeClaim started, initalOffset:%d\n",
		claim.Topic(),
		claim.InitialOffset(),
	)

	var lastSchemaId *int
	var err error
	processor, err := newLoadProcessor(
		h.consumerGroupID,
		claim.Topic(),
		claim.Partition(),
		h.saramaConfig,
		h.redshifter,
		h.redshiftGroup,
	)
	if err != nil {
		return fmt.Errorf(
			"Error making the load processor for topic: %s, err: %v",
			claim.Topic(), err)
	}
	maxBufSize := h.maxSize
	if h.maxBytesPerBatch != nil {
		maxBufSize = serializer.DefaultMessageBufferSize
	}
	msgBatch := serializer.NewMessageSyncBatch(
		claim.Topic(),
		claim.Partition(),
		h.maxSize, // Deprecated
		maxBufSize,
		h.maxBytesPerBatch,
		processor,
	)
	maxWaitTicker := time.NewTicker(
		time.Duration(*h.maxWaitSeconds) * time.Second,
	)

	klog.V(4).Infof("%s: read msgs", claim.Topic())
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	claimMsgChan := claim.Messages()
	for {
		select {
		case <-h.ctx.Done():
			klog.V(2).Infof(
				"%s: consumeClaim returning, main ctx done",
				claim.Topic(),
			)
			return nil
		case <-session.Context().Done():
			klog.V(2).Infof(
				"%s: consumeClaim returning. session ctx done, ctxErr: %v",
				claim.Topic(),
				session.Context().Err(),
			)
			return fmt.Errorf("session ctx done, err: %v", session.Context().Err())
		case message, ok := <-claimMsgChan:
			if !ok {
				klog.V(2).Infof(
					"%s: consumeClaim returning. read msg channel closed",
					claim.Topic(),
				)
				return nil
			}
			select {
			default:
			case <-h.ctx.Done():
				klog.V(2).Infof(
					"%s: consumeClaim returning, main ctx done",
					claim.Topic(),
				)
				return nil
			case <-session.Context().Done():
				klog.V(2).Infof(
					"%s: consumeClaim returning. session ctx done, ctxErr: %v",
					claim.Topic(),
					session.Context().Err(),
				)
				return fmt.Errorf("session ctx done, err: %v", session.Context().Err())
			}

			// Deserialize the message
			msg, err := h.serializer.Deserialize(message)
			if err != nil {
				klog.Fatalf("Error deserializing binary, err: %s\n", err)
			}
			if msg == nil || msg.Value == nil {
				klog.Fatalf("Got message as nil, message: %+v\n", msg)
			}

			// Process the batch by schemaID
			job := StringMapToJob(msg.Value.(map[string]interface{}))
			upstreamJobSchemaId := job.SchemaId
			if lastSchemaId == nil {
				lastSchemaId = new(int)
			} else if *lastSchemaId != upstreamJobSchemaId {
				klog.V(2).Infof(
					"%s: schema changed, %d => %d (batch flush)\n",
					claim.Topic(),
					*lastSchemaId,
					upstreamJobSchemaId,
				)
				maxWaitTicker.Stop()
				err = msgBatch.Process(session)
				maxWaitTicker.Reset(time.Duration(*h.maxWaitSeconds) * time.Second)
				if err != nil {
					return err
				}
			}

			// Process the batch by size hit
			msgBatch.Insert(msg)
			if msgBatch.SizeHit(job.BatchBytes) {
				klog.V(2).Infof(
					"%s: maxSize hit",
					msg.Topic,
				)
				maxWaitTicker.Stop()
				err = h.throttle(claim.Topic())
				if err != nil {
					return err
				}
				h.loadRunning.Store(claim.Topic(), true)
				err = msgBatch.Process(session)
				maxWaitTicker.Reset(time.Duration(*h.maxWaitSeconds) * time.Second)
				if err != nil {
					h.loadRunning.Store(claim.Topic(), false)
					return err
				}
			}
			*lastSchemaId = upstreamJobSchemaId
			h.loadRunning.Store(claim.Topic(), false)
		case <-maxWaitTicker.C:
			// Process the batch by time
			klog.V(2).Infof(
				"%s: maxWaitSeconds hit",
				claim.Topic(),
			)
			maxWaitTicker.Stop()
			if msgBatch.Size() > 0 {
				err = h.throttle(claim.Topic())
				if err != nil {
					return err
				}
			}
			h.loadRunning.Store(claim.Topic(), true)
			err = msgBatch.Process(session)
			maxWaitTicker.Reset(time.Duration(*h.maxWaitSeconds) * time.Second)
			if err != nil {
				h.loadRunning.Store(claim.Topic(), false)
				return err
			}
			h.loadRunning.Store(claim.Topic(), false)
		}
	}
}
