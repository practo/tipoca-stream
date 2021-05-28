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
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/util"
	"github.com/prometheus/common/model"
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

	saramaConfig kafka.SaramaConfig
	serializer   serializer.Serializer

	redshifter      *redshift.Redshift
	redshiftSchema  string
	redshiftGroup   *string
	redshiftMetrics bool

	// prometheusClient is used to query the running loaders
	// so that concurrency of load can be maintained below a threshold
	prometheusClient prometheus.Client

	// schemaQueries stores the queries per topic in schema
	// if this is available for a topic, we are able to randomize maxWait
	// better for the topic, it specifies which table is being used more
	// in redshift
	schemaQueries *model.Vector

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
	redshiftSchema string,
	redshiftGroup *string,
	redshiftMetrics bool,
	prometheusClient prometheus.Client,
	schemaQueries *model.Vector,
) *loaderHandler {
	return &loaderHandler{
		ready: ready,
		ctx:   ctx,

		consumerGroupID: consumerGroupID,

		maxSize: loaderConfig.MaxSize, // Deprecated

		maxWaitSeconds:   loaderConfig.MaxWaitSeconds,
		maxBytesPerBatch: loaderConfig.MaxBytesPerBatch,

		saramaConfig: saramaConfig,
		serializer:   serializer.NewSerializer(viper.GetString("schemaRegistryURL")),

		redshifter:      redshifter,
		redshiftSchema:  redshiftSchema,
		redshiftGroup:   redshiftGroup,
		redshiftMetrics: redshiftMetrics,

		prometheusClient: prometheusClient,
		schemaQueries:    schemaQueries,
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

type throttleBudget struct {
	max      int
	interval int // seconds
}

func (h *loaderHandler) throttleBudget(topic string, firstLoad bool) (throttleBudget, error) {
	// When Redshift Metric is disabled, all topics are throtted in the
	// same way, regardless of their usage in Redshift.
	// i.e. they get the same throttling budget.
	if !h.redshiftMetrics {
		klog.V(2).Infof("%s: redshiftMetrics disabled, recommended to enable", topic)
		if firstLoad {
			return throttleBudget{max: 120, interval: 15}, nil // 30mins max
		} else {
			return throttleBudget{max: 10, interval: 15}, nil // 2.5 mins max
		}
	}

	// When Redshift Metric is enabled,
	// throttling budget is based on the usage of tables in redshift.
	queries, err := h.prometheusClient.Query(
		fmt.Sprintf(
			"redshift_scan_query_total{schema='%s', tablename='%s'}",
			h.redshiftSchema,
			topic,
		),
	)
	if err != nil {
		return throttleBudget{}, err
	}

	if queries > 0 && firstLoad {
		return throttleBudget{max: 120, interval: 15}, nil // 30mins  max
	} else if queries > 0 {
		return throttleBudget{max: 3, interval: 10}, nil // 30 seconds max, just to spread out the load
	} else if queries == 0 && firstLoad { // case of tables which have not been queried in last 1d and this the first run
		return throttleBudget{max: 8, interval: 900}, nil // 2hrs max
	} else { // case of tables which have not been queried in last 1d and this not the first run
		return throttleBudget{max: 4, interval: 900}, nil // 1hr max
	}
}

func (h *loaderHandler) throttle(topic string, metric metricSetter, sinkGroup string) error {
	if sinkGroup == "reload" {
		klog.V(2).Infof("%s: throttling is disabled for reload sinkgroup", sinkGroup)
		return nil
	}
	// never throttle if promtheus client is not set
	// this makes throttling using prometheus an addon feature
	if h.prometheusClient == nil {
		klog.V(2).Infof("%s: prometheus disabled, throttle disabled", topic)
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
	klog.V(4).Infof("%s: running loaders(local): %v", topic, localLoadRunning)

	firstLoad := true
	_, ok := h.loadRunning.Load(topic)
	if ok {
		firstLoad = false
	}

	var budget throttleBudget
	for cnt := 0; ; cnt++ {
		budget, err := h.throttleBudget(topic, firstLoad)
		if err != nil {
			return err
		}
		if cnt >= budget.max {
			break
		}

		runningLoaders, err := h.prometheusClient.Query("sum(rsk_loader_running > 0)")
		if err != nil {
			return err
		}
		klog.V(4).Infof("%s: running loaders(metric): %v", topic, runningLoaders)

		if (runningLoaders <= MaxRunningLoaders) && (localLoadRunning <= MaxRunningLoaders) {
			return nil
		}

		klog.V(2).Infof("%s: throttled for %+v seconds", topic, budget.interval)
		metric.incThrottled()
		time.Sleep(time.Duration(budget.interval) * time.Second)
	}

	klog.V(2).Infof("%s: exhausted throttle budget: %+v, go load!", topic, budget)

	return nil
}

// randomMaxWait helps to keep the maxWait +- 20% of the specified value
// this is required to spread the load in Redshift
func (h *loaderHandler) randomMaxWait(topic string) *int {
	var maxAllowed, minAllowed *int
	_, _, table := transformer.ParseTopic(topic)
	queries, err := h.prometheusClient.FilterVector(
		*h.schemaQueries,
		"tablename",
		table,
	)
	if err != nil {
		klog.Warningf("Can't use prometheus to decide maxWait, err: %v", err)
		return h.maxWaitSeconds
	}
	if queries != nil && float64(*queries) > 0.0 {
		maxAllowed = h.maxWaitSeconds
	} else {
		minAllowed = h.maxWaitSeconds
	}
	newMaxWait := util.Randomize(*h.maxWaitSeconds, 0.20, maxAllowed, minAllowed)

	return &newMaxWait
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

	sinkGroup := viper.GetString("sinkGroup")
	metric := metricSetter{
		consumergroup: h.consumerGroupID,
		topic:         claim.Topic(),
		rsk:           viper.GetString("rsk"),
		sinkGroup:     sinkGroup,
	}
	processor, err := newLoadProcessor(
		h.consumerGroupID,
		claim.Topic(),
		claim.Partition(),
		h.saramaConfig,
		h.redshifter,
		h.redshiftGroup,
		metric,
	)

	// randomize maxWait if prometheus and redshift metrics are available
	if h.prometheusClient != nil && h.redshiftMetrics {
		h.maxWaitSeconds = h.randomMaxWait(claim.Topic())
	}
	klog.V(2).Infof("%s: maxWaitSeconds=%vs", claim.Topic(), *h.maxWaitSeconds)

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
				err = h.throttle(claim.Topic(), metric, sinkGroup)
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
				h.loadRunning.Store(claim.Topic(), false)
			}
			*lastSchemaId = upstreamJobSchemaId
		case <-maxWaitTicker.C:
			// Process the batch by time
			klog.V(2).Infof(
				"%s: maxWaitSeconds hit",
				claim.Topic(),
			)
			maxWaitTicker.Stop()
			if msgBatch.Size() > 0 {
				err = h.throttle(claim.Topic(), metric, sinkGroup)
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
