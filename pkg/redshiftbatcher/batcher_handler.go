package redshiftbatcher

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer/masker"
	"github.com/spf13/viper"
	"sync"
	"syscall"
	"time"
)

var (
	DefaultMaxBytesPerBatch  string = "1024"
	DefaultMaxWaitSeconds    int    = 30
	DefaultMaxConcurrency    int    = 10
	DefaultMaxProcessingTime int32  = 180000
)

type BatcherConfig struct {
	// Mask should be turned on or off
	Mask bool `yaml:"mask,omitempty"`
	// MaskSalt specifies the salt to be used for masking
	MaskSalt string `yaml:"maskSalt,omitempty"`
	// MaskFile can be the either of the two:
	// 1. Absolute path of the mask configuration file. This file needs to be
	// be mounted as config map when the batcher starts.
	// 2. Git File or Folder. Examples:
	// https://github.com/practo/tipoca-stream/pkg/database.yaml
	// then this file or repo be cloned and kept at / when batcher starts.
	MaskFile string `yaml:"maskFile,omitempty"`
	// MaskFileVersion is the git version of the MaskFile
	// It is useful when the MaskFile is specified is a Git File.
	// otherwise when MaskFile is an abosolute mounted file, it is not used.
	MaskFileVersion string `yaml:"maskFileVersion,omitempty"`

	// MaxSize is the maximum size of a batch, on exceeding this batch is pushed
	// regarless of the wait time.
	// Deprecated: in favour of MaxBytesPerBatch
	MaxSize int `yaml:"maxSize,omitempty"`

	// MaxWaitSeconds after which the batch would be flushed
	// Defaults to 30
	MaxWaitSeconds *int `yaml:"maxWaitSeconds,omitempty"`
	// MaxConcurrency is the maximum number of concurrent processing to run
	// Defaults to 10
	MaxConcurrency *int `yaml:"maxConcurrency,omitempty"`
	// MaxBytesPerBatch is the maximum bytes per batch. Default is there
	// if the user has not specified a default will be applied.
	// If this is specified, maxSize specification is not considered.
	// Default would be specified after MaxSize is gone
	MaxBytesPerBatch *int64 `yaml:"maxBytesPerBatch,omitempty"`
}

// batcherHandler is the sarama consumer handler
// batcherHandler.ConsumeClaim() is called for every topic partition
type batcherHandler struct {
	ready chan bool
	ctx   context.Context

	maxSize int // Deprecated in favour of maxBytesPerBatch

	maxWaitSeconds   *int
	maxConcurrency   *int
	maxBytesPerBatch *int64

	consumerGroupID        string
	kafkaConfig            kafka.KafkaConfig
	saramaConfig           kafka.SaramaConfig
	maskConfig             masker.MaskConfig
	serializer             serializer.Serializer
	kafkaLoaderTopicPrefix string
}

func NewHandler(
	ready chan bool,
	ctx context.Context,
	consumerGroupID string,
	batcherConfig BatcherConfig,
	kafkaConfig kafka.KafkaConfig,
	saramaConfig kafka.SaramaConfig,
	maskConfig masker.MaskConfig,
	loaderPrefix string,
) *batcherHandler {

	// apply defaults
	if batcherConfig.MaxWaitSeconds == nil {
		batcherConfig.MaxWaitSeconds = &DefaultMaxWaitSeconds
	}
	if batcherConfig.MaxConcurrency == nil {
		batcherConfig.MaxConcurrency = &DefaultMaxConcurrency
	}

	return &batcherHandler{
		ready: ready,
		ctx:   ctx,

		consumerGroupID: consumerGroupID,

		maxSize: batcherConfig.MaxSize, // Deprecated

		maxWaitSeconds:   batcherConfig.MaxWaitSeconds,
		maxConcurrency:   batcherConfig.MaxConcurrency,
		maxBytesPerBatch: batcherConfig.MaxBytesPerBatch,

		kafkaConfig:            kafkaConfig,
		saramaConfig:           saramaConfig,
		maskConfig:             maskConfig,
		serializer:             serializer.NewSerializer(viper.GetString("schemaRegistryURL")),
		kafkaLoaderTopicPrefix: loaderPrefix,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *batcherHandler) Setup(sarama.ConsumerGroupSession) error {
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

// Cleanup is run at the end of a session,
// once all ConsumeClaim goroutines have exited
func (h *batcherHandler) Cleanup(sarama.ConsumerGroupSession) error {
	klog.V(1).Info("cleaning up handler")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// ConsumeClaim is managed by the consumer.manager routine
func (h *batcherHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	klog.V(1).Infof(
		"%s: consumeClaim started, initalOffset:%d\n",
		claim.Topic(),
		claim.InitialOffset(),
	)

	var lastSchemaId *int
	processChan := make(chan []*serializer.Message, *h.maxConcurrency)
	errChan := make(chan error)
	processor := newBatchProcessor(
		h.consumerGroupID,
		claim.Topic(),
		claim.Partition(),
		processChan,
		h.kafkaConfig,
		h.saramaConfig,
		h.maskConfig,
		h.kafkaLoaderTopicPrefix,
		*h.maxConcurrency,
	)
	maxBufSize := h.maxSize
	if h.maxBytesPerBatch != nil {
		maxBufSize = serializer.DefaultMessageBufferSize
	}
	msgBatch := serializer.NewMessageAsyncBatch(
		claim.Topic(),
		claim.Partition(),
		h.maxSize, // Deprecated
		maxBufSize,
		h.maxBytesPerBatch,
		processChan,
	)
	maxWaitTicker := time.NewTicker(
		time.Duration(*h.maxWaitSeconds) * time.Second,
	)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go processor.Process(wg, session, processChan, errChan)

	defer func() {
		klog.V(2).Infof("%s: wg wait() for processing to return", claim.Topic())
		wg.Wait()
		klog.V(2).Infof("%s: wg done. processing returned", claim.Topic())
	}()

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
			return kafka.ErrSaramaSessionContextDone
		case message, ok := <-claimMsgChan:
			if !ok {
				klog.V(2).Infof(
					"%s: consumeClaim returning. read msg channel closed",
					claim.Topic(),
				)
				return nil
			}

			if len(message.Value) == 0 {
				klog.V(2).Infof(
					"%s: skipping msg, received tombstone, message: %+v\n",
					claim.Topic(),
					message,
				)
				continue
			}

			// Deserialize the message
			msg, err := h.serializer.Deserialize(message)
			if err != nil {
				return fmt.Errorf("%s: consumeClaim returning, error deserializing binary, err: %s\n", claim.Topic(), err)
			}
			if msg == nil || msg.Value == nil {
				return fmt.Errorf("%s: consumeClaim returning, error, got message as nil, message: %+v\n", claim.Topic(), msg)
			}

			if lastSchemaId == nil {
				lastSchemaId = new(int)
			} else if *lastSchemaId != msg.SchemaId {
				klog.V(2).Infof(
					"%s: schema changed, %d => %d (batch flush)\n",
					claim.Topic(),
					*lastSchemaId,
					msg.SchemaId,
				)
				// Flush the batch due to schema change
				msgBatch.Flush(session.Context())
			}
			// Flush the batch by maxBytes or size on insert in batch
			msgBatch.Insert(session.Context(), msg)
			*lastSchemaId = msg.SchemaId
		case <-maxWaitTicker.C:
			// Flush the batch by time
			klog.V(2).Infof(
				"%s: maxWaitSeconds hit",
				claim.Topic(),
			)
			msgBatch.Flush(session.Context())
		case err := <-errChan:
			syscall.Kill(syscall.Getpid(), syscall.SIGINT)
			klog.Errorf(
				"consumeClaim returning, %s: error occured in processing, err: %v, triggered shutdown",
				claim.Topic(),
				err,
			)
			time.Sleep(30 * time.Second)
			return nil
		}
	}
}
