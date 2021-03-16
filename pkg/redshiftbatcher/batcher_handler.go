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

const (
	DefaultMaxConcurrency    = 10
	DefaultMaxProcessingTime = 600
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
	MaxSize int `yaml:"maxSize,omitempty"`
	// MaxWaitSeconds after which the bash would be pushed regardless of its size.
	MaxWaitSeconds int `yaml:"maxWaitSeconds,omitempty"`
	// MaxConcurrency is the maximum number of concurrent batch processing to run
	// Defaults to 10
	MaxConcurrency int `yaml:"maxConcurrency,omitempty"`
}

// batcherHandler is the sarama consumer handler
// batcherHandler.ConsumeClaim() is called for every topic partition
type batcherHandler struct {
	ready chan bool
	ctx   context.Context

	maxSize        int
	maxWaitSeconds int
	maxConcurrency int

	consumerGroupID string

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
	if batcherConfig.MaxConcurrency == 0 {
		batcherConfig.MaxConcurrency = DefaultMaxConcurrency
	}

	return &batcherHandler{
		ready: ready,
		ctx:   ctx,

		consumerGroupID: consumerGroupID,

		maxSize:        batcherConfig.MaxSize,
		maxWaitSeconds: batcherConfig.MaxWaitSeconds,
		maxConcurrency: batcherConfig.MaxConcurrency,

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
	processChan := make(chan []*serializer.Message, 1000)
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
		h.maxConcurrency,
	)
	msgBatch := serializer.NewMessageAsyncBatch(
		claim.Topic(),
		claim.Partition(),
		h.maxSize,
		processChan,
	)
	maxWaitTicker := time.NewTicker(
		time.Duration(h.maxWaitSeconds) * time.Second,
	)

	wg := &sync.WaitGroup{}
	go processor.Process(wg, session, processChan, errChan)
	wg.Add(1)
	defer wg.Wait()

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
				return fmt.Errorf("error deserializing binary, err: %s\n", err)
			}
			if msg == nil || msg.Value == nil {
				return fmt.Errorf("got message as nil, message: %+v\n", msg)
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
				msgBatch.Flush()
			}
			// Flush the batch by size or insert in batch
			msgBatch.Insert(msg)
			*lastSchemaId = msg.SchemaId
		case <-maxWaitTicker.C:
			// Flush the batch by time
			klog.V(2).Infof(
				"%s: maxWaitSeconds hit",
				claim.Topic(),
			)
			msgBatch.Flush()
		case err := <-errChan:
			syscall.Kill(syscall.Getpid(), syscall.SIGINT)
			klog.Errorf(
				"%s: error occured in processing, err: %v, triggered shutdown",
				claim.Topic(),
				err,
			)
			time.Sleep(30 * time.Second)
			return nil
		}
	}
}
