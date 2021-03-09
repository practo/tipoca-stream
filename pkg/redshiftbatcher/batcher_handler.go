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
	"time"
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
}

// batcherHandler is the sarama consumer handler
// batcherHandler.ConsumeClaim() is called for every topic partition
type batcherHandler struct {
	ready chan bool
	ctx   context.Context

	maxSize        int
	maxWaitSeconds int

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
	return &batcherHandler{
		ready: ready,
		ctx:   ctx,

		consumerGroupID: consumerGroupID,

		maxSize:        batcherConfig.MaxSize,
		maxWaitSeconds: batcherConfig.MaxWaitSeconds,

		kafkaConfig:            kafkaConfig,
		saramaConfig:           saramaConfig,
		maskConfig:             maskConfig,
		serializer:             serializer.NewSerializer(viper.GetString("schemaRegistryURL")),
		kafkaLoaderTopicPrefix: loaderPrefix,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *batcherHandler) Setup(sarama.ConsumerGroupSession) error {
	klog.V(3).Info("Setting up consumer")

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
	klog.V(3).Info("Cleaning up consumer")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// ConsumeClaim is managed by the consumer.manager routine
func (h *batcherHandler) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {

	klog.V(2).Infof(
		"ConsumeClaim for topic:%s, partition:%d, initalOffset:%d\n",
		claim.Topic(),
		claim.Partition(),
		claim.InitialOffset(),
	)

	var lastSchemaId *int
	var err error
	processor := newBatchProcessor(
		session,
		h.consumerGroupID,
		claim.Topic(),
		claim.Partition(),
		h.kafkaConfig,
		h.saramaConfig,
		h.maskConfig,
		h.kafkaLoaderTopicPrefix,
	)
	msgBatch := serializer.NewMessageBatch(
		claim.Topic(),
		claim.Partition(),
		h.maxSize,
		processor,
	)
	maxWaitTicker := time.NewTicker(
		time.Duration(h.maxWaitSeconds) * time.Second,
	)

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	claimMsgChan := claim.Messages()

	for {
		select {
		case <-h.ctx.Done():
			klog.V(2).Infof(
				"ConsumeClaim gracefully shutdown for topic: %s (above)",
				claim.Topic(),
			)
			return nil
		case <-session.Context().Done():
			return fmt.Errorf("Session context done, recreate, err: %v", session.Context().Err())
		case message, ok := <-claimMsgChan:
			if !ok {
				klog.V(2).Infof(
					"topic:%s: ConsumeClaim ending, hit",
					claim.Topic(),
				)
				err = msgBatch.Process(h.ctx)
				if err != nil {
					return err
				}
				klog.V(2).Infof(
					"ConsumeClaim ended for topic: %s, partition: %d (would rerun by manager)\n",
					claim.Topic(),
					claim.Partition(),
				)
				return nil
			}

			select {
			default:
			case <-h.ctx.Done():
				klog.V(2).Infof(
					"ConsumeClaim gracefully shutdown for topic: %s",
					claim.Topic(),
				)
				return nil
			case <-session.Context().Done():
				return fmt.Errorf("Session context done, recreate, err: %v", session.Context().Err())
			}

			if len(message.Value) == 0 {
				klog.V(2).Infof(
					"Skipping message, received a tombstone event, message: %+v\n",
					message)
				continue
			}

			// Deserialize the message
			msg, err := h.serializer.Deserialize(message)
			if err != nil {
				klog.Fatalf("Error deserializing binary, err: %s\n", err)
			}
			if msg == nil || msg.Value == nil {
				klog.Fatalf("Got message as nil, message: %+v\n", msg)
			}

			if lastSchemaId == nil {
				lastSchemaId = new(int)
			} else if *lastSchemaId != msg.SchemaId {
				klog.V(2).Infof(
					"topic:%s: schema changed, %d => %d\n",
					claim.Topic(),
					*lastSchemaId,
					msg.SchemaId,
				)
				err = msgBatch.Process(h.ctx)
				if err != nil {
					return err
				}
			} else {
			}
			// Process the batch by size or insert in batch
			err = msgBatch.Insert(h.ctx, msg)
			if err != nil {
				return err
			}
			*lastSchemaId = msg.SchemaId
		case <-maxWaitTicker.C:
			// Process the batch by time
			klog.V(2).Infof(
				"topic:%s: maxWaitSeconds hit",
				claim.Topic(),
			)
			err = msgBatch.Process(h.ctx)
			if err != nil {
				return err
			}
		}
	}
}
