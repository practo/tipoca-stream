package transformer

import (
	"github.com/practo/tipoca-stream/kafka-go/pkg/serializer"
)

type MsgTransformer interface {
	Transform(message *serializer.Message) error
}

type SchemaTransformer interface {
	// TransformKey transforms the topic schema into name of the primary
	// key and its type.
	TransformKey(topic string) (string, string, error)
	// Transform value transforms the schemaId for various use cases.
	TransformValue(schemaId int) (interface{}, error)
}
