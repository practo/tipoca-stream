package transformer

import (
	"github.com/practo/tipoca-stream/kafka-go/pkg/serializer"
	"strings"
)

type MessageTransformer interface {
	Transform(message *serializer.Message) error
}

type SchemaTransformer interface {
	// TransformKey transforms the topic schema into name of the primary
	// key and its type.
	TransformKey(topic string) (string, string, error)
	// Transform value transforms the schemaId for various use cases.
	TransformValue(topic string, schemaId int) (interface{}, error)
}

// ParseTopic breaks down the topic string into server, database, table
func ParseTopic(topic string) (string, string, string) {
	t := strings.Split(topic, ".")
	return t[0], t[1], t[2]
}
