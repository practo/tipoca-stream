package transformer

import (
	"github.com/practo/tipoca-stream/kafka-go/pkg/serializer"
)

type MsgTransformer interface {
	Transform(message *serializer.Message) error
}

type SchemaTransformer interface {
	Transform(schemaId int) (interface{}, error)
}
