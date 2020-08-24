package transformer

import (
	"encoding/json"
	"fmt"

	"github.com/practo/tipoca-stream/kafka-go/pkg/serializer"
)

type Transformer interface {
	TransformSchema(schema string) (interface{}, error)
	TransformMessage(message *serializer.Message) error
}

func NewTransformer() Transformer {
	return &redshiftTransformer{}
}
