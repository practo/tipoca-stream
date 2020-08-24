package transformer

import (
	"encoding/json"
	"fmt"

	"github.com/practo/tipoca-stream/kafka-go/pkg/serializer"
)

type Transformer interface {
	Transform(message *serializer.Message) error
}

func NewTransformer() Transformer {
	return &redshiftTransformer{}
}

type redshiftTransformer struct{}

func (c *redshiftTransformer) getOperation(
	message *serializer.Message,
	before map[string]string,
	after map[string]string) (string, error) {

	r := 0
	if len(before) != 0 {
		r += 1
	}
	if len(after) != 0 {
		r += 2
	}
	switch r {
	case 0:
		return "", fmt.Errorf(
			"message: %v has both before and after as nil\n", message)
	case 1:
		return "DELETE", nil
	case 2:
		return "CREATE", nil
	case 3:
		return "UPDATE", nil
	default:
		return "", fmt.Errorf(
			"message: %v not possible get operation\n", message)
	}
}

func (c *redshiftTransformer) Transform(message *serializer.Message) error {
	d := &debeziumParser{}

	before := d.before(message.Value)
	after := d.after(message.Value)

	operation, err := c.getOperation(message, before, after)
	if err != nil {
		return err
	}
	after["operation"] = operation

	message.Value, err = json.Marshal(after)
	if err != nil {
		return err
	}

	return nil
}
