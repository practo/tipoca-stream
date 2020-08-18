package transformer

import (
    "fmt"
    "encoding/json"

    "github.com/practo/tipoca-stream/consumer-go/pkg/serializer"
)

type Transformer interface {
    Transform(message *serializer.Message) error
}

func NewTransformer() Transformer {
    return &redshiftTransformer{}
}

type redshiftTransformer struct {}

func (c *redshiftTransformer) getOperation(
    message *serializer.Message,
    before map[string]string,
    after map[string]string) (string, error) {

    r := 0
    if before != nil {
        r += 1
    }
    if after != nil {
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
    d := &debeziumTransformer{}

    before := d.before(message.Value)
    after  := d.after(message.Value)

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
