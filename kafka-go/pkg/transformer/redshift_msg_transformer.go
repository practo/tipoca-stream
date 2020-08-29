package transformer

import (
	"encoding/json"
	"fmt"
	"github.com/practo/tipoca-stream/kafka-go/pkg/serializer"
)

type debeziumMsgParser struct {
	msg interface{}
}

func (d *debeziumMsgParser) extract(
	key string, msg map[string]interface{},
	result map[string]string) map[string]string {

	dataKey := msg[key]
	if dataKey == nil {
		return result
	}

	data := dataKey.(map[string]interface{})

	// Why such handling? https://github.com/linkedin/goavro/issues/217
	for _, v := range data {
		for k2, v2 := range v.(map[string]interface{}) {
			switch v2.(type) {
			case map[string]interface{}:
				for _, v3 := range v2.(map[string]interface{}) {
					result[k2] = fmt.Sprintf("%v", v3)
				}
			default:
				result[k2] = fmt.Sprintf("%v", v2)
			}
		}
	}

	return result
}

func (d *debeziumMsgParser) after() map[string]string {
	result := make(map[string]string)
	if d.msg == nil {
		return result
	}

	data := d.msg.(map[string]interface{})
	if data == nil {
		return result
	}

	return d.extract("after", data, result)
}

func (d *debeziumMsgParser) before() map[string]string {
	result := make(map[string]string)
	if d.msg == nil {
		return result
	}

	data := d.msg.(map[string]interface{})
	if data == nil {
		return result
	}

	return d.extract("before", data, result)
}

func NewMsgTransformer() MsgTransformer {
	return &redshiftMsgTransformer{}
}

type redshiftMsgTransformer struct{}

func (c *redshiftMsgTransformer) getOperation(
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

// Transform debezium event into a s3 message annotating extra information
func (c *redshiftMsgTransformer) Transform(
	message *serializer.Message) error {

	d := &debeziumMsgParser{
		msg: message.Value,
	}

	before := d.before()
	after := d.after()

	operation, err := c.getOperation(message, before, after)
	if err != nil {
		return err
	}
	after["kafkaOffset"] = fmt.Sprintf("%v", message.Offset)
	after["operation"] = operation

	message.Value, err = json.Marshal(after)
	if err != nil {
		return err
	}

	return nil
}
