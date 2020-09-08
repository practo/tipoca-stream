package transformer

import (
	"encoding/json"
	"fmt"
	"github.com/practo/tipoca-stream/kafka-go/pkg/serializer"
	"strings"
)

const (
	OperationColumn     = "operation"
	OperationColumnType = "string"
	OperationCreate     = "CREATE"
	OperationUpdate     = "UPDATE"
	OperationDelete     = "DELETE"
)

type debeziumMsgParser struct {
	msg interface{}
}

func (d *debeziumMsgParser) extract(key string, msg map[string]interface{},
	result map[string]*string) {

	dataKey := msg[key]
	if dataKey == nil {
		return
	}

	data := dataKey.(map[string]interface{})

	// Why such handling? https://github.com/linkedin/goavro/issues/217
	for _, v := range data {
		for k2, v2 := range v.(map[string]interface{}) {
			switch v2.(type) {
			case map[string]interface{}:
				for _, v3 := range v2.(map[string]interface{}) {
					columnValue := fmt.Sprintf("%v", v3)
					result[strings.ToLower(k2)] = &columnValue
				}
			case nil:
				result[strings.ToLower(k2)] = nil
			default:
				columnValue := fmt.Sprintf("%v", v2)
				result[strings.ToLower(k2)] = &columnValue
			}
		}
	}
}

func (d *debeziumMsgParser) after() map[string]*string {
	result := make(map[string]*string)
	if d.msg == nil {
		return result
	}

	data := d.msg.(map[string]interface{})
	if data == nil {
		return result
	}

	d.extract("after", data, result)

	return result
}

func (d *debeziumMsgParser) before() map[string]*string {
	result := make(map[string]*string)
	if d.msg == nil {
		return result
	}

	data := d.msg.(map[string]interface{})
	if data == nil {
		return result
	}

	d.extract("before", data, result)

	return result
}

func NewMsgTransformer() MsgTransformer {
	return &debeziumMsgTransformer{}
}

type debeziumMsgTransformer struct{}

func (c *debeziumMsgTransformer) getOperation(message *serializer.Message,
	beforeLen int, afterLen int) (string, error) {

	r := 0
	if beforeLen != 0 {
		r += 1
	}
	if afterLen != 0 {
		r += 2
	}
	switch r {
	case 0:
		return "", fmt.Errorf(
			"message: %v has both before and after as nil\n", message)
	case 1:
		return OperationDelete, nil
	case 2:
		return OperationCreate, nil
	case 3:
		return OperationUpdate, nil
	default:
		return "", fmt.Errorf(
			"message: %v not possible get operation\n", message)
	}
}

// Transform debezium event into a s3 message annotating extra information
func (c *debeziumMsgTransformer) Transform(
	message *serializer.Message) error {

	d := &debeziumMsgParser{
		msg: message.Value,
	}

	before := d.before()
	after := d.after()

	operation, err := c.getOperation(message, len(before), len(after))
	if err != nil {
		return err
	}

	// redshift only has all columns as lower cases
	kafkaOffset := fmt.Sprintf("%v", message.Offset)
	after["kafkaoffset"] = &kafkaOffset
	after["operation"] = &operation

	message.Value, err = json.Marshal(after)
	if err != nil {
		return err
	}

	return nil
}
