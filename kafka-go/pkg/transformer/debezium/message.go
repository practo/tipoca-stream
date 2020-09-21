package debezium

import (
	"encoding/json"
	"fmt"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/kafka-go/pkg/redshift"
	"github.com/practo/tipoca-stream/kafka-go/pkg/serializer"
	"github.com/practo/tipoca-stream/kafka-go/pkg/transformer"
	"strconv"
	"strings"
	"time"
)

const (
	OperationCreate     = "CREATE"
	OperationUpdate     = "UPDATE"
	OperationDelete     = "DELETE"
	OperationColumn     = "operation"
	OperationColumnType = "character varying(15)"

	millisInSecond = 1000
	nsInSecond     = 1000000
)

// Converts Unix Epoch from milliseconds to time.Time
// Why? https://github.com/Tigraine/go-timemill
func FromUnixMilli(ms int64) time.Time {
	return time.Unix(
		ms/int64(millisInSecond), (ms%int64(millisInSecond))*int64(nsInSecond))
}

func NewMessageTransformer() transformer.MessageTransformer {
	return &messageTransformer{}
}

type messageParser struct {
	message interface{}
}

// extract extracts out the columns name and value from the debezium message
func (d *messageParser) extract(key string, message map[string]interface{},
	result map[string]*string) {

	dataKey := message[key]
	if dataKey == nil {
		return
	}
	data := dataKey.(map[string]interface{})

	// why handled liket this ?: https://github.com/linkedin/goavro/issues/217
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

// after extracts out the "after" columns in the debezium message
func (d *messageParser) after() map[string]*string {
	result := make(map[string]*string)
	if d.message == nil {
		return result
	}
	data := d.message.(map[string]interface{})
	if data == nil {
		return result
	}
	d.extract("after", data, result)

	return result
}

// before extracts out the "before" columns in the debezium message
func (d *messageParser) before() map[string]*string {
	result := make(map[string]*string)
	if d.message == nil {
		return result
	}
	data := d.message.(map[string]interface{})
	if data == nil {
		return result
	}
	d.extract("before", data, result)

	return result
}

type messageTransformer struct{}

func (c *messageTransformer) getOperation(message *serializer.Message,
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
func (c *messageTransformer) Transform(
	message *serializer.Message, table redshift.Table) error {

	d := &messageParser{
		message: message.Value,
	}

	before := d.before()
	after := d.after()

	// transform debezium timestamp to redshift loadable value
	for _, column := range table.Columns {
		if column.Type == redshift.RedshiftTimeStampDataType {
			mstr, ok := after[column.Name]
			if !ok {
				klog.Warningf("column %s not found, skipped\n", column.Name)
				continue
			}
			if mstr == nil {
				continue
			}

			m, err := strconv.Atoi(*mstr)
			if err != nil {
				return err
			}
			ts := FromUnixMilli(int64(m))
			fts := fmt.Sprintf(
				"%d-%02d-%02d %02d:%02d:%02d",
				ts.Year(), ts.Month(), ts.Day(),
				ts.Hour(), ts.Minute(), ts.Second(),
			)
			after[column.Name] = &fts
		}
	}

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
