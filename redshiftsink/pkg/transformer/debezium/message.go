package debezium

import (
	"fmt"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	"strconv"
	"strings"
	"time"
)

const (
	OperationColumn     = "operation"
	OperationColumnType = "character varying(15)"

	millisInSecond  = 1000
	microInSecond   = 1000000
	nsMicroInSecond = 1000
	nsMilliInSecond = 1000000
)

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
		return serializer.OperationDelete, nil
	case 2:
		return serializer.OperationCreate, nil
	case 3:
		return serializer.OperationUpdate, nil
	default:
		return "", fmt.Errorf(
			"message: %v not possible get operation\n", message)
	}
}

// Converts Unix Epoch from milliseconds to time.Time
// It is faster this way, benchmark: https://github.com/Tigraine/go-timemill
func FromUnixMilli(ms int64) time.Time {
	return time.Unix(
		ms/int64(millisInSecond),
		(ms%int64(millisInSecond))*int64(nsMilliInSecond),
	)
}

func FromUnixMicro(us int64) time.Time {
	return time.Unix(
		us/int64(microInSecond),
		(us%int64(microInSecond))*int64(nsMicroInSecond),
	)
}

func convertDebeziumDate(days int) string {
	ts := FromUnixMilli(int64(days * 86400 * 1000))
	ts = ts.UTC()
	return fmt.Sprintf(
		"%d-%02d-%02d",
		ts.Year(), ts.Month(), ts.Day(),
	)
}

func convertDebeziumTimeStamp(timestamp string) string {
	// strings replace would be faster than formatting time
	ts := strings.ReplaceAll(timestamp, "T", " ")
	return strings.ReplaceAll(ts, "Z", "")
}

func convertDebeziumMilliseconds(ms int, length int) string {
	ts := FromUnixMilli(int64(ms))
	ts = ts.UTC()

	result := fmt.Sprintf(
		"%d-%02d-%02d %02d:%02d:%02d",
		ts.Year(), ts.Month(), ts.Day(),
		ts.Hour(), ts.Minute(), ts.Second(),
	)

	if length == 0 {
		return result
	}

	// fraction: 1988-08-21 14:01:02.23
	ns := strconv.Itoa(ts.Nanosecond())
	if len(ns) == 0 {
		return result
	}
	if len(ns) < length {
		klog.Warningf(
			"nano second got: %v is less than length: %v\n", ns, length)
		length = len(ns)
	}

	return fmt.Sprintf("%s.%s", result, ns[:length])
}

func convertDebeziumMicroseconds(us int, length int) string {
	ts := FromUnixMicro(int64(us))
	ts = ts.UTC()

	result := fmt.Sprintf(
		"%d-%02d-%02d %02d:%02d:%02d",
		ts.Year(), ts.Month(), ts.Day(),
		ts.Hour(), ts.Minute(), ts.Second(),
	)

	if length == 0 {
		return result
	}

	// fraction: 1988-08-21 14:01:02.23
	ns := strconv.Itoa(ts.Nanosecond())
	if len(ns) == 0 {
		return result
	}
	if len(ns) < length {
		klog.Warningf(
			"nano second got: %v is less than length: %v\n", ns, length)
		length = len(ns)
	}

	return fmt.Sprintf("%s.%s", result, ns[:length])
}

// convertDebeziumFormattedTime formats the debezium time into redshift time
// maitaining the precsion
// https://debezium.io/documentation/reference/1.2/connectors/mysql.html#_temporal_values
func convertDebeziumFormattedTime(
	value string, sourceType string, sourceLength string) (string, error) {

	switch sourceType {
	case "DATE":
		ts, err := strconv.Atoi(value)
		if err != nil {
			return "", fmt.Errorf(
				"Error converting date col val to int, err: %v\n", err)
		}
		return convertDebeziumDate(ts), nil
	case "TIMESTAMP":
		return convertDebeziumTimeStamp(value), nil
	case "DATETIME":
		var colLength int
		if sourceLength == "" {
			colLength = 0
		} else {
			c, err := strconv.Atoi(sourceLength)
			if err != nil {
				return "", fmt.Errorf(
					"Error converting col length to int, err: %v\n", err)
			}
			colLength = c
		}

		ts, err := strconv.Atoi(value)
		if err != nil {
			return "", fmt.Errorf(
				"Error converting datetime col val to int, err: %v\n",
				err)
		}

		var redshiftTime string
		if colLength < 3 {
			redshiftTime = convertDebeziumMilliseconds(ts, colLength)
		} else {
			redshiftTime = convertDebeziumMicroseconds(ts, colLength)
		}

		return redshiftTime, nil
	default:
		return "", fmt.Errorf("Unhandled source type: %v\n", sourceType)
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

	operation, err := c.getOperation(message, len(before), len(after))
	if err != nil {
		return err
	}

	value := make(map[string]*string)

	switch operation {
	case serializer.OperationCreate:
		value = after
	case serializer.OperationUpdate:
		value = after
	case serializer.OperationDelete:
		value = before
	default:
		return fmt.Errorf("Unknown operation: %s\n", operation)
	}

	// Transform DATETIME, TIMESTAMP and DATE from debezium to redshift

	for _, column := range table.Columns {
		if column.Type != redshift.RedshiftTimeStamp &&
			column.Type != redshift.RedshiftDate {
			continue
		}
		mstr, ok := value[column.Name]
		if !ok {
			klog.Warningf("column %s not found, skipped\n", column.Name)
			continue
		}
		if mstr == nil {
			continue
		}

		formattedTime, err := convertDebeziumFormattedTime(
			*mstr,
			column.SourceType.ColumnType,
			column.SourceType.ColumnLength,
		)
		if err != nil {
			return err
		}
		value[column.Name] = &formattedTime
	}

	// redshift only has all columns as lower cases
	kafkaOffset := fmt.Sprintf("%v", message.Offset)
	value["kafkaoffset"] = &kafkaOffset
	value["operation"] = &operation
	message.Operation = operation

	message.Value = value
	message.MaskSchema = make(map[string]serializer.MaskInfo)

	return nil
}
