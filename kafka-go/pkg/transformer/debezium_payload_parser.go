package transformer

import (
	"fmt"
)

type debeziumPayloadParser struct {
	payload interface{}
}

func (d *debeziumPayloadParser) extract(
	key string, payload map[string]interface{},
	result map[string]string) map[string]string {

	dataKey := payload[key]
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

func (d *debeziumPayloadParser) after() map[string]string {
	result := make(map[string]string)
	if d.payload == nil {
		return result
	}

	data := d.payload.(map[string]interface{})
	if data == nil {
		return result
	}

	return d.extract("after", data, result)
}

func (d *debeziumPayloadParser) before() map[string]string {
	result := make(map[string]string)
	if d.payload == nil {
		return result
	}

	data := d.payload.(map[string]interface{})
	if data == nil {
		return result
	}

	return d.extract("before", data, result)
}
