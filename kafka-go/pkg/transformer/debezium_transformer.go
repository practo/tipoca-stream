package transformer

import (
	"fmt"
)

type debeziumTransformer struct{}

func (d *debeziumTransformer) extract(
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

func (d *debeziumTransformer) after(native interface{}) map[string]string {
	result := make(map[string]string)
	if native == nil {
		return result
	}

	data := native.(map[string]interface{})
	if data == nil {
		return result
	}

	return d.extract("after", data, result)
}

func (d *debeziumTransformer) before(native interface{}) map[string]string {
	result := make(map[string]string)
	if native == nil {
		return result
	}

	data := native.(map[string]interface{})
	if data == nil {
		return result
	}

	return d.extract("before", data, result)
}
