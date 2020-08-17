package serializer

type Message struct {
	SchemaId  int
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Value     interface{}
}

func getValues(native interface{}) ([]string, error) {
        data := native.(map[string]interface{})

    	after := data["after"].(map[string]interface{})

        csv := make([]interface{}, 0)
    	for _, v := range after {
    		for _, v2 := range v.(map[string]interface{}) {
    			switch dtype := v2.(type) {
    			case map[string]interface{}:
    				for _, v3 := range v2.(map[string]interface{}) {
    					csv = append(csv, v3)
    				}
    			case string:
    				csv = append(csv, v2)
    			case int:
    				csv = append(csv, v2)
    			case float64:
    				csv = append(csv, v2)
    			default:
                    return csv,  fmt.Errorf("dataType %s not handled\n", dtype)
    			}
    		}
    	}

        return csv, nil
}
