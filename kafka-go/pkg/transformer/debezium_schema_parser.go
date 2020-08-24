package transformer

import (

)

type debeziumSchemaParser struct{
    schema map[string]interface{}
}

func (d *debeziumSchemaParser) namespace() string {
    return d["namespace"]
}
