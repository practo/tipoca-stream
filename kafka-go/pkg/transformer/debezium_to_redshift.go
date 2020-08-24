package transfomer

import (
    "github.com/practo/tipoca-stream/kafka-go/redshift"
    "github.com/practo/tipoca-stream/kafka-go/pkg/serializer"
)


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

// Transform debezium event into a s3 message annotating extra information
func (c *redshiftTransformer) TransformMessage(
    message *serializer.Message) error {

	d := &debeziumPayloadParser{
        payload: message.Value,
    }

	before := d.before()
	after := d.after()

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

// Transform debezium schema into redshift table
func (c *redshiftTransformer) TransformSchema(
    schemaStr string) (interface{}, error) {

    d := &debeziumSchemaParser{schemaStr: schemaStr, schema: Debezium{}}
    tableDelim := "."

    namespace := strings.Split(d.schema.Namespace, tableDelim)

    columns := d.ColumnsBefore()
    var redshiftColumns []redshift.ColInfo
    for _, column := range columns {
        redshiftColumns = append(redshiftColumns, redshift.ColInfo{
            Name: column.,
            Type: "",
            DefaultVal: "",
            NotNull: false,
            PrimaryKey: false,
            DistKey: false,
            SortOrdinal: 0,
        })
    }

    table := redshift.Table{
        Name:       namespace[len(namespace) - 1],
        Columns:    redshiftColumns,
        Meta:       redshift.Meta{
            DataDateColumn: "",
            Schema: strings.Join(namespace[0:len(namespace)-1], tableDelim),
        },
    }

    return table, nil
}
