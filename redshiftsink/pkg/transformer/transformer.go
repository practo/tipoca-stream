package transformer

import (
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"strings"
)

const (
	TempTablePrimary     = "kafkaoffset"
	TempTablePrimaryType = "character varying(max)"
	TempTableOp          = "debeziumop"
	TempTableOpType      = "character varying(6)"
	LengthColumnSuffix   = "_length"
	MobileCoulmnSuffix   = "_init5"
)

type MessageTransformer interface {
	Transform(message *serializer.Message, table redshift.Table) error
}

type SchemaTransformer interface {
	// TransformKey transforms the topic schema into name of the primary
	// key and its type.
	TransformKey(topic string) (string, string, error)
	// Transform value transforms the schemaId for various use cases.
	// it uses maskSchema to change the type of the schema datatypes if required
	TransformValue(topic string, schemaId int,
		maskSchema map[string]serializer.MaskInfo) (interface{}, error)
}

// ParseTopic breaks down the topic string into server, database, table
func ParseTopic(topic string) (string, string, string) {
	t := strings.Split(topic, ".")
	return t[0], t[1], t[2]
}
