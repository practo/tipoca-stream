package transformer

import (
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"strings"
)

const (
	LengthColumnSuffix = "_length"
)

type MessageTransformer interface {
	Transform(message *serializer.Message, table redshift.Table) error
}

type SchemaTransformer interface {
	// TransformKey transforms the topic schema into name of the primary
	// key and its type.
	TransformKey(topic string) (string, string, error)
	// Transform value transforms the schemaId for various use cases.
	// maskConfigDir is used to do the type change in schema for masked fields
	// only if masking is turned on, else it is passed as empty string ""
	// SortKey and DistKey are also set reading the mask configs if mask is true
	TransformValue(
		topic string, schemaId int, maskConfDir string) (interface{}, error)
}

// ParseTopic breaks down the topic string into server, database, table
func ParseTopic(topic string) (string, string, string) {
	t := strings.Split(topic, ".")
	return t[0], t[1], t[2]
}
