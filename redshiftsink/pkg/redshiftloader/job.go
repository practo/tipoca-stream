package redshiftloader

import (
	"fmt"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"strings"
)

var JobAvroSchema string = `{
    "type": "record",
    "name": "redshiftloader",
    "fields": [
        {"name": "upstreamTopic", "type": "string"},
        {"name": "startOffset", "type": "long"},
        {"name": "endOffset", "type": "long"},
        {"name": "csvDialect", "type": "string"},
        {"name": "s3Path", "type": "string"},
        {"name": "schemaId", "type": "int"},
        {"name": "maskSchema", "type": "string"}
    ]
}`

type Job struct {
	UpstreamTopic string                         `json:"upstreamTopic"`
	StartOffset   int64                          `json:"startOffset"`
	EndOffset     int64                          `json:"endOffset"`
	CsvDialect    string                         `json:"csvDialect"`
	S3Path        string                         `json:"s3Path"`
	SchemaId      int                            `json:"schemaId"` // schema id of debezium event
	MaskSchema    map[string]serializer.MaskInfo `json:"maskSchema"`
}

func NewJob(
	upstreamTopic string, startOffset int64, endOffset int64,
	csvDialect string, s3Path string, schemaId int,
	maskSchema map[string]serializer.MaskInfo) Job {

	return Job{
		UpstreamTopic: upstreamTopic,
		StartOffset:   startOffset,
		EndOffset:     endOffset,
		CsvDialect:    csvDialect,
		S3Path:        s3Path,
		SchemaId:      schemaId,
		MaskSchema:    maskSchema,
	}
}

// StringMapToUser returns a User from a map representation of the User.
func StringMapToJob(data map[string]interface{}) Job {
	job := Job{}
	for k, v := range data {
		switch k {
		case "upstreamTopic":
			if value, ok := v.(string); ok {
				job.UpstreamTopic = value
			}
		case "startOffset":
			if value, ok := v.(int64); ok {
				job.StartOffset = value
			}
		case "endOffset":
			if value, ok := v.(int64); ok {
				job.EndOffset = value
			}
		case "csvDialect":
			if value, ok := v.(string); ok {
				job.CsvDialect = value
			}
		case "s3Path":
			if value, ok := v.(string); ok {
				job.S3Path = value
			}
		case "schemaId":
			if value, ok := v.(int32); ok {
				job.SchemaId = int(value)
			} else if value, ok := v.(int); ok {
				job.SchemaId = value
			}
		case "maskSchema":
			schema := make(map[string]serializer.MaskInfo)
			if value, ok := v.(string); ok {
				schema = ToSchemaMap(value)
			}
			job.MaskSchema = schema
		}

	}

	return job
}

// TODO: hack, to release fast, found unwanted complications in
// using map[string]interface in goavro(will revisit)
func ToSchemaMap(r string) map[string]serializer.MaskInfo {
	m := make(map[string]serializer.MaskInfo)

	columns := strings.Split(r, "|")
	if len(columns) == 0 {
		return m
	}

	for _, col := range columns {
		if col == "" {
			continue
		}

		info := strings.Split(col, ",")
		name := info[0]
		var masked, sortCol, distCol, lengthCol bool
		if info[1] == "true" {
			masked = true
		}
		if info[2] == "true" {
			sortCol = true
		}
		if info[3] == "true" {
			distCol = true
		}
		if info[4] == "true" {
			lengthCol = true
		}

		m[name] = serializer.MaskInfo{
			Masked:    masked,
			SortCol:   sortCol,
			DistCol:   distCol,
			LengthCol: lengthCol,
		}
	}

	return m
}

// TODO: hack, to release fast, found unwanted complications in
// using map[string]interface in goavro (will revisit)
func ToSchemaString(m map[string]serializer.MaskInfo) string {
	var r string

	for name, info := range m {
		col := fmt.Sprintf(
			"%s,%t,%t,%t,%t",
			name,
			info.Masked,
			info.SortCol,
			info.DistCol,
			info.LengthCol,
		)
		r = r + col + "|"
	}

	return r
}

// ToStringMap returns a map representation of the Job
func (c Job) ToStringMap() map[string]interface{} {
	return map[string]interface{}{
		"upstreamTopic": c.UpstreamTopic,
		"startOffset":   c.StartOffset,
		"endOffset":     c.EndOffset,
		"csvDialect":    c.CsvDialect,
		"s3Path":        c.S3Path,
		"schemaId":      c.SchemaId,
		"maskSchema":    ToSchemaString(c.MaskSchema),
	}
}