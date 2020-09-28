package redshiftloader

import (
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
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
		{
			"name": "maskSchema",
			"type": {
				"type": "array",
				"items": {
					"name": "maskSchema_record",
					"type": "record",
					"fields": [
						{
							"name": "Name",
							"type": "string"
						},
						{
							"name": "Masked",
							"type": "boolean"
						},
						{
							"name": "SortCol",
							"type": "boolean"
						},
						{
							"name": "DistCol",
							"type": "boolean"
						},
						{
							"name": "LengthCol",
							"type": "boolean"
						}
					]
				}
			}
		}
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

type maskSchema struct {
	Name      string
	Masked    bool
	SortCol   bool
	DistCol   bool
	LengthCol bool
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
			if value, ok := v.([]maskSchema); ok {
				for _, s := range value {
					schema[s.Name] = serializer.MaskInfo{
						Masked:    s.Masked,
						SortCol:   s.SortCol,
						DistCol:   s.DistCol,
						LengthCol: s.LengthCol,
					}
				}
			}
			job.MaskSchema = schema
		}

	}

	return job
}

// ToStringMap returns a map representation of the Job
func (c Job) ToStringMap() map[string]interface{} {
	datumIn := map[string]interface{}{
		"upstreamTopic": c.UpstreamTopic,
		"startOffset":   c.StartOffset,
		"endOffset":     c.EndOffset,
		"csvDialect":    c.CsvDialect,
		"s3Path":        c.S3Path,
		"schemaId":      c.SchemaId,
	}

	var mschema []maskSchema
	if len(c.MaskSchema) != 0 {
		for key, value := range c.MaskSchema {
			mschema = append(mschema, maskSchema{
				Name:      key,
				Masked:    value.Masked,
				SortCol:   value.SortCol,
				DistCol:   value.DistCol,
				LengthCol: value.LengthCol,
			})
		}
		datumIn["maskSchema"] = mschema
	}

	return datumIn
}
