package redshiftloader

var JobAvroSchema string = `{
    "type": "record",
    "name": "redshiftloader",
    "fields": [
        {"name": "upstreamTopic", "type": "string"},
        {"name": "startOffset", "type": "long"},
        {"name": "endOffset", "type": "long"},
        {"name": "csvDialect", "type": "string"},
        {"name": "s3Path", "type": "string"},
        {"name": "schemaId", "type": "int"}
    ]
}`

type Job struct {
	UpstreamTopic string `json:"upstreamTopic"`
	StartOffset   int64  `json:"startOffset"`
	EndOffset     int64  `json:"endOffset"`
	CsvDialect    string `json:"csvDialect"`
	S3Path        string `json:"s3Path"`
	SchemaId      int    `json:"schemaId"` // schema id of debezium event
}

func NewJob(
	upstreamTopic string, startOffset int64, endOffset int64,
	csvDialect string, s3Path string, schemaId int) Job {

	return Job{
		UpstreamTopic: upstreamTopic,
		StartOffset:   startOffset,
		EndOffset:     endOffset,
		CsvDialect:    csvDialect,
		S3Path:        s3Path,
		SchemaId:      schemaId,
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
			}
		}
	}

	return job
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
	}
}
