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
	upstreamTopic string `json:upstreamTopic`
	startOffset   int64  `json:startOffset`
	endOffset     int64  `json:endOffset`
	csvDialect    string `json:csvDialect`
	s3Path        string `json:s3Path`
	schemaId      int    `json:schemaId` // schema id of debezium event
}

func NewJob(
	upstreamTopic string, startOffset int64, endOffset int64,
	csvDialect string, s3Path string, schemaId int) Job {

	return Job{
		upstreamTopic: upstreamTopic,
		startOffset:   startOffset,
		endOffset:     endOffset,
		csvDialect:    csvDialect,
		s3Path:        s3Path,
		schemaId:      schemaId,
	}
}

// StringMapToUser returns a User from a map representation of the User.
func StringMapToJob(data map[string]interface{}) Job {
	job := Job{}
	for k, v := range data {
		switch k {
		case "upstreamTopic":
			if value, ok := v.(string); ok {
				job.upstreamTopic = value
			}
		case "startOffset":
			if value, ok := v.(int64); ok {
				job.startOffset = value
			}
		case "endOffset":
			if value, ok := v.(int64); ok {
				job.endOffset = value
			}
		case "csvDialect":
			if value, ok := v.(string); ok {
				job.csvDialect = value
			}
		case "s3Path":
			if value, ok := v.(string); ok {
				job.s3Path = value
			}
		case "schemaId":
			if value, ok := v.(int); ok {
				job.schemaId = value
			}
		}
	}

	return job
}

// ToStringMap returns a map representation of the Job
func (c Job) ToStringMap() map[string]interface{} {
	return map[string]interface{}{
		"upstreamTopic": c.upstreamTopic,
		"startOffset":   c.startOffset,
		"endOffset":     c.endOffset,
		"csvDialect":    c.csvDialect,
		"s3Path":        c.s3Path,
		"schemaId":      c.schemaId,
	}
}

func (c Job) UpstreamTopic() string {
	return c.upstreamTopic
}

func (c Job) StartOffset() int64 {
	return c.startOffset
}

func (c Job) EndOffset() int64 {
	return c.endOffset
}

func (c Job) CsvDialect() string {
	return c.csvDialect
}

func (c Job) S3Path() string {
	return c.s3Path
}

func (c Job) SchemaId() int {
	return c.schemaId
}
