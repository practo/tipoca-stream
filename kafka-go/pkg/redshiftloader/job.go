package redshiftloader

const JobAvroSchema = `{
    "type": "record",
    "name": "redshiftloader",
    "fields": [
        {"name": "upstreamTopic", "type": "string"},
        {"name": "startOffset", "type": "long"},
        {"name": "endOffset", "type": "long"},
        {"name": "csvDialect", "type": "string"},
        {"name": "s3Path", "type": "string"},
        {"name": "schemaID", "type": "int"}
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
