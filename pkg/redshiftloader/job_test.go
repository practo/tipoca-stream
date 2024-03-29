package redshiftloader

import (
	// "fmt"
	"github.com/practo/tipoca-stream/pkg/serializer"
	"reflect"
	"testing"
)

func TestToStringMap(t *testing.T) {
	maskSchema := map[string]serializer.MaskInfo{
		"kafkaoffset": serializer.MaskInfo{},
		"id":          serializer.MaskInfo{Masked: true},
	}
	extraMaskSchema := map[string]serializer.ExtraMaskInfo{}

	job := NewJob(
		"upstreamTopic",
		2091,
		2100,
		",",
		"s3path",
		1,
		2,
		maskSchema,
		extraMaskSchema,
		false,
		10,
		-1,
		-1,
		-1,
	)
	// fmt.Printf("job_now=%+v\n\n", job)

	sMap := job.ToStringMap()
	// fmt.Printf("sMap=%+v\n\n", sMap)

	jobOut := StringMapToJob(sMap)
	// fmt.Printf("jobOut=%+v\n\n", jobOut)

	if ok := reflect.DeepEqual(job, jobOut); !ok {
		t.Errorf("Compare Failed\njob=%+v\njobOut=%+v\n", job, jobOut)
	}
}
