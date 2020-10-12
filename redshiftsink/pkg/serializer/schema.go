package serializer

import (
	"fmt"
	"github.com/practo/klog/v2"
	"github.com/riferrei/srclient"
	"math/rand"
	"time"
)

// GetSchemaWithRetry gets the latest schema with some retry on failure
// TOOD: move to this library if it works out well
// https://github.com/avast/retry-go
func GetSchemaWithRetry(client *srclient.SchemaRegistryClient,
	schemaId int, attempts int) (*srclient.Schema, error) {
	for i := 0; ; i++ {
		schema, err := client.GetSchema(schemaId)
		if err == nil {
			return schema, nil
		}
		if i >= (attempts - 1) {
			return nil, fmt.Errorf(
				"Failed to get schema by id: %d, err:%v\n", schemaId, err)
		}
		klog.Warningf(
			"Retrying.. Error fetching schema by id: %d err:%v\n",
			schemaId, err)
		sleepFor := rand.Intn(30-2+1) + 2
		time.Sleep(time.Duration(sleepFor) * time.Second)
	}
}

// GetLatestSchemaWithRetry gets the latest schema with some retry on failure
// TODO: move to this library if it works out well
// https://github.com/avast/retry-go
func GetLatestSchemaWithRetry(client *srclient.SchemaRegistryClient,
	topic string, isKey bool, attempts int) (*srclient.Schema, error) {
	for i := 0; ; i++ {
		schema, err := client.GetLatestSchema(topic, isKey)
		if err == nil {
			return schema, nil
		}
		if i >= (attempts - 1) {
			return nil, fmt.Errorf(
				"Failed to get latest schema, topic: %s, err:%v\n", topic, err)
		}
		klog.Warningf(
			"Retrying.. Error getting latest schema, topic:%s, err:%v\n",
			topic, err)
		sleepFor := rand.Intn(30-2+1) + 2
		time.Sleep(time.Duration(sleepFor) * time.Second)
	}
}
