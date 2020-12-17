package schemaregistry

import (
	"fmt"
	"github.com/linkedin/goavro/v2"
	"github.com/practo/klog/v2"
	"github.com/riferrei/srclient"
	"math/rand"
	"time"
)

// SchemaRegistry supports multiple schema registries.
// An adapter interface to support various schema registries out there!
// at present it supports only one(cSchemaRegistry)
type SchemaRegistry interface {
	GetSchema(schemaID int) (*Schema, error)
	GetLatestSchema(subject string, key bool) (*Schema, error)
	CreateSchema(subject string, scheme string, schemaType SchemaType, key bool) (*Schema, error)
}

type Schema struct {
	id      int
	schema  string
	version int
	codec   *goavro.Codec
}

func (schema *Schema) ID() int {
	return schema.id
}

func (schema *Schema) Schema() string {
	return schema.schema
}

func (schema *Schema) Version() int {
	return schema.version
}

func (schema *Schema) Codec() *goavro.Codec {
	return schema.codec
}

type SchemaType string

const (
	Avro SchemaType = "AVRO"
)

func NewRegistry(url string) SchemaRegistry {
	return &cSchemaRegistry{
		client: srclient.CreateSchemaRegistryClient(url),
	}
}

type cSchemaRegistry struct {
	client *srclient.SchemaRegistryClient
}

func (c *cSchemaRegistry) GetSchema(schemaID int) (*Schema, error) {
	cSchema, err := c.client.GetSchema(schemaID)
	if err != nil {
		return nil, err
	}

	return toSchema(cSchema), nil
}

func (c *cSchemaRegistry) GetLatestSchema(
	subject string, key bool) (*Schema, error) {
	cSchema, err := c.client.GetLatestSchema(subject, key)
	if err != nil {
		return nil, err
	}

	return toSchema(cSchema), nil
}

func (c *cSchemaRegistry) CreateSchema(
	subject string, schema string,
	schemaType SchemaType, key bool) (*Schema, error) {

	cSchema, err := c.client.CreateSchema(
		subject, schema, tocSchemaType(schemaType), key)
	if err != nil {
		return nil, err
	}

	return toSchema(cSchema), nil
}

func toSchema(cSchema *srclient.Schema) *Schema {
	return &Schema{
		id:      cSchema.ID(),
		schema:  cSchema.Schema(),
		version: cSchema.Version(),
		codec:   cSchema.Codec(),
	}
}

func tocSchemaType(schemaType SchemaType) srclient.SchemaType {
	switch schemaType {
	case Avro:
		return srclient.Avro
	}

	return ""
}

func GetSchemaWithRetry(
	registry SchemaRegistry,
	schemaId int,
	attempts int,
) (
	*Schema,
	error,
) {
	for i := 0; ; i++ {
		schema, err := registry.GetSchema(schemaId)
		if err == nil {
			return schema, nil
		}
		if i >= (attempts - 1) {
			return nil, fmt.Errorf(
				"Failed to get schema by id: %d, err:%v\n", schemaId, err)
		}
		klog.Warningf(
			"Retrying. Error fetching schema by id: %d err:%v\n",
			schemaId, err)
		sleepFor := rand.Intn(30-2+1) + 2
		time.Sleep(time.Duration(sleepFor) * time.Second)
	}
}

func GetLatestSchemaWithRetry(
	registry SchemaRegistry,
	topic string,
	key bool,
	attempts int,
) (
	*Schema,
	error,
) {
	for i := 0; ; i++ {
		schema, err := registry.GetLatestSchema(topic, key)
		if err == nil {
			return schema, nil
		}
		if i >= (attempts - 1) {
			return nil, fmt.Errorf(
				"Failed to get latest schema, topic: %s, err:%v\n", topic, err)
		}
		klog.Warningf(
			"Retrying. Error getting latest schema, topic:%s, err:%v\n",
			topic, err)
		sleepFor := rand.Intn(30-2+1) + 2
		time.Sleep(time.Duration(sleepFor) * time.Second)
	}
}
