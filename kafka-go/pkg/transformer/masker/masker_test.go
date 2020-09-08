package masker

import (
	"encoding/json"
	"github.com/practo/tipoca-stream/kafka-go/pkg/serializer"
	"os"
	"testing"
)

func TestNonPiiKeys(t *testing.T) {
	topic := "dbserver.inventory.customers"
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	masker, err := NewMsgMasker(dir, topic)
	if err != nil {
		t.Errorf("Error making masker, err: %v\n", err)
	}

	columns := map[string]string{
		"kafkaoffset": "1023",
		"operation":   "create",
		"id":          "1001",
		"first_name":  "Mother",
		"last_name":   "Teresa",
		"email":       "mother@example.org",
	}

	value, err := json.Marshal(columns)
	if err != nil {
		t.Error(err)
	}

	message := &serializer.Message{
		SchemaId:  int(1),
		Topic:     topic,
		Partition: 0,
		Offset:    0,
		Key:       "key",
		Value:     value,
	}

	err = masker.Transform(message)
	if err != nil {
		t.Error(err)
	}

	var maskedColumns map[string]string

	err = json.Unmarshal(message.Value.([]byte), &maskedColumns)
	if err != nil {
		t.Error(err)
	}

	if maskedColumns["id"] != "1001" {
		t.Errorf("Expected id=1001, got %v\n", maskedColumns["id"])
	}
	maskedFirstName := "79da9eaa3469eabd7dd1afb249048331b2d64341"
	if maskedColumns["first_name"] != maskedFirstName {
		t.Errorf(
			"Expected first_name=%v, got %v\n",
			maskedFirstName,
			maskedColumns["first_name"],
		)
	}
}
