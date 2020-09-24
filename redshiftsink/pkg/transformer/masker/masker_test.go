package masker

import (
	"encoding/json"
	"fmt"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"os"
	"testing"
)

func stringPtr(s string) *string {
	return &s
}

func testMask(t *testing.T, salt, dir, topic, cName string,
	columns map[string]*string, result *string) {

	masker, err := NewMsgMasker(salt, dir, topic)
	if err != nil {
		t.Errorf("Error making masker, err: %v\n", err)
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
	err = masker.Transform(message, redshift.Table{})
	if err != nil {
		t.Error(err)
	}
	var maskedColumns map[string]*string
	err = json.Unmarshal(message.Value.([]byte), &maskedColumns)
	if err != nil {
		t.Error(err)
	}

	if maskedColumns[cName] == nil {
		if maskedColumns[cName] != result {
			t.Errorf(
				"Expected %s=%v, got %v\n", cName, result, maskedColumns[cName],
			)
		}
		return
	}

	if *maskedColumns[cName] != *result {
		t.Errorf(
			"Expected %s=%s, got %v\n", cName, *result, *maskedColumns[cName],
		)
	}
}

func TestMaskTransformations(t *testing.T) {
	t.Parallel()

	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	salt := "testhash"

	tests := []struct {
		name           string
		topic          string
		cName          string
		columns        map[string]*string
		expectedResult *string
	}{
		{
			name:  "test1: unmask test",
			topic: "dbserver.database.customers",
			cName: "id",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"operation":   stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   nil,
				"email":       stringPtr("customer@example.com"),
			},
			expectedResult: stringPtr("1001"),
		},
		{
			name:  "test2: mask test",
			topic: "dbserver.database.customers",
			cName: "first_name",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"operation":   stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   nil,
				"email":       stringPtr("customer@example.com"),
			},
			expectedResult: stringPtr(
				"32b26a271530f105cbc35cb653110e1a49d019b6"),
		},
		{
			name:  "test3: mask test for nil columns",
			topic: "dbserver.database.customers",
			cName: "last_name",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"operation":   stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   nil,
				"email":       stringPtr("customer@example.com"),
			},
			expectedResult: nil,
		},
		{
			name:  "test4: mask test for case sensitivity",
			topic: "dbserver.database.justifications",
			cName: "createdAt",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"operation":   stringPtr("create"),
				"source":      stringPtr("chrome"),
				"type":        stringPtr("CLASS"),
				"createdAt":   stringPtr("2020-09-20 20:56:45"),
				"email":       stringPtr("customer@example.com"),
			},
			expectedResult: stringPtr("2020-09-20 20:56:45"),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testMask(
				t, salt, dir, tc.topic, tc.cName, tc.columns, tc.expectedResult)
		})
	}
}

func TestSalthash(t *testing.T) {
	salt := "testhash"
	data := "275402"
	expectedResult := "95b623a5d57372c26025828015f537ad42104f9c"
	gotResult := mask(data, salt)

	if *gotResult != expectedResult {
		t.Errorf("Expected: %v, Got: %v\n", expectedResult, *gotResult)
	}
}
