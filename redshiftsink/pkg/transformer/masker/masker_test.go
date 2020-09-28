package masker

import (
	"encoding/json"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"os"
	"testing"
)

func TestSaltMask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		salt      string
		data      string
		resultVal string
	}{
		{
			name:      "test id",
			salt:      "testhash",
			data:      "275402",
			resultVal: "95b623a5d57372c26025828015f537ad42104f9c",
		},
		{
			name:      "test string",
			salt:      "testhash",
			data:      "Batman",
			resultVal: "9ba53e85b996f6278aa647d8da8f355aafd16149",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			r := mask(tc.data, tc.salt)
			if tc.resultVal != *r {
				t.Errorf("expected: %v, got: %v\n", tc.resultVal, *r)
			}
		})
	}
}

func testMasker(t *testing.T, salt, dir, topic, cName string,
	columns map[string]*string, result *string,
	resultMaskSchema map[string]serializer.MaskInfo) {

	masker, err := NewMsgMasker(salt, dir, topic)
	if err != nil {
		t.Fatalf("Error making masker, err: %v\n", err)
	}

	value, err := json.Marshal(columns)
	if err != nil {
		t.Fatal(err)
	}
	message := &serializer.Message{
		SchemaId:   int(1),
		Topic:      topic,
		Partition:  0,
		Offset:     0,
		Key:        "key",
		Value:      value,
		MaskSchema: make(map[string]serializer.MaskInfo),
	}
	err = masker.Transform(message, redshift.Table{})
	if err != nil {
		t.Fatal(err)
	}
	var maskedColumns map[string]*string
	err = json.Unmarshal(message.Value.([]byte), &maskedColumns)
	if err != nil {
		t.Fatal(err)
	}

	if len(resultMaskSchema) > 0 {
		for column, maskInfo := range resultMaskSchema {
			maskColumn, ok := message.MaskSchema[column]
			if !ok {
				t.Errorf("column=%v, maskColumn=%v missing\n", column, maskInfo)
				continue
			}
			if maskColumn.Masked != maskInfo.Masked ||
				maskColumn.SortCol != maskInfo.SortCol ||
				maskColumn.DistCol != maskInfo.DistCol ||
				maskColumn.LengthCol != maskInfo.LengthCol {
				t.Errorf(
					"column=%v, maskColumn=%+v does not match %+v\n",
					column, maskColumn, maskInfo)
			}
		}
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

func TestMasker(t *testing.T) {
	t.Parallel()

	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	salt := "testhash"

	tests := []struct {
		name             string
		topic            string
		cName            string
		columns          map[string]*string
		resultVal        *string
		resultMaskSchema map[string]serializer.MaskInfo
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
			resultVal:        stringPtr("1001"),
			resultMaskSchema: make(map[string]serializer.MaskInfo),
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
			resultVal: stringPtr(
				"9ba53e85b996f6278aa647d8da8f355aafd16149"),
			resultMaskSchema: make(map[string]serializer.MaskInfo),
		},
		{
			name:  "test3: never mask nil columns",
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
			resultVal:        nil,
			resultMaskSchema: make(map[string]serializer.MaskInfo),
		},
		{
			name:  "test4: mask with case insensitivity",
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
			resultVal:        stringPtr("2020-09-20 20:56:45"),
			resultMaskSchema: make(map[string]serializer.MaskInfo),
		},
		{
			name:  "test5: length keys",
			topic: "dbserver.database.customers",
			cName: "email_length",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"operation":   stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   nil,
				"email":       stringPtr("customer@example.com"),
			},
			resultVal:        stringPtr("20"),
			resultMaskSchema: make(map[string]serializer.MaskInfo),
		},
		{
			name:  "test6: conditionalNonPii unmasking(no match)",
			topic: "dbserver.database.customers",
			cName: "email",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"operation":   stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   stringPtr("DhoniUnmatched"),
				"email":       stringPtr("customer@practo.com"),
			},
			resultVal:        stringPtr("d129eef03b45b9679db4d35922786281ee805877"),
			resultMaskSchema: make(map[string]serializer.MaskInfo),
		},
		{
			name:  "test7: dependentNonPii unmasking(match)",
			topic: "dbserver.database.customers",
			cName: "first_name",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"operation":   stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   stringPtr("Dhoni"),
				"email":       stringPtr("customer@example.com"),
			},
			resultVal:        stringPtr("Batman"),
			resultMaskSchema: make(map[string]serializer.MaskInfo),
		},
		{
			name:  "test8: dependentNonPii unmasking(no match)",
			topic: "dbserver.database.customers",
			cName: "first_name",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"operation":   stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   stringPtr("DhoniUnmatched"),
				"email":       stringPtr("customer@example.com"),
			},
			resultVal: stringPtr("9ba53e85b996f6278aa647d8da8f355aafd16149"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"kafkaoffset": serializer.MaskInfo{},
				"operation":   serializer.MaskInfo{},
				"id": serializer.MaskInfo{
					Masked: false, SortCol: true},
				"first_name": serializer.MaskInfo{Masked: true}, // first name may not be masked but masked should always come as true as it is depdenent Non Pii
				"last_name":  serializer.MaskInfo{Masked: true},
				"email": serializer.MaskInfo{
					Masked: true, DistCol: true, LengthCol: true},
			},
		},
		{
			name:  "test9: mask schema test when field is not in config)",
			topic: "dbserver.database.customers",
			cName: "dob",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"operation":   stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   stringPtr("DhoniUnmatched"),
				"email":       stringPtr("customer@example.com"),
				"dob":         stringPtr("1998-01-10"),
			},
			resultVal: stringPtr("b944b9b788724a0c474c5758e55529ebd44e7d48"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"kafkaoffset": serializer.MaskInfo{},
				"operation":   serializer.MaskInfo{},
				"id": serializer.MaskInfo{
					Masked: false, SortCol: true},
				"first_name": serializer.MaskInfo{Masked: true}, // first name may not be masked but masked should always come as true as it is depdenent Non Pii
				"last_name":  serializer.MaskInfo{Masked: true},
				"email": serializer.MaskInfo{
					Masked: true, DistCol: true, LengthCol: true},
				"dob": serializer.MaskInfo{Masked: true},
			},
		},
		{
			name:  "test10: mask with case insensitivity (sort keys)",
			topic: "dbserver.database.justifications",
			cName: "createdat", // lower case
			columns: map[string]*string{
				"source":    stringPtr("chrome"),
				"type":      stringPtr("CLASS"),
				"createdat": stringPtr("2020-09-20 20:56:45"), // lower case
				"email":     stringPtr("customer@example.com"),
			},
			resultVal: stringPtr("2020-09-20 20:56:45"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"source":    serializer.MaskInfo{DistCol: true},
				"type":      serializer.MaskInfo{},
				"createdat": serializer.MaskInfo{SortCol: true},
				"email":     serializer.MaskInfo{Masked: true},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testMasker(
				t, salt, dir, tc.topic,
				tc.cName, tc.columns, tc.resultVal, tc.resultMaskSchema)
		})
	}
}
