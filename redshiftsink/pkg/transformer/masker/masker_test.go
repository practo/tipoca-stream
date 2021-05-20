package masker

import (
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"os"
	"path/filepath"
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
			r := Mask(tc.data, tc.salt)
			if tc.resultVal != *r {
				t.Errorf("expected: %v, got: %v\n", tc.resultVal, *r)
			}
		})
	}
}

func testMasker(t *testing.T, salt, topic, cName string,
	columns map[string]*string, result *string,
	resultMaskSchema map[string]serializer.MaskInfo,
	redshiftTable redshift.Table) {

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	configFilePath := filepath.Join(dir, "database.yaml")
	maskConfig, err := NewMaskConfig("/", configFilePath, "", "")
	if err != nil {
		t.Fatalf("Error making mask config: %v", err)
	}
	masker := NewMsgMasker(salt, topic, maskConfig)

	message := &serializer.Message{
		SchemaId:   int(1),
		Topic:      topic,
		Partition:  0,
		Offset:     0,
		Key:        "key",
		Value:      columns,
		MaskSchema: make(map[string]serializer.MaskInfo),
	}
	err = masker.Transform(message, redshiftTable)
	if err != nil {
		t.Fatal(err)
	}

	maskedColumns, ok := message.Value.(map[string]*string)
	if !ok {
		t.Fatalf("Error converting message value, message:%+v\n", message)
	}

	if len(resultMaskSchema) > 0 {
		for column, maskInfo := range resultMaskSchema {
			maskColumn, ok := message.MaskSchema[column]
			if !ok {
				t.Errorf("column=%+v, maskColumn=%+v missing\n", column, maskInfo)
				continue
			}
			if maskColumn.Masked != maskInfo.Masked ||
				maskColumn.SortCol != maskInfo.SortCol ||
				maskColumn.DistCol != maskInfo.DistCol ||
				maskColumn.LengthCol != maskInfo.LengthCol ||
				maskColumn.MobileCol != maskInfo.MobileCol {
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

	if result != nil && maskedColumns != nil {
		if *maskedColumns[cName] != *result {
			t.Errorf(
				"Expected %s=%s, got %v\n", cName, *result, *maskedColumns[cName],
			)
		}
	}
}

func TestMasker(t *testing.T) {
	t.Parallel()
	salt := "testhash"

	tests := []struct {
		name             string
		topic            string
		cName            string
		columns          map[string]*string
		resultVal        *string
		resultMaskSchema map[string]serializer.MaskInfo
		redshiftTable    redshift.Table
	}{
		{
			name:  "test1: unmask test",
			topic: "dbserver.database.customers",
			cName: "id",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"debeziumop":  stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   nil,
				"email":       stringPtr("customer@example.com"),
			},
			resultVal:        stringPtr("1001"),
			resultMaskSchema: make(map[string]serializer.MaskInfo),
			redshiftTable:    redshift.Table{},
		},
		{
			name:  "test2: mask test",
			topic: "dbserver.database.customers",
			cName: "first_name",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"debeziumop":  stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   nil,
				"email":       stringPtr("customer@example.com"),
			},
			resultVal: stringPtr(
				"9ba53e85b996f6278aa647d8da8f355aafd16149"),
			resultMaskSchema: make(map[string]serializer.MaskInfo),
			redshiftTable:    redshift.Table{},
		},
		{
			name:  "test3: never mask nil columns",
			topic: "dbserver.database.customers",
			cName: "last_name",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"debeziumop":  stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   nil,
				"email":       stringPtr("customer@example.com"),
			},
			resultVal:        nil,
			resultMaskSchema: make(map[string]serializer.MaskInfo),
			redshiftTable:    redshift.Table{},
		},
		{
			name:  "test4: mask with case insensitivity",
			topic: "dbserver.database.justifications",
			cName: "createdAt",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"debeziumop":  stringPtr("create"),
				"source":      stringPtr("chrome"),
				"type":        stringPtr("CLASS"),
				"createdAt":   stringPtr("2020-09-20 20:56:45"),
				"email":       stringPtr("customer@example.com"),
			},
			resultVal:        stringPtr("2020-09-20 20:56:45"),
			resultMaskSchema: make(map[string]serializer.MaskInfo),
			redshiftTable:    redshift.Table{},
		},
		{
			name:  "test5: length keys",
			topic: "dbserver.database.customers",
			cName: "email_length",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"debeziumop":  stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   nil,
				"email":       stringPtr("customer@example.com"),
			},
			resultVal:        stringPtr("20"),
			resultMaskSchema: make(map[string]serializer.MaskInfo),
			redshiftTable:    redshift.Table{},
		},
		{
			name:  "test6: conditionalNonPii unmasking(no match)",
			topic: "dbserver.database.customers",
			cName: "email",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"debeziumop":  stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   stringPtr("DhoniUnmatched"),
				"email":       stringPtr("customer@practo.com"),
			},
			resultVal:        stringPtr("d129eef03b45b9679db4d35922786281ee805877"),
			resultMaskSchema: make(map[string]serializer.MaskInfo),
			redshiftTable:    redshift.Table{},
		},
		{
			name:  "test7: dependentNonPii unmasking(match)",
			topic: "dbserver.database.customers",
			cName: "first_name",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"debeziumop":  stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   stringPtr("Dhoni"),
				"email":       stringPtr("customer@example.com"),
			},
			resultVal:        stringPtr("Batman"),
			resultMaskSchema: make(map[string]serializer.MaskInfo),
			redshiftTable:    redshift.Table{},
		},
		{
			name:  "test8: dependentNonPii unmasking(no match)",
			topic: "dbserver.database.customers",
			cName: "first_name",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"debeziumop":  stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   stringPtr("DhoniUnmatched"),
				"email":       stringPtr("customer@example.com"),
			},
			resultVal: stringPtr("9ba53e85b996f6278aa647d8da8f355aafd16149"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"kafkaoffset": serializer.MaskInfo{},
				"debeziumop":  serializer.MaskInfo{},
				"id": serializer.MaskInfo{
					Masked: false, SortCol: true},
				"first_name": serializer.MaskInfo{Masked: true}, // first name may not be masked but masked should always come as true as it is depdenent Non Pii
				"last_name":  serializer.MaskInfo{Masked: true},
				"email": serializer.MaskInfo{
					Masked: true, DistCol: true, LengthCol: true},
			},
			redshiftTable: redshift.Table{},
		},
		{
			name:  "test9: mask schema test when field is not in config)",
			topic: "dbserver.database.customers",
			cName: "dob",
			columns: map[string]*string{
				"kafkaoffset": stringPtr("87"),
				"debeziumop":  stringPtr("create"),
				"id":          stringPtr("1001"),
				"first_name":  stringPtr("Batman"),
				"last_name":   stringPtr("DhoniUnmatched"),
				"email":       stringPtr("customer@example.com"),
				"dob":         stringPtr("1998-01-10"),
			},
			resultVal: stringPtr("b944b9b788724a0c474c5758e55529ebd44e7d48"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"kafkaoffset": serializer.MaskInfo{},
				"debeziumop":  serializer.MaskInfo{},
				"id": serializer.MaskInfo{
					Masked: false, SortCol: true},
				"first_name": serializer.MaskInfo{Masked: true}, // first name may not be masked but masked should always come as true as it is depdenent Non Pii
				"last_name":  serializer.MaskInfo{Masked: true},
				"email": serializer.MaskInfo{
					Masked: true, DistCol: true, LengthCol: true},
				"dob": serializer.MaskInfo{Masked: true},
			},
			redshiftTable: redshift.Table{},
		},
		{
			name:  "test10: case insensitivity (sort keys, dist keys)",
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
			redshiftTable: redshift.Table{},
		},
		{
			name:  "test11: case insensitivity1 (conditionalNonPii)",
			topic: "dbserver.database.justifications",
			cName: "reason",
			columns: map[string]*string{
				"justice": stringPtr("mohan"),
				"reason":  stringPtr("want"),
			},
			resultVal: stringPtr("want"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"justice": serializer.MaskInfo{Masked: true},
				"reason":  serializer.MaskInfo{Masked: true},
			},
			redshiftTable: redshift.Table{},
		},
		{
			name:  "test12: case insensitivity2 (conditionalNonPii)",
			topic: "dbserver.database.justifications",
			cName: "reason",
			columns: map[string]*string{
				"justice": stringPtr("mahatma"),
				"reason":  stringPtr("wanted"),
			},
			resultVal: stringPtr("f08c46950f7d175e58d4dd989f7475f3c8184ff3"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"justice": serializer.MaskInfo{Masked: true},
				"reason":  serializer.MaskInfo{Masked: true},
			},
			redshiftTable: redshift.Table{},
		},
		{
			name:  "test13: case insensitivity (dependentNonPii)",
			topic: "dbserver.database.justifications",
			cName: "justice",
			columns: map[string]*string{
				"justice": stringPtr("mahatma"),
				"reason":  stringPtr("want"),
			},
			resultVal: stringPtr("mahatma"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"justice": serializer.MaskInfo{Masked: true},
				"reason":  serializer.MaskInfo{Masked: true},
			},
			redshiftTable: redshift.Table{},
		},
		{
			name:  "test14: mobile keys",
			topic: "dbserver.database.customers",
			cName: "mobile_number_init5",
			columns: map[string]*string{
				"mobile_number": stringPtr("+919812345678"),
			},
			resultVal:        stringPtr("+9198"),
			resultMaskSchema: make(map[string]serializer.MaskInfo),
			redshiftTable:    redshift.Table{},
		},
		{
			name:  "test14: mapping pii keys",
			topic: "dbserver.database.establishments",
			cName: "hashed_id",
			columns: map[string]*string{
				"id": stringPtr("2011"),
			},
			resultVal: stringPtr("9b8297b23539abcda0344522bca05a99feecba10"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"id":        serializer.MaskInfo{Masked: false},
				"hashed_id": serializer.MaskInfo{Masked: true},
			},
			redshiftTable: redshift.Table{},
		},
		{
			name:  "test15: mapping pii keys other values as unmasked",
			topic: "dbserver.database.establishments",
			cName: "id",
			columns: map[string]*string{
				"id": stringPtr("2011"),
			},
			resultVal: stringPtr("2011"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"id":        serializer.MaskInfo{Masked: false},
				"hashed_id": serializer.MaskInfo{Masked: true},
			},
			redshiftTable: redshift.Table{},
		},
		{
			name:  "test16: test missing column in message",
			topic: "dbserver.database.settings",
			cName: "plan_enabled",
			columns: map[string]*string{
				"id": stringPtr("2011"),
			},
			resultVal: nil,
			resultMaskSchema: map[string]serializer.MaskInfo{
				"plan_enabled": serializer.MaskInfo{Masked: true},
			},
			redshiftTable: redshift.Table{
				Columns: []redshift.ColInfo{
					redshift.ColInfo{
						Name: "id",
					},
					redshift.ColInfo{
						Name: "plan_enabled",
					},
				},
			},
		},
		{
			name:  "test17: conditional and non conditonal non pii datatype test",
			topic: "dbserver.database.customers",
			cName: "notes",
			columns: map[string]*string{
				"notes": stringPtr("I am not interested in politics"),
			},
			resultVal: stringPtr("I am not interested in politics"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"notes": serializer.MaskInfo{Masked: true},
			},
			redshiftTable: redshift.Table{
				Columns: []redshift.ColInfo{
					redshift.ColInfo{
						Name: "notes",
					},
				},
			},
		},
		{
			name:  "test18: boolean_keys regex matched, check value of main free text col",
			topic: "dbserver.database.customers",
			cName: "favourite_quote",
			columns: map[string]*string{
				"favourite_quote": stringPtr("Life would be tragic if it weren't funny"),
			},
			resultVal: stringPtr("3212da4cc0dc4c0023b912dcacab20f55feabb2e"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"favourite_quote":               serializer.MaskInfo{Masked: true},
				"favourite_quote_has_philosphy": serializer.MaskInfo{Masked: false},
			},
			redshiftTable: redshift.Table{},
		},
		{
			name:  "test19: boolean_keys regex match failed, check value of main free text col",
			topic: "dbserver.database.customers",
			cName: "favourite_quote",
			columns: map[string]*string{
				"favourite_quote": stringPtr("Wife would be tragic if she wasn't funny"),
			},
			resultVal: stringPtr("840ed39a7e650148cbdf0d516194d5c67c035e55"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"favourite_quote":               serializer.MaskInfo{Masked: true},
				"favourite_quote_has_philosphy": serializer.MaskInfo{Masked: false},
			},
			redshiftTable: redshift.Table{},
		},
		{
			name:  "test20: boolean_keys regex matched, check value of main free text col",
			topic: "dbserver.database.customers",
			cName: "favourite_quote_has_philosphy",
			columns: map[string]*string{
				"favourite_quote": stringPtr("Life would be tragic if it weren't funny"),
			},
			resultVal: stringPtr("true"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"favourite_quote":               serializer.MaskInfo{Masked: true},
				"favourite_quote_has_philosphy": serializer.MaskInfo{Masked: false},
			},
			redshiftTable: redshift.Table{},
		},
		{
			name:  "test21: boolean_keys regex match failed - check value of extra col",
			topic: "dbserver.database.customers",
			cName: "favourite_quote_has_philosphy",
			columns: map[string]*string{
				"favourite_quote": stringPtr("Wife would be tragic if she wasn't funny"),
			},
			resultVal: stringPtr("false"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"favourite_quote":               serializer.MaskInfo{Masked: true},
				"favourite_quote_has_philosphy": serializer.MaskInfo{Masked: false},
			},
			redshiftTable: redshift.Table{},
		},
		{
			name:  "test22 boolean_keys regex matched",
			topic: "dbserver.database.customers",
			cName: "favourite_food_has_pizza",
			columns: map[string]*string{
				"favourite_food": stringPtr("pizza,pasta,burgers,kebabs"),
			},
			resultVal: stringPtr("true"),
			resultMaskSchema: map[string]serializer.MaskInfo{
				"favourite_food":           serializer.MaskInfo{Masked: true},
				"favourite_food_has_pizza": serializer.MaskInfo{Masked: false},
			},
			redshiftTable: redshift.Table{},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testMasker(
				t, salt, tc.topic,
				tc.cName, tc.columns,
				tc.resultVal, tc.resultMaskSchema,
				tc.redshiftTable,
			)
		})
	}
}
