package redshift

import (
	"testing"
)

func TestLongTypeSchema(t *testing.T) {
	inputTable := Table{
		Name: "inputTable",
		Columns: []ColInfo{
			ColInfo{
				Name:       "kill_id",
				Type:       "long",
				DefaultVal: "",
				NotNull:    false,
				PrimaryKey: true,
			},
		},
		Meta: Meta{
			Schema: "schema",
		},
	}

	targetTable := Table{
		Name: "targetTable",
		Columns: []ColInfo{
			ColInfo{
				Name:       "kill_id",
				Type:       "character varying(65535)",
				DefaultVal: "",
				NotNull:    false,
				PrimaryKey: true,
			},
		},
		Meta: Meta{
			Schema: "schema",
		},
	}

	columnOps, err := CheckSchemas(inputTable, targetTable)
	if err != nil {
		t.Error(err)
	}

	if len(columnOps) > 0 {
		t.Error("Schema diff not expected")
	}
}

func TestRedshiftDataTypeGet(t *testing.T) {
	redshiftType, err := GetRedshiftDataType("mysql", "long", "LONGTEXT")
	if err != nil {
		t.Error(err)
	}
	if redshiftType != "VARCHAR(MAX)" {
		t.Errorf("Expected redshiftType=VARCHAR(MAX) got=%v\n", redshiftType)
	}

	redshiftType, err = GetRedshiftDataType("mysql", "long", "SOMERANDOM")
	if err != nil {
		t.Error(err)
	}
	if redshiftType != "long" {
		t.Errorf("Expected redshiftType=long got=%v\n", redshiftType)
	}

	redshiftType, err = GetRedshiftDataType("mongo", "long", "SOMERANDOM")
	if err == nil {
		t.Error("Expected error to happen for unspported mongo test")
	}
}
