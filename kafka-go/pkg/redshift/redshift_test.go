package redshift

import (
	"testing"
)

func TestRedshiftDataTypeGet(t *testing.T) {
	redshiftType, err := GetRedshiftDataType("mysql", "long", "LONGTEXT")
	if err != nil {
		t.Error(err)
	}
	if redshiftType != "character varying(max)" {
		t.Errorf(
			"Expected redshiftType=character varying(max) got=%v\n",
			redshiftType)
	}

	redshiftType, err = GetRedshiftDataType("mysql", "long", "SOMERANDOM")
	if err != nil {
		t.Error(err)
	}
	if redshiftType != "int8" {
		t.Errorf("Expected redshiftType=int8 got=%v\n", redshiftType)
	}

	_, err = GetRedshiftDataType("mongo", "long", "SOMERANDOM")
	if err == nil {
		t.Error("Expected error to happen for unspported mongo test")
	}

	redshiftType, err = GetRedshiftDataType("mysql", "string", "MEDIUMTEXT")
	if err != nil {
		t.Error(err)
	}
	if redshiftType != "character varying(max)" {
		t.Errorf(
			"Expected redshiftType=character varying(max) got=%v\n",
			redshiftType)
	}
}
