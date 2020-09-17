package redshift

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

func TestRedshiftDataTypeGet(t *testing.T) {
	redshiftType, err := GetRedshiftDataType("mysql", "long", "LONGTEXT")
	if err != nil {
		t.Error(err)
	}
	if redshiftType != "character varying(65535)" {
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
	if redshiftType != "character varying(65535)" {
		t.Errorf(
			"Expected redshiftType=character varying(max) got=%v\n",
			redshiftType)
	}
}

func sortStringMap(m map[string]string, sortedMap map[string]string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		sortedMap[k] = m[k]
	}
}

// TODO: compare createTable output with GetTableMetadata's output
func TestTableMetadataToMysqlRedshiftMap(t *testing.T) {
	sortedMap := make(map[string]string)
	sortStringMap(mysqlToRedshiftTypeMap, sortedMap)

	createTable := `CREATE TABLE "inventory.inventory.typetest" (`
	count := 0
	var col string
	for _, value := range sortedMap {
		col = fmt.Sprintf(` "col_%d" %s,`, count, value)
		createTable = createTable + col
		count += 1
	}

	createTable = strings.TrimSuffix(createTable, ",")
	createTable = createTable + " );"

	fmt.Println(createTable)
}
