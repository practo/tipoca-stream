package redshift

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

func testRedshiftDataTypeGet(t *testing.T, sqlType, debeziumType,
	sourceColType string, columnMasked bool, expectedResult string) error {
	redshiftType, err := GetRedshiftDataType(
		sqlType, debeziumType, sourceColType, columnMasked,
	)
	if err != nil {
		return err
	}
	if redshiftType != expectedResult {
		return fmt.Errorf(
			"Expected redshiftType=character varying(max) got=%v\n",
			redshiftType)
	}

	return nil
}

func TestRedshiftDataTypeGet(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		sqlType        string
		debeziumType   string
		sourceColType  string
		columnMasked   bool
		expectedResult string
		expectError    bool
	}{
		{
			name:           "test1: mysql to redshift conversion",
			sqlType:        "mysql",
			debeziumType:   "long",
			sourceColType:  "LONGTEXT",
			columnMasked:   false,
			expectedResult: "character varying(65535)",
			expectError:    false,
		},
		{
			name:           "test2: unknown mysql type",
			sqlType:        "mysql",
			debeziumType:   "long",
			sourceColType:  "UNKNOWN_TYPE",
			columnMasked:   false,
			expectedResult: "bigint",
			expectError:    false,
		},
		{
			name:           "test3: unsupported sqltype",
			sqlType:        "mongo",
			debeziumType:   "long",
			sourceColType:  "VARCHAR",
			columnMasked:   false,
			expectedResult: "",
			expectError:    true,
		},
		{
			name:           "test4: test datatype",
			sqlType:        "mysql",
			debeziumType:   "string",
			sourceColType:  "MEDIUMTEXT",
			columnMasked:   false,
			expectedResult: "character varying(65535)",
			expectError:    false,
		},
		{
			name:           "test5: test masked column datatype",
			sqlType:        "mysql",
			debeziumType:   "int",
			sourceColType:  "INTEGER",
			columnMasked:   true,
			expectedResult: RedshiftMaskedDataType,
			expectError:    false,
		},
		{
			name:           "test5: test datetime masking",
			sqlType:        "mysql",
			debeziumType:   "timestamp",
			sourceColType:  "datetime",
			columnMasked:   true,
			expectedResult: RedshiftMaskedDataType,
			expectError:    false,
		},
		{
			name:           "test6: test type double",
			sqlType:        "mysql",
			debeziumType:   "double",
			sourceColType:  "double",
			columnMasked:   false,
			expectedResult: "double precision",
			expectError:    false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := testRedshiftDataTypeGet(
				t,
				tc.sqlType,
				tc.debeziumType,
				tc.sourceColType,
				tc.columnMasked,
				tc.expectedResult,
			)
			if err != nil && tc.expectError == false {
				t.Error(err)
			}
		})
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
