package redshift

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

func testRedshiftDataTypeGet(t *testing.T, sqlType, debeziumType,
	sourceColType, sourceColLength, sourceColumnScale string,
	columnMasked bool, expectedResult string) error {
	redshiftType, err := GetRedshiftDataType(
		sqlType, debeziumType, sourceColType, sourceColLength, columnMasked,
	)
	if err != nil {
		return err
	}
	if redshiftType != expectedResult {
		return fmt.Errorf(
			"expected=%s got=%v\n",
			expectedResult,
			redshiftType)
	}

	return nil
}

func TestRedshiftDataTypeGet(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		sqlType         string
		debeziumType    string
		sourceColType   string
		sourceColLength string
		columnMasked    bool
		expectedResult  string
		expectError     bool
	}{
		{
			name:            "test1: mysql to redshift conversion",
			sqlType:         "mysql",
			debeziumType:    "long",
			sourceColType:   "LONGTEXT",
			sourceColLength: "",
			columnMasked:    false,
			expectedResult:  "character varying(65535)",
			expectError:     false,
		},
		{
			name:            "test2: unknown mysql type",
			sqlType:         "mysql",
			debeziumType:    "long",
			sourceColType:   "UNKNOWN_TYPE",
			sourceColLength: "",
			columnMasked:    false,
			expectedResult:  "bigint",
			expectError:     false,
		},
		{
			name:            "test3: unsupported sqltype",
			sqlType:         "mongo",
			debeziumType:    "long",
			sourceColType:   "VARCHAR",
			sourceColLength: "",
			columnMasked:    false,
			expectedResult:  "",
			expectError:     true,
		},
		{
			name:            "test4: test datatype",
			sqlType:         "mysql",
			debeziumType:    "string",
			sourceColType:   "MEDIUMTEXT",
			sourceColLength: "",
			columnMasked:    false,
			expectedResult:  "character varying(65535)",
			expectError:     false,
		},
		{
			name:            "test5: test masked column datatype",
			sqlType:         "mysql",
			debeziumType:    "int",
			sourceColType:   "INTEGER",
			sourceColLength: "",
			columnMasked:    true,
			expectedResult:  RedshiftMaskedDataType,
			expectError:     false,
		},
		{
			name:            "test5: test datetime masking",
			sqlType:         "mysql",
			debeziumType:    "timestamp",
			sourceColType:   "datetime",
			sourceColLength: "",
			columnMasked:    true,
			expectedResult:  RedshiftMaskedDataType,
			expectError:     false,
		},
		{
			name:            "test6: test type double",
			sqlType:         "mysql",
			debeziumType:    "double",
			sourceColType:   "double",
			sourceColLength: "",
			columnMasked:    false,
			expectedResult:  "double precision",
			expectError:     false,
		},
		{
			name:            "test7: test default length when masked",
			sqlType:         "mysql",
			debeziumType:    "string",
			sourceColType:   "VARCHAR",
			sourceColLength: "",
			columnMasked:    true,
			expectedResult:  "character varying(50)",
			expectError:     false,
		},
		{
			name:            "test8: test default length when unmasked",
			sqlType:         "mysql",
			debeziumType:    "string",
			sourceColType:   "VARCHAR",
			sourceColLength: "",
			columnMasked:    false,
			expectedResult:  "character varying(256)",
			expectError:     false,
		},
		{
			name:            "test9: test length, masked, mid bound, ratio should apply",
			sqlType:         "mysql",
			debeziumType:    "string",
			sourceColType:   "VARCHAR",
			sourceColLength: "255",
			columnMasked:    true,
			expectedResult:  "character varying(1020)",
			expectError:     false,
		},
		{
			name:            "test10: test length, unmasked, mid bound, ratio should apply",
			sqlType:         "mysql",
			debeziumType:    "string",
			sourceColType:   "VARCHAR",
			sourceColLength: "255",
			columnMasked:    false,
			expectedResult:  "character varying(1020)",
			expectError:     false,
		},
		{
			name:            "test11: test length, upper bound should apply when it crosses it",
			sqlType:         "mysql",
			debeziumType:    "string",
			sourceColType:   "VARCHAR",
			sourceColLength: "40000",
			columnMasked:    true,
			expectedResult:  "character varying(65535)",
			expectError:     false,
		},
		{
			name:            "test12: test length, lower bound should apply when masked",
			sqlType:         "mysql",
			debeziumType:    "string",
			sourceColType:   "VARCHAR",
			sourceColLength: "5",
			columnMasked:    true,
			expectedResult:  "character varying(50)",
			expectError:     false,
		},
		{
			name:            "test13: test length, lower bound should not apply when unmasked",
			sqlType:         "mysql",
			debeziumType:    "string",
			sourceColType:   "VARCHAR",
			sourceColLength: "5",
			columnMasked:    false,
			expectedResult:  "character varying(20)",
			expectError:     false,
		},
		{
			name:            "test14: test length, for integer, code not ready so should return default, unmasked",
			sqlType:         "mysql",
			debeziumType:    "int",
			sourceColType:   "integer",
			sourceColLength: "10",
			columnMasked:    false,
			expectedResult:  "integer",
			expectError:     false,
		},
		{
			name:            "test15: test length, for integer, code not ready so should return default, masked",
			sqlType:         "mysql",
			debeziumType:    "int",
			sourceColType:   "integer",
			sourceColLength: "10",
			columnMasked:    true,
			expectedResult:  "character varying(50)",
			expectError:     false,
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
				tc.sourceColLength,
				tc.sourceColScale,
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
