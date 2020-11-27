package masker

import (
	"os"
	"path/filepath"
	"testing"
)

func testMasked(t *testing.T, topic, table, cName, cValue string,
	result bool, allColumns map[string]*string) {

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	configFilePath := filepath.Join(dir, "database.yaml")

	m, err := NewMaskConfig(topic, configFilePath, "")
	if err != nil {
		t.Error(err)
	}

	gotResult := m.PerformUnMasking(table, cName, &cValue, allColumns)
	if gotResult != result {
		t.Errorf(
			"Expected column: %v to have mask=%v in table:%v, got mask=%v\n",
			cName, result, table, gotResult,
		)
	}
}

func TestMaskConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		topic      string
		table      string
		cName      string
		cValue     string
		unMasked   bool
		allColumns map[string]*string
	}{
		{
			name:     "test1: test column is unmasked(case insensitive)",
			topic:    "dbserver.database.justifications",
			table:    "justifications",
			cName:    "createdat",
			cValue:   "2020-09-29 06:51:32",
			unMasked: true,
			allColumns: map[string]*string{
				"createdAt": stringPtr("2020-09-29 06:51:32"),
			},
		},
		{
			name:     "test2: test column is masked",
			topic:    "dbserver.database.justifications",
			table:    "justifications",
			cName:    "source",
			cValue:   "android",
			unMasked: true,
			allColumns: map[string]*string{
				"source": stringPtr("android"),
			},
		},
		{
			name:     "test3: test dependentNonPii with matching value",
			topic:    "dbserver.database.customers",
			table:    "customers",
			cName:    "first_name",
			cValue:   "Sally",
			unMasked: true,
			allColumns: map[string]*string{
				"first_name": stringPtr("Sally"),
				"last_name":  stringPtr("Jones"),
			},
		},
		{
			name:     "test4: test dependentNonPii with unmatching value",
			topic:    "dbserver.database.customers",
			table:    "customers",
			cName:    "first_name",
			cValue:   "Sally",
			unMasked: false,
			allColumns: map[string]*string{
				"first_name": stringPtr("Sally"),
				"last_name":  stringPtr("Patel"),
			},
		},
		{
			name:     "test5: test dependentNonPii with column not defined",
			topic:    "dbserver.database.customers",
			table:    "customers",
			cName:    "first_name",
			cValue:   "Sally",
			unMasked: false,
			allColumns: map[string]*string{
				"first_name": stringPtr("SallyNotDefined"),
				"last_name":  stringPtr("Patel"),
			},
		},
		{
			name:     "test6: test conditonalNonPii with matching value",
			topic:    "dbserver.database.customers",
			table:    "customers",
			cName:    "email",
			cValue:   "customer@example.com",
			unMasked: true,
			allColumns: map[string]*string{
				"first_name": stringPtr("Customer"),
				"last_name":  stringPtr("Hawking"),
				"email":      stringPtr("customer@example.com"),
			},
		},
		{
			name:     "test7: test conditonalNonPii with unmatching value",
			topic:    "dbserver.database.customers",
			table:    "customers",
			cName:    "email",
			cValue:   "customer@practo.com",
			unMasked: false,
			allColumns: map[string]*string{
				"first_name": stringPtr("Customer"),
				"last_name":  stringPtr("Hawking"),
				"email":      stringPtr("customer@practo.com"),
			},
		},
		{
			name:     "test8: mask test when field is not in config",
			topic:    "dbserver.database.customers",
			table:    "customers",
			cName:    "dob",
			cValue:   "customer@practo.com",
			unMasked: false,
			allColumns: map[string]*string{
				"first_name": stringPtr("Customer"),
				"last_name":  stringPtr("Hawking"),
				"email":      stringPtr("customer@practo.com"),
				"dob":        stringPtr("1998-01-10"),
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testMasked(
				t, tc.topic, tc.table, tc.cName,
				tc.cValue, tc.unMasked, tc.allColumns,
			)
		})
	}
}
