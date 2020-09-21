package masker

import (
	"os"
	"testing"
)

func testMasked(t *testing.T, dir, topic, table, cName string, result bool) {
	m, err := NewMaskConfig(dir, topic)
	if err != nil {
		t.Error(err)
	}

	gotResult := m.Masked(table, cName)
	if gotResult != result {
		t.Errorf(
			"Expected column: %v to have mask=%v in table:%v, got mask=%v\n",
			cName, result, table, gotResult,
		)
	}
}

func TestMaskConfigs(t *testing.T) {
	t.Parallel()

	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	tests := []struct {
		name           string
		topic          string
		table          string
		cName          string
		expectedResult bool
	}{
		{
			name:           "test1: test column is masked with case sensitive",
			topic:          "dbserver.database.justifications",
			table:          "justifications",
			cName:          "createdat",
			expectedResult: false,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testMasked(
				t, dir, tc.topic, tc.table, tc.cName, tc.expectedResult,
			)
		})
	}
}
