package debezium

import (
	"testing"
)

func TestConvertDebeziumFormattedTime(t *testing.T) {
	// t.Parallel()

	tests := []struct {
		name          string
		value         string
		sourceType    string
		sourceLength  string
		formattedTime string
	}{
		{
			name:          "test1: DATE > epoch",
			value:         "6807",
			sourceType:    "DATE",
			sourceLength:  "",
			formattedTime: "1988-08-21",
		},
		{
			name:          "test2: DATE < epoch",
			value:         "-13023",
			sourceType:    "DATE",
			sourceLength:  "",
			formattedTime: "1934-05-07",
		},
		{
			name:          "test3: TIMESTAMP",
			value:         "1988-08-21T14:01:02Z",
			sourceType:    "TIMESTAMP",
			sourceLength:  "",
			formattedTime: "1988-08-21 14:01:02",
		},
		{
			name:          "test4: DATETIME",
			value:         "588175262000",
			sourceType:    "DATETIME",
			sourceLength:  "",
			formattedTime: "1988-08-21 14:01:02",
		},
		{
			name:          "test5: DATETIME(0)",
			value:         "588175262230",
			sourceType:    "DATETIME",
			sourceLength:  "0",
			formattedTime: "1988-08-21 14:01:02",
		},
		{
			name:          "test6: DATETIME(2)",
			value:         "588175262230",
			sourceType:    "DATETIME",
			sourceLength:  "2",
			formattedTime: "1988-08-21 14:01:02.23",
		},
		{
			name:          "test7: DATETIME(6)",
			value:         "588175262234000",
			sourceType:    "DATETIME",
			sourceLength:  "6",
			formattedTime: "1988-08-21 14:01:02.234000",
		},
		{
			name:          "test8: DATETIME(6)",
			value:         "588175262123456",
			sourceType:    "DATETIME",
			sourceLength:  "6",
			formattedTime: "1988-08-21 14:01:02.123456",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			result, err := convertDebeziumFormattedTime(
				tc.value,
				tc.sourceType,
				tc.sourceLength,
			)
			if err != nil {
				t.Errorf("Error converting, %v\n", err)
			}
			if result != tc.formattedTime {
				t.Errorf(
					"expected: %v, got: %v\n",
					tc.formattedTime,
					result,
				)
			}
		})
	}
}
