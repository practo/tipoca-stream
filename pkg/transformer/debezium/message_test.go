package debezium

import (
	"testing"
)

func TestConvertDebeziumFormattedTime(t *testing.T) {
	t.Parallel()

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
		{
			name:          "test9: TIME",
			value:         "39600000000",
			sourceType:    "TIME",
			sourceLength:  "",
			formattedTime: "11:00:00",
		},
		{
			name:          "test10: TIME",
			value:         "40810000000",
			sourceType:    "TIME",
			sourceLength:  "",
			formattedTime: "11:20:10",
		},
		{
			name:          "test10: DATETIME(3)",
			value:         "1602736317708",
			sourceType:    "DATETIME",
			sourceLength:  "3",
			formattedTime: "2020-10-15 04:31:57.708",
		},
		{
			name:          "test11: DATE epoch",
			value:         "0",
			sourceType:    "DATE",
			sourceLength:  "",
			formattedTime: "1970-01-01",
		},
		{
			name:          "test12: DATE recent",
			value:         "19181",
			sourceType:    "DATE",
			sourceLength:  "",
			formattedTime: "2022-07-08",
		},
		{
			name:          "test13: DATE max year",
			value:         "2932896",
			sourceType:    "DATE",
			sourceLength:  "",
			formattedTime: "9999-12-31",
		},
		// Years < 1000 must be zero-padded to 4 digits, else Redshift
		// COPY rejects the value (Invalid Date Format - length must be
		// 10 or more) and the loader retries the batch forever.
		{
			name:          "test14: DATE year < 1000 is zero-padded",
			value:         "-645643",
			sourceType:    "DATE",
			sourceLength:  "",
			formattedTime: "0202-04-17",
		},
		{
			name:          "test15: DATE 3 digit year is zero-padded",
			value:         "-361910",
			sourceType:    "DATE",
			sourceLength:  "",
			formattedTime: "0979-02-15",
		},
		{
			name:          "test16: DATE last 3 digit year",
			value:         "-354286",
			sourceType:    "DATE",
			sourceLength:  "",
			formattedTime: "0999-12-31",
		},
		{
			name:          "test17: DATE first 4 digit year",
			value:         "-354285",
			sourceType:    "DATE",
			sourceLength:  "",
			formattedTime: "1000-01-01",
		},
		{
			name:          "test18: DATETIME year < 1000 is zero-padded",
			value:         "-55704758400000",
			sourceType:    "DATETIME",
			sourceLength:  "",
			formattedTime: "0204-10-15 00:00:00",
		},
		{
			name:          "test19: DATETIME(6) year < 1000 is zero-padded",
			value:         "-55805894999243000",
			sourceType:    "DATETIME",
			sourceLength:  "6",
			formattedTime: "0201-08-01 10:30:00.757000",
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
