package debezium

import (
	"testing"
)

func TestConvertDebeziumTimeStamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		ms             int
		expectedResult string
	}{
		{
			name:           "test 1",
			ms:             1529476623000,
			expectedResult: "2018-06-20 06:37:03",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			result := convertDebeziumTimeStamp(tc.ms)
			if result != tc.expectedResult {
				t.Errorf(
					"expected: %v, got: %v\n",
					tc.expectedResult,
					result,
				)
			}
		})
	}
}

func TestConvertDebeziumDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		days           int
		expectedResult string
	}{
		{
			name:           "test 1",
			days:           6806,
			expectedResult: "1988-08-20",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			result := convertDebeziumDate(tc.days)
			if result != tc.expectedResult {
				t.Errorf(
					"expected: %v, got: %v\n",
					tc.expectedResult,
					result,
				)
			}
		})
	}
}
