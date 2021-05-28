package util

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestRandomize(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	t.Parallel()

	maxAllowed := 1800
	minAllowed := 1800

	tests := []struct {
		name        string
		value       int
		diffPercent float32
		min         int
		max         int
		maxAllowed  *int
		minAllowed  *int
	}{
		{
			name:        "with allowed",
			value:       1800,
			diffPercent: 0.20,
			min:         1440,
			max:         2160,
			maxAllowed:  nil,
			minAllowed:  nil,
		},
		{
			name:        "with max allowed",
			value:       1800,
			diffPercent: 0.20,
			min:         1440,
			max:         1800,
			maxAllowed:  &maxAllowed,
			minAllowed:  nil,
		},
		{
			name:        "with min allowed",
			value:       1800,
			diffPercent: 0.20,
			min:         1800,
			max:         2160,
			maxAllowed:  nil,
			minAllowed:  &minAllowed,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			v := Randomize(tc.value, tc.diffPercent, tc.maxAllowed, tc.minAllowed)
			fmt.Println(v)
			if v < tc.min || v > tc.max {
				t.Errorf("expected in range: >%v <%v, got: %v\n", tc.min, tc.max, v)
			}
		})
	}
}
