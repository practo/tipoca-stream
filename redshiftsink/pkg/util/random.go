package util

import (
	"math/rand"
)

func Randomize(
	value int,
	diffPercent float32,
	maxAllowed *int,
	minAllowed *int,
) int {
	var max, min int
	diff := int(float32(value) * diffPercent)

	if maxAllowed != nil {
		max = *maxAllowed
	} else {
		max = value + diff
	}

	if minAllowed != nil {
		min = *minAllowed
	} else {
		min = value - diff
	}

	return min + rand.Intn(max-min)
}
