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
	diff := int(float32(value) * diffPercent)

	min := value - diff
	max := value + diff

	if maxAllowed != nil {
		max = *maxAllowed
	}

	if minAllowed != nil {
		min = *minAllowed
	}

	return min + rand.Intn(max-min)
}
