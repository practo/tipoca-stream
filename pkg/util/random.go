package util

import (
	"math/rand"
)

func Randomize(value int, diffPercent float32) int {
	diff := int(float32(value) * diffPercent)
	max := value + diff
	min := value - diff

	return min + rand.Intn(max-min)
}
