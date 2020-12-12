package masker

import (
	"reflect"
)

func Diff(m1 MaskConfig, m2 MaskConfig) []string {
	diff := make(map[string]bool)
	tableDiff := []string{}

	if !reflect.DeepEqual(m1.NonPiiKeys, m2.NonPiiKeys) {
		for table, v1 := range m1.NonPiiKeys {
			v2, ok := m2.NonPiiKeys[table]
			if !ok {
				diff[table] = true
				continue
			}

			if !reflect.DeepEqual(v1, v2) {
				diff[table] = true
			}
		}
	}

	_ = diff

	return tableDiff
}
