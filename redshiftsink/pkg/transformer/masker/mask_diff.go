package masker

import (
	"reflect"
)

type MaskDiff interface {
	Diff()
	ModifiedTables() map[string]bool
}

type MaskDiffer struct {
	current MaskConfig
	desired MaskConfig

	// modified stores the list of tables that has been modified.
	// i.e present in both current and desired but different
	modified map[string]bool
}

func NewMaskDiffer(current MaskConfig, desired MaskConfig) MaskDiff {
	return &MaskDiffer{
		current: current,
		desired: desired,

		modified: make(map[string]bool),
	}
}

func (m *MaskDiffer) ModifiedTables() map[string]bool {
	return m.modified
}

func (m *MaskDiffer) setModified(table string) {
	m.modified[table] = true
}

func (m *MaskDiffer) tableModified(table string) bool {
	_, ok := m.modified[table]
	if ok {
		return true
	}

	return false
}

func (m *MaskDiffer) diffMapInterface(
	m1 map[string]interface{}, m2 map[string]interface{}) {

	if reflect.DeepEqual(m1, m2) {
		return
	}

	for table, c1 := range m2 {
		if m.tableModified(table) {
			continue
		}
		c2, ok := m1[table]
		if !ok {
			m.setModified(table)
			continue
		}

		if !reflect.DeepEqual(c1, c2) {
			m.setModified(table)
		}
	}
}

func (m *MaskDiffer) diffMapSlice(
	m1 map[string][]string, m2 map[string][]string) {

	if reflect.DeepEqual(m1, m2) {
		return
	}

	for table, c1 := range m2 {
		if m.tableModified(table) {
			continue
		}
		c2, ok := m1[table]
		if !ok {
			m.setModified(table)
			continue
		}

		if !reflect.DeepEqual(c1, c2) {
			m.setModified(table)
		}
	}
}

// Diff does the diff between current and desired config and stores the result
// in modified, removed and added.
func (m *MaskDiffer) Diff() {
	if reflect.DeepEqual(m.current, m.desired) {
		return
	}

	m.diffMapSlice(m.current.NonPiiKeys, m.desired.NonPiiKeys)
	m.diffMapSlice(m.current.LengthKeys, m.desired.LengthKeys)
	m.diffMapSlice(m.current.MobileKeys, m.desired.MobileKeys)
	m.diffMapSlice(m.current.MappingPIIKeys, m.desired.MappingPIIKeys)
	m.diffMapSlice(m.current.SortKeys, m.desired.SortKeys)
	m.diffMapSlice(m.current.DistKeys, m.desired.DistKeys)
	m.diffMapInterface(
		m.current.ConditionalNonPiiKeys, m.desired.ConditionalNonPiiKeys)
	m.diffMapInterface(
		m.current.DependentNonPiiKeys, m.desired.DependentNonPiiKeys)
}
