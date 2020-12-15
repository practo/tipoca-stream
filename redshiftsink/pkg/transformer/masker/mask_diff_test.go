package masker

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestMaskDiff(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	conf0 := filepath.Join(dir, "database.yaml")
	conf1 := filepath.Join(dir, "database.yaml")
	conf2 := filepath.Join(dir, "database_maskdiff.yaml")
	m0, err := NewMaskConfig(conf0, "", "")
	if err != nil {
		t.Error(err)
	}
	m1, err := NewMaskConfig(conf1, "", "")
	if err != nil {
		t.Error(err)
	}
	m2, err := NewMaskConfig(conf2, "", "")
	if err != nil {
		t.Error(err)
	}

	differ := NewMaskDiffer(m0, m1)
	differ.Diff()
	gotDiff := differ.ModifiedTables()
	if len(gotDiff) != 0 {
		t.Errorf("expected no difference, got: %+v", gotDiff)
	}

	differ = NewMaskDiffer(m1, m2)
	differ.Diff()
	gotDiff = differ.ModifiedTables()
	expected := map[string]bool{
		"justifications": true,
		"establishments": true,
		"customers":      true,
		"addedNewTable":  true,
	}
	if !reflect.DeepEqual(gotDiff, expected) {
		t.Errorf("expected :%v, got: %+v", expected, gotDiff)
	}
}
