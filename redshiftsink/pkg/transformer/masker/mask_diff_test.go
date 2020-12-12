package masker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMaskDiff(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	conf1 := filepath.Join(dir, "database.yaml")
	conf2 := filepath.Join(dir, "database_maskdiff.yaml")

	m1, err := NewMaskConfig(conf1, "", "")
	if err != nil {
		t.Error(err)
	}

	m2, err := NewMaskConfig(conf2, "", "")
	if err != nil {
		t.Error(err)
	}

	Diff(m1, m2)
    t.Log("done mask diff test")
}
