package controllers

import (
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	masker "github.com/practo/tipoca-stream/redshiftsink/pkg/transformer/masker"
)

type MaskVersionDiffer struct {
	topics   []string
	gitToken string

	maskFile           string
	desiredMaskVersion string
	currentMaskVersion string
}

func NewMaskVersionDiffer(
	topics []string,
	gitToken string,
	maskFile string,
	desiredMaskVersion string,
	status *tipocav1.MaskStatus) *MaskVersionDiffer {

	var currentMaskVersion string
	if status != nil || status.CurrenMaskVersion != nil {
		currentMaskVersion = *status.CurrenMaskVersion
	}

	return &MaskVersionDiffer{
		topics:             topics,
		maskFile:           maskFile,
		desiredMaskVersion: desiredMaskVersion,
		currentMaskVersion: currentMaskVersion,
	}
}

// Diff finds the list of topics which would need update
func (m *MaskVersionDiffer) Diff() error {
	if m.currentMaskVersion == "" {
		return nil
	}

	currentMaskConfig, err := masker.NewMaskConfig(
		m.maskFile,
		m.currentMaskVersion,
		m.gitToken,
	)
	if err != nil {
		return err
	}

	desiredMaskConfig, err := masker.NewMaskConfig(
		m.maskFile,
		m.desiredMaskVersion,
		m.gitToken,
	)
	if err != nil {
		return err
	}

	differ := masker.NewMaskDiffer(currentMaskConfig, desiredMaskConfig)
	tables := differ.Modified()

	_ = tables

	return nil
}
