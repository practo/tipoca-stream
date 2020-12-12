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

// Diff performs the diff and makes a map of version and topics
// which should be sinked together
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

	masker.Diff(currentMaskConfig, desiredMaskConfig)
	return nil
}
