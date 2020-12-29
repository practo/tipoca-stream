package controllers

import (
	klog "github.com/practo/klog/v2"
	transformer "github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	masker "github.com/practo/tipoca-stream/redshiftsink/pkg/transformer/masker"
)

// MaskDiff reads two database mask configurations and returns the list of
// topics whose mask values has changed.
func MaskDiff(
	topics []string,
	maskFile string,
	desiredVersion string,
	currentVersion string,
	gitToken string,
	homeDir string,
) (
	[]string,
	error,
) {
	if currentVersion == "" {
		return topics, nil
	}

	if currentVersion == desiredVersion {
		return []string{}, nil
	}

	currentMaskConfig, err := masker.NewMaskConfig(
		homeDir, maskFile, currentVersion, gitToken)
	if err != nil {
		return []string{}, err
	}

	desiredMaskConfig, err := masker.NewMaskConfig(
		homeDir, maskFile, desiredVersion, gitToken)
	if err != nil {
		return []string{}, err
	}

	differ := masker.NewMaskDiffer(currentMaskConfig, desiredMaskConfig)
	differ.Diff()
	tablesModified := differ.ModifiedTables()
	if len(tablesModified) == 0 {
		return []string{}, nil
	}

	modifiedTopics := []string{}
	for _, topic := range topics {
		_, _, table := transformer.ParseTopic(topic)
		_, ok := tablesModified[table]
		if ok {
			modifiedTopics = append(modifiedTopics, topic)
		}
	}

	if len(modifiedTopics) == 0 {
		klog.Warningf(
			"Table in mask conf is not present in kafka, tablesModified: %v",
			tablesModified,
		)
	}

	return modifiedTopics, nil
}
