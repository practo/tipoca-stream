package controllers

import (
	klog "github.com/practo/klog/v2"
	transformer "github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	masker "github.com/practo/tipoca-stream/redshiftsink/pkg/transformer/masker"
)

func MaskDiff(
	topics []string,
	maskFile string,
	currentVersion string,
	desiredVersion string,
	t string) ([]string, error) {

	currentMaskConfig, err := masker.NewMaskConfig(maskFile, currentVersion, t)
	if err != nil {
		return []string{}, err
	}

	klog.Info("desired")
	desiredMaskConfig, err := masker.NewMaskConfig(maskFile, desiredVersion, t)
	if err != nil {
		return []string{}, err
	}

	differ := masker.NewMaskDiffer(currentMaskConfig, desiredMaskConfig)
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
