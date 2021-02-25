package controllers

import (
	klog "github.com/practo/klog/v2"
	transformer "github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	masker "github.com/practo/tipoca-stream/redshiftsink/pkg/transformer/masker"
	"os"
	"path/filepath"
	"sync"
)

// MaskDiff reads two database mask configurations and returns the list of
// topics whose mask values has changed.
// also returns the updated list of kafka topics
func MaskDiff(
	topics []string,
	maskFile string,
	desiredVersion string,
	currentVersion string,
	gitToken string,
	kafkaTopicsCache *sync.Map,
) (
	[]string,
	[]string,
	error,
) {
	cacheKey := maskFile + desiredVersion
	if currentVersion == desiredVersion {
		// this is required to prevent network IO: git pull and computations
		// but would eat up some memory, but keep opeartor fast
		cacheLoaded, ok := kafkaTopicsCache.Load(cacheKey)
		if ok {
			topics = cacheLoaded.([]string)
			return []string{}, topics, nil
		}
		klog.V(3).Info("Cache miss for shrinking topics, computing...")
	}

	currentDir, err := os.Getwd()
	if err != nil {
		return []string{}, topics, nil
	}
	maskDir := filepath.Join(currentDir, "maskdiff")
	os.Mkdir(maskDir, 0755)

	desiredMaskConfig, err := masker.NewMaskConfig(
		maskDir, maskFile, desiredVersion, gitToken)
	if err != nil {
		return []string{}, topics, err
	}

	var includedTabesMap map[string]bool
	if desiredMaskConfig.IncludeTables != nil {
		includedTabesMap = toMap(*desiredMaskConfig.IncludeTables)
		// shrink the total topics by include tables specification
		shrinkedTopics := []string{}
		for _, topic := range topics {
			_, _, table := transformer.ParseTopic(topic)
			_, ok := includedTabesMap[table]
			if ok {
				shrinkedTopics = append(shrinkedTopics, topic)
			}
		}
		topics = shrinkedTopics
		kafkaTopicsCache.Store(cacheKey, topics)
	}

	if currentVersion == "" {
		return topics, topics, nil
	}

	currentMaskConfig, err := masker.NewMaskConfig(
		maskDir, maskFile, currentVersion, gitToken)
	if err != nil {
		return []string{}, topics, err
	}

	differ := masker.NewMaskDiffer(currentMaskConfig, desiredMaskConfig)
	differ.Diff()
	tablesModified := differ.ModifiedTables()
	if len(tablesModified) == 0 {
		return []string{}, topics, nil
	}

	if desiredMaskConfig.IncludeTables != nil {
		// ignore the tables which aer not part of include tables
		newTablesModified := make(map[string]bool)
		for table, _ := range tablesModified {
			_, ok := includedTabesMap[table]
			if !ok {
				klog.Warningf("Excluding table: %v", table)
			} else {
				newTablesModified[table] = true
			}
		}
		tablesModified = newTablesModified
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

	return modifiedTopics, topics, nil
}
