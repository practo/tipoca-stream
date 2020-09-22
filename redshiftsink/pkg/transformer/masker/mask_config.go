package masker

import (
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"path/filepath"
	"strings"
)

var (
	ignoreColumns = map[string]bool{
		"kafkaoffset": true,
		"operation":   true,
	}
)

type MaskConfig struct {
	// NonPiiKeys specifies the columns that needs be unmasked
	NonPiiKeys map[string][]string `yaml:"non_pii_keys,omitempty"`
	// SortKeys sets the Redshift column to use the column as the SortKey
	SortKeys map[string][]string `yaml:"sort_keys,omitempty"`
	// DistKeys sets the Redshift column to use the column as the DistKey
	DistKeys map[string][]string `yaml:"dist_keys,omitempty"`
}

// TODO: document the convention to specify configuration files
// Convention: Explained with an example:
// Say topic="datapipe.inventory.customers"
//     maskConfigDir="/usr" (in redshiftbatcher config)
//     mask=true         (in redshiftbatcher config)
// 	   then the configuration file should be present at below location:
//     /usr/inventory.yaml
func NewMaskConfig(dir string, topic string) (MaskConfig, error) {
	var maskConfig MaskConfig
	_, database, _ := transformer.ParseTopic(topic)

	configFile := filepath.Join(dir, database+".yaml")
	klog.V(2).Infof("Using mask config file: %s\n", configFile)

	yamlFile, err := ioutil.ReadFile(configFile)
	if err != nil {
		return maskConfig, err
	}

	err = yaml.Unmarshal(yamlFile, &maskConfig)
	if err != nil {
		return maskConfig, err
	}

	// convert to lower case, redshift works with lowercase
	for table, columns := range maskConfig.NonPiiKeys {
		var loweredColumns []string
		for _, column := range columns {
			loweredColumns = append(loweredColumns, strings.ToLower(column))
		}
		maskConfig.NonPiiKeys[table] = loweredColumns
	}

	return maskConfig, nil
}

func (m MaskConfig) SortKey(table, cName string) bool {
	columns, ok := m.SortKeys[table]
	if !ok {
		return false
	}

	for _, column := range columns {
		if column == cName {
			return true
		}
	}

	return false
}

func (m MaskConfig) DistKey(table, cName string) bool {
	columns, ok := m.DistKeys[table]
	if !ok {
		return false
	}

	for _, column := range columns {
		if column == cName {
			return true
		}
	}

	return false
}

// Masked tells if the column is masked or not based on the maskconfig
// It is used to determine the type of the masked column by the loader
func (m MaskConfig) Masked(table, cName string) bool {
	if m.PerformUnMasking(table, cName) {
		return false
	}

	return true
}

func (m MaskConfig) PerformUnMasking(table, cName string) bool {
	// usecase: kafkaoffset, operation are staged columns that need to unmasked
	cName = strings.ToLower(cName)

	_, ok := ignoreColumns[cName]
	if ok {
		return true
	}

	if m.unMaskNonPiiKeys(table, cName) {

		return true
	}

	return false
}

func (m MaskConfig) unMaskNonPiiKeys(table, cName string) bool {
	columnsToUnmask, ok := m.NonPiiKeys[table]
	if !ok {
		return true
	}

	for _, c := range columnsToUnmask {
		if c == cName {
			return true
		}
	}

	return false
}
