package masker

import (
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"path/filepath"
)

var (
	ignoreColumns = map[string]bool{
		"kafkaoffset": true,
		"operation":   true,
	}
)

type MaskConfig struct {
	NonPiiKeys map[string][]string `yaml:"non_pii_keys"`
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

	return maskConfig, nil
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
	_, ok := ignoreColumns[cName]
	if ok {
		return true
	}

	if m.unMaskNonPiiKeys(table, cName) || m.unMaskConditionalNonPiiKeys(
		table, cName) || m.unMaskMobileKeys(table, cName) {

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

// TODO:
func (m MaskConfig) unMaskConditionalNonPiiKeys(table, cName string) bool {
	return false
}

// TODO:
func (m MaskConfig) unMaskMobileKeys(table, cName string) bool {
	return false
}