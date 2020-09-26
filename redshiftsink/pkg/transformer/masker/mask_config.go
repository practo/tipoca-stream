package masker

import (
	"fmt"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"path/filepath"
	"regexp"
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

	// ConditionalNonPiiKeys unmasks columns if it matches a list of pattern
	ConditionalNonPiiKeys map[string]interface{} `yaml:"conditional_non_pii_keys,omitempty"`

	// DependentNonPiiKeys unmasks columns based on the values of other columns
	DependentNonPiiKeys map[string]interface{} `yaml:"dependent_non_pii_keys,omitempty"`

	// LengthKeys creates extra column containing the length of original column
	LengthKeys map[string][]string `yaml:"length_keys,omitempty"`

	// SortKeys sets the Redshift column to use the column as the SortKey
	SortKeys map[string][]string `yaml:"sort_keys,omitempty"`

	// DistKeys sets the Redshift column to use the column as the DistKey
	DistKeys map[string][]string `yaml:"dist_keys,omitempty"`

	// regexes cache is used to prevent regex Compile on everytime computations.
	regexes map[string]*regexp.Regexp
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

	fmt.Println("unmarkshaling")

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

	maskConfig.regexes = make(map[string]*regexp.Regexp)

	return maskConfig, nil
}

func (m MaskConfig) LengthKey(table, cName string) bool {
	columns, ok := m.LengthKeys[table]
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

func (m MaskConfig) ConditionalNonPiiKey(table, cName string) bool {
	columnsRaw, ok := m.ConditionalNonPiiKeys[table]
	if !ok {
		return false
	}
	columns, ok := columnsRaw.([]map[string]interface{})
	if !ok {
		klog.Fatalf(
			"Type assertion error! table: %s, cName: %s\n", table, cName,
		)
	}

	for _, column := range columns {
		for columnName, _ := range column {
			if columnName == cName {
				return true
			}
		}
	}

	return false
}

func (m MaskConfig) DependentNonPiiKey(table, cName string) bool {
	columnsRaw, ok := m.DependentNonPiiKeys[table]
	if !ok {
		return false
	}
	columns, ok := columnsRaw.([]map[string]interface{})
	if !ok {
		klog.Fatalf(
			"Type assertion error! table: %s, cName: %s\n", table, cName,
		)
	}

	for _, column := range columns {
		for columnName, _ := range column {
			if columnName == cName {
				return true
			}
		}
	}

	return false
}

// PerformUnMasking checks if unmasking should be done or not
func (m MaskConfig) PerformUnMasking(table, cName string, cValue string,
	allColumns map[string]*string) bool {

	cName = strings.ToLower(cName)
	// usecase: kafkaoffset, operation are staged columns that need to unmasked
	_, ok := ignoreColumns[cName]
	if ok {
		return true
	}

	if m.unMaskNonPiiKeys(table, cName) ||
		m.unMaskConditionalNonPiiKeys(table, cName, cValue) ||
		m.unMaskDependentNonPiiKeys(table, cName, cValue, allColumns) {

		return true
	}

	return false
}

func (m MaskConfig) unMaskNonPiiKeys(table, cName string) bool {
	columnsToUnmask, ok := m.NonPiiKeys[table]
	if !ok {
		return false
	}

	for _, c := range columnsToUnmask {
		if c == cName {
			return true
		}
	}

	return false
}

func (m MaskConfig) unMaskConditionalNonPiiKeys(
	table, cName string, cValue string) bool {

	columnsToCheckRaw, ok := m.ConditionalNonPiiKeys[table]
	if !ok {
		return false
	}
	columnsToCheck, ok := columnsToCheckRaw.([]map[string]interface{})
	if !ok {
		klog.Fatalf(
			"Type assertion error! table: %s, cName: %s\n", table, cName)
	}

	for _, c := range columnsToCheck {
		for columnName, patternsRaw := range c {
			if columnName != cName {
				continue
			}
			patterns, ok := patternsRaw.([]string)
			if !ok {
				klog.Fatalf(
					"Type assertion error! table: %s, cName: %s\n",
					table, cName)
			}
			for _, pattern := range patterns {
				// replace sql patterns with regex patterns
				// TODO: cover all cases :pray
				pattern = strings.ReplaceAll(pattern, "%", ".*")
				regex, ok := m.regexes[pattern]
				if !ok {
					regex, err := regexp.Compile(pattern)
					if err != nil {
						klog.Fatalf(
							"Regex: %s compile failed, err:%v\n", pattern, err)
					}
					m.regexes[pattern] = regex
				}

				if regex.MatchString(cValue) {
					return true
				}
			}
		}
	}

	return false
}

func (m MaskConfig) unMaskDependentNonPiiKeys(
	table, cName string, cValue string, allColumns map[string]*string) bool {

	// if table not in config, no unmasking required
	columnsToCheckRaw, ok := m.DependentNonPiiKeys[table]
	if !ok {
		return false
	}
	columnsToCheck, ok := columnsToCheckRaw.([]map[string]interface{})
	if !ok {
		klog.Fatalf(
			"Type assertion error! table: %s, cName: %s\n", table, cName)
	}

	for _, c := range columnsToCheck {
		for dependentColumnName, providerColumnRaw := range c {
			if dependentColumnName != cName {
				continue
			}
			providerColumn, ok := providerColumnRaw.(map[string]interface{})
			if !ok {
				klog.Fatalf(
					"Type assertion error! table: %s, cName: %s\n",
					table, cName)
			}

			for providerColumnName, value := range providerColumn {
				pcValue, ok := allColumns[providerColumnName]
				if ok && fmt.Sprintf("%s", value) == *pcValue {
					return true
				}
			}
		}
	}

	return false
}
