package masker

import (
	"fmt"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/git"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	ignoreColumns = map[string]bool{
		transformer.TempTablePrimary: true,
		transformer.TempTableOp:      true,
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

	// MobileKeys creates extra column containing first
	// MOBILE_KEYS_EXPOSED_LENGTH characters exposed in the mobile number
	MobileKeys map[string][]string `yaml:"mobile_keys,omitempty"`

	// MappingPIIKey creates extra column containing hashed values
	MappingPIIKeys map[string][]string `yaml:"mapping_pii_keys"`

	// SortKeys sets the Redshift column to use the column as the SortKey
	SortKeys map[string][]string `yaml:"sort_keys,omitempty"`

	// DistKeys sets the Redshift column to use the column as the DistKey
	DistKeys map[string][]string `yaml:"dist_keys,omitempty"`

	// regexes cache is used to prevent regex Compile on everytime computations.
	regexes map[string]*regexp.Regexp
}

func loweredKeys(keys map[string][]string) {
	for table, columns := range keys {
		var loweredColumns []string
		for _, column := range columns {
			loweredColumns = append(loweredColumns, strings.ToLower(column))
		}
		keys[table] = loweredColumns
	}
}

func downloadMaskFile(maskFile string, maskFileVersion string) (string, error) {
	url, err := git.ParseURL(maskFile)
	if err != nil {
		return "", err
	}

	switch url.Scheme {
	case "file":
		klog.V(2).Info("Mask file is of file type, nothing to download")
		return maskFile, nil
	default:
		if maskFileVersion == "" {
			return "", fmt.Errorf(
				"maskFileVersion is mandatory if maskFile is not local file.",
			)
		}
		var repo, configFilePath string
		switch url.Host {
		case "github.com":
			var org, repoName string
			org, repoName, configFilePath = git.ParseGithubURL(url.Path)
			repo = org + "/" + repoName
		default:
			return "", fmt.Errorf("parsing not supported for: %s\n", url.Host)
		}

		dir, err := ioutil.TempDir("", "maskdir")
		if err != nil {
			return "", err
		}
		defer os.RemoveAll(dir)

		g := git.New(dir, repo,
			viper.GetString("batcher.githubAccessToken"),
		)

		klog.V(2).Infof("Downloading git repo: %s", repo)
		err = g.Clone()
		if err != nil {
			return "", err
		}
		err = g.Checkout(maskFileVersion)
		if err != nil {
			return "", err
		}
		klog.V(4).Infof("Downloaded git repo at: %s", dir)

		_, err = git.Copy(filepath.Join(dir, configFilePath), "/")
		if err != nil {
			return "", err
		}
		klog.V(2).Info("Copied the mask file at the read location")

		return "/" + filepath.Base(configFilePath), nil
	}
}

func NewMaskConfig(
	topic string, maskFile string, maskFileVersion string) (MaskConfig, error) {
	var maskConfig MaskConfig
	configFilePath, err := downloadMaskFile(maskFile, maskFileVersion)
	if err != nil {
		return maskConfig, err
	}
	klog.V(2).Infof("Using mask config file: %s\n", configFilePath)

	yamlFile, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		return maskConfig, fmt.Errorf(
			"Unable to read file: %s, err: %v", configFilePath, err)
	}

	err = yaml.Unmarshal(yamlFile, &maskConfig)
	if err != nil {
		return maskConfig, fmt.Errorf(
			"Unable to unmarshal: %v, err: %v", configFilePath, err)
	}

	// convert to lower case, redshift works with lowercase
	loweredKeys(maskConfig.NonPiiKeys)
	loweredKeys(maskConfig.LengthKeys)
	loweredKeys(maskConfig.MobileKeys)
	loweredKeys(maskConfig.MappingPIIKeys)
	loweredKeys(maskConfig.SortKeys)
	loweredKeys(maskConfig.DistKeys)

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

func (m MaskConfig) MobileKey(table, cName string) bool {
	columns, ok := m.MobileKeys[table]
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

func (m MaskConfig) MappingPIIKey(table, cName string) bool {
	columns, ok := m.MappingPIIKeys[table]
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

func (m MaskConfig) hasMappingPIIKey(table string) bool {
	_, ok := m.MappingPIIKeys[table]
	if ok {
		return true
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
	columnsToCheckRaw, ok := m.ConditionalNonPiiKeys[table]
	if !ok {
		return false
	}
	columnsToCheck, ok := columnsToCheckRaw.(map[interface{}]interface{})
	if !ok {
		klog.Fatalf(
			"Type assertion error! table: %s, cName: %s\n", table, cName)
	}

	for columnNameRaw, _ := range columnsToCheck {
		columnName := columnNameRaw.(string)
		columnName = strings.ToLower(columnName)
		if columnName == cName {
			return true
		}
	}

	return false
}

func (m MaskConfig) DependentNonPiiKey(table, cName string) bool {
	columnsToCheckRaw, ok := m.DependentNonPiiKeys[table]
	if !ok {
		return false
	}
	columnsToCheck, ok := columnsToCheckRaw.(map[interface{}]interface{})
	if !ok {
		klog.Fatalf(
			"Type assertion error! table: %s, cName: %s\n", table, cName)
	}

	for dependentColumnNameRaw, _ := range columnsToCheck {
		dependentColumnName := dependentColumnNameRaw.(string)
		dependentColumnName = strings.ToLower(dependentColumnName)
		if dependentColumnName == cName {
			return true
		}
	}

	return false
}

// PerformUnMasking checks if unmasking should be done or not
func (m MaskConfig) PerformUnMasking(table, cName string, cValue *string,
	allColumns map[string]*string) bool {

	cName = strings.ToLower(cName)
	// usecase: temp columns does not need to be masked
	_, ok := ignoreColumns[cName]
	if ok {
		return true
	}

	if m.unMaskNonPiiKeys(table, cName) ||
		m.unMaskConditionalNonPiiKeys(table, cName, cValue) ||
		m.unMaskDependentNonPiiKeys(table, cName, allColumns) {

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
	table, cName string, cValue *string) bool {
	if cValue == nil {
		return false
	}

	columnsToCheckRaw, ok := m.ConditionalNonPiiKeys[table]
	if !ok {
		return false
	}

	columnsToCheck, ok := columnsToCheckRaw.(map[interface{}]interface{})
	if !ok {
		klog.Fatalf(
			"Type assertion error! table: %s, cName: %s\n", table, cName)
	}

	for columnNameRaw, patternsR := range columnsToCheck {
		columnName := columnNameRaw.(string)
		columnName = strings.ToLower(columnName)
		if columnName != cName {
			continue
		}

		patternsRaw := patternsR.([]interface{})

		var err error
		for _, patternRaw := range patternsRaw {
			pattern := fmt.Sprintf("%v", patternRaw)
			// replace sql patterns with regex patterns
			// TODO: cover all cases :pray
			pattern = strings.ReplaceAll(pattern, "%", ".*")
			pattern = "^" + pattern + "$"
			regex, ok := m.regexes[pattern]
			if !ok {
				regex, err = regexp.Compile(pattern)
				if err != nil {
					klog.Fatalf(
						"Regex: %s compile failed, err:%v\n", pattern, err)
				}
				m.regexes[pattern] = regex
			}

			if regex.MatchString(*cValue) {
				return true
			}
		}
	}

	return false
}

func (m MaskConfig) unMaskDependentNonPiiKeys(
	table, cName string, allColumns map[string]*string) bool {

	// if table not in config, no unmasking required
	columnsToCheckRaw, ok := m.DependentNonPiiKeys[table]
	if !ok {
		return false
	}

	columnsToCheck, ok := columnsToCheckRaw.(map[interface{}]interface{})
	if !ok {
		klog.Fatalf(
			"Type assertion error! table: %s, cName: %s\n", table, cName)
	}

	for dependentColumnNameRaw, providerColumnRaw := range columnsToCheck {
		dependentColumnName := dependentColumnNameRaw.(string)
		dependentColumnName = strings.ToLower(dependentColumnName)
		if dependentColumnName != cName {
			continue
		}

		providerColumn, ok := providerColumnRaw.(map[interface{}]interface{})
		if !ok {
			klog.Fatalf(
				"Type assertion error! table: %s, cName: %s\n",
				table, cName)
		}

		for providerColumnNameRaw, valuesRaw := range providerColumn {
			providerColumnName := providerColumnNameRaw.(string)
			providerColumnName = strings.ToLower(providerColumnName)
			values := valuesRaw.([]interface{})
			for _, valueRaw := range values {

				value := fmt.Sprintf("%v", valueRaw)
				pcValue, ok := allColumns[providerColumnName]
				if !ok {
					continue
				}
				if pcValue == nil {
					continue
				}
				if ok && value == *pcValue {
					return true
				}
			}
		}
	}

	return false
}
