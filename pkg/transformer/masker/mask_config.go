package masker

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/pkg/git"
	"github.com/practo/tipoca-stream/pkg/transformer"
	"gopkg.in/yaml.v2"
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

	// IncludeTables restrict tables that are allowed to be sinked.
	IncludeTables *[]string `yaml:"include_tables,omitempty"`

	// RegexPatternBooleanKeys helps in keeping free text columns masked
	// and adds boolean columns giving boolean info about the kind of
	// value in the free text column.
	RegexPatternBooleanKeys map[string]interface{} `yaml:"regex_pattern_boolean_keys,omitempty"`

	// regexes cache is used to prevent regex Compile on every message mask run.
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

func loweredList(items *[]string) *[]string {
	if items == nil {
		return nil
	}
	lower := []string{}
	for _, item := range *items {
		lower = append(lower, strings.ToLower(item))
	}

	return &lower
}

func downloadMaskFile(
	homeDir string,
	maskFile string,
	maskFileVersion string,
	gitToken string) (string, error) {

	url, err := git.ParseURL(maskFile)
	if err != nil {
		return "", err
	}

	switch url.Scheme {
	case "file":
		klog.V(5).Info("Mask file is of file type, nothing to download")
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
			repo, configFilePath = git.ParseGithubURL(url.Path)
		default:
			return "", fmt.Errorf("parsing not supported for: %s\n", url.Host)
		}

		dir, err := ioutil.TempDir("", "maskdir")
		if err != nil {
			return "", err
		}
		defer os.RemoveAll(dir)

		g := git.New(
			dir,
			fmt.Sprintf("%s://%s/%s", url.Scheme, url.Host, repo),
			gitToken,
		)

		klog.V(5).Infof("Downloading git repo: %s", repo)
		err = g.Clone()
		if err != nil {
			return "", err
		}
		err = g.Checkout(maskFileVersion)
		if err != nil {
			return "", err
		}
		klog.V(5).Infof("Downloaded git repo at: %s", dir)

		sourceFile := filepath.Join(dir, configFilePath)
		destFile := filepath.Join(homeDir, filepath.Base(sourceFile))
		_, err = git.Copy(sourceFile, destFile)
		if err != nil {
			return "", fmt.Errorf(
				"Error copying! src: %s, dest: %s, err:%v\n",
				sourceFile, destFile, err)
		}
		klog.V(5).Info("Copied the mask file at the read location")

		return destFile, nil
	}
}

func NewMaskConfig(
	homeDir string,
	maskFile string,
	maskFileVersion string,
	gitToken string) (MaskConfig, error) {

	var maskConfig MaskConfig
	configFilePath, err := downloadMaskFile(
		homeDir, maskFile, maskFileVersion, gitToken)
	if err != nil {
		return maskConfig, err
	}
	klog.V(4).Infof("Using mask config file: %s\n", configFilePath)

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
	klog.V(3).Infof("Loaded mask configuration from: %s\n", maskFile)

	// convert to lower case, redshift works with lowercase
	loweredKeys(maskConfig.NonPiiKeys)
	loweredKeys(maskConfig.LengthKeys)
	loweredKeys(maskConfig.MobileKeys)
	loweredKeys(maskConfig.MappingPIIKeys)
	loweredKeys(maskConfig.SortKeys)
	loweredKeys(maskConfig.DistKeys)

	maskConfig.IncludeTables = loweredList(maskConfig.IncludeTables)
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

// BoolColumns returns extra boolean columns for the parent column(free text col)
// to make analysis on the data contained in parent column possible using the
// boolean columns
func (m MaskConfig) BoolColumns(table, cName string, cValue *string) map[string]*string {
	columnsToCheckRaw, ok := m.RegexPatternBooleanKeys[table]
	if !ok {
		return nil
	}

	columnsToCheck, ok := columnsToCheckRaw.(map[interface{}]interface{})
	if !ok {
		klog.Fatalf(
			"Type assertion error! table: %s, cName: %s\n", table, cName)
	}

	boolColumns := make(map[string]*string)
	for freeTextColumnNameRaw, regexesRaw := range columnsToCheck {
		freeTextColumnName := freeTextColumnNameRaw.(string)
		freeTextColumnName = strings.ToLower(freeTextColumnName) // favourite_quote
		if freeTextColumnName != cName {
			continue
		}

		regexes, ok := regexesRaw.(map[interface{}]interface{})
		if !ok {
			klog.Fatalf(
				"Type assertion error! table: %s, cName: %s\n",
				table, cName)
		}

		for regexNameRaw, patternRaw := range regexes {
			regexName := regexNameRaw.(string)
			regexName = strings.ToLower(regexName)
			caseInsensitivePattern := fmt.Sprintf("(?i)%s", patternRaw.(string))

			var err error
			regex, ok := m.regexes[caseInsensitivePattern]
			if !ok {
				regex, err = regexp.Compile(caseInsensitivePattern)
				if err != nil {
					klog.Fatalf(
						"Regex: %s compile failed, err:%v\n", caseInsensitivePattern, err)
				}
				m.regexes[caseInsensitivePattern] = regex
			}

			if cValue != nil && regex.MatchString(*cValue) {
				boolColumns[fmt.Sprintf("%s_%s", freeTextColumnName, regexName)] = stringPtr("true")
			} else {
				boolColumns[fmt.Sprintf("%s_%s", freeTextColumnName, regexName)] = stringPtr("false")
			}

		}
	}

	return boolColumns
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
