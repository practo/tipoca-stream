package masker

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/kafka-go/pkg/serializer"
	"github.com/practo/tipoca-stream/kafka-go/pkg/transformer"
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

type masker struct {
	server   string
	database string
	table    string
	topic    string

	configFile string
	config     MaskConfig
}

func NewMsgMasker(dir string, topic string) (
	transformer.MsgTransformer, error) {

	server, database, table := transformer.ParseTopic(topic)
	// TODO: document the convention to specify configuration files
	// Convention: Explained with an example:
	// Say topic="datapipe.inventory.customers"
	//     maskConfigDir="/usr" (in redshiftbatcher config)
	//     mask=true         (in redshiftbatcher config)
	// Then the configuration file should be present at below location:
	//        /usr/inventory.yaml
	configFile := filepath.Join(dir, database+".yaml")
	klog.V(2).Infof("Using mask config file: %s\n", configFile)
	yamlFile, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}
	var config MaskConfig
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return nil, err
	}

	return &masker{
		config:     config,
		configFile: configFile,
		server:     server,
		database:   database,
		table:      table,
	}, nil
}

func (m *masker) mask(data string) *string {
	val := fmt.Sprintf("%x", sha1.Sum(
		[]byte(data),
	))

	return &val
}

func (m *masker) stageColumns(cName string) bool {
	_, ok := ignoreColumns[cName]
	if ok {
		return true
	}

	return false
}

func (m *masker) unMaskNonPiiKeys(cName string) bool {
	columnsToUnmask, ok := m.config.NonPiiKeys[m.table]
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

func (m *masker) unMaskConditionalNonPiiKeys(cName string) bool {
	return false
}

func (m *masker) unMaskMobileKeys(cName string) bool {
	return false
}

func (m *masker) performUnMasking(cName string) bool {
	if m.stageColumns(cName) {
		return true
	}
	if m.unMaskNonPiiKeys(cName) || m.unMaskConditionalNonPiiKeys(
		cName) || m.unMaskMobileKeys(cName) {

		return true
	}

	return false
}

// Transform masks the message based on the masking rules specified in the
// configuration file at: maskConfigDir + database.yaml
// Default: mask everything, unless specified not to in configuraton
// Rules:
// 1. unMaskNonPiiKeys()
// 2. unMaskConditionalNonPiiKeys() TODO://
// 3. unMaskMobileKeys() TODO://
func (m *masker) Transform(message *serializer.Message) error {
	var columns map[string]*string
	err := json.Unmarshal(message.Value.([]byte), &columns)
	if err != nil {
		return err
	}

	maskedColumns := make(map[string]*string)
	for cName, cVal := range columns {
		// takes care of null values
		if cVal == nil {
			maskedColumns[cName] = nil
			continue
		}
		maskedColumns[cName] = m.mask(*cVal)
		if m.performUnMasking(cName) {
			maskedColumns[cName] = cVal
		}
	}

	message.Value, err = json.Marshal(maskedColumns)
	if err != nil {
		return err
	}

	return nil
}
