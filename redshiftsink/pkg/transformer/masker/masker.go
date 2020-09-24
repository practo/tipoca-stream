package masker

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	"strconv"
)

type masker struct {
	salt     string
	database string
	table    string
	topic    string

	config MaskConfig
}

func NewMsgMasker(salt string, dir string, topic string) (
	transformer.MessageTransformer, error) {

	_, database, table := transformer.ParseTopic(topic)
	maskConfig, err := NewMaskConfig(dir, topic)
	if err != nil {
		return nil, err
	}

	return &masker{
		salt:     salt,
		database: database,
		table:    table,
		config:   maskConfig,
	}, nil
}

func mask(data string, salt string) *string {
	val := fmt.Sprintf("%x", sha1.Sum(
		[]byte(data+salt),
	))

	return &val
}

func stringPtr(s string) *string {
	return &s
}

// Transform masks the message based on the masking rules specified in the
// configuration file at: maskConfigDir + database.yaml
// Default: mask everything, unless specified not to in the configuraton
// Rules:
// 1. unMaskNonPiiKeys()
// 2. unMaskConditionalNonPiiKeys() //TODO:
// 3. unMaskMobileKeys() //TODO:
func (m *masker) Transform(
	message *serializer.Message, table redshift.Table) error {

	var rawColumns map[string]*string
	err := json.Unmarshal(message.Value.([]byte), &rawColumns)
	if err != nil {
		return err
	}

	columns := make(map[string]*string)
	extraColumns := make(map[string]*string)

	for cName, cVal := range rawColumns {
		if cVal == nil {
			columns[cName] = nil
			continue
		}
		columns[cName] = mask(*cVal, m.salt)
		if m.config.LengthKey(m.table, cName) {
			extraColumns[cName+transformer.LengthColumnSuffix] = stringPtr(
				strconv.Itoa(len(*cVal)))
		}
		if m.config.PerformUnMasking(m.table, cName) {
			columns[cName] = cVal
		}
	}
	for cName, cVal := range extraColumns {
		columns[cName] = cVal
	}

	message.Value, err = json.Marshal(columns)
	if err != nil {
		return err
	}

	return nil
}
