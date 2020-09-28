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
	maskSchema := make(map[string]serializer.MaskInfo)

	for cName, cVal := range rawColumns {
		if cVal == nil {
			columns[cName] = nil
			// nil value is not masked but its schema should have masked type
			maskInfo := serializer.MaskInfo{Masked: false}
			maskSchema[cName] = maskInfo
			continue
		}

		unmasked := m.config.PerformUnMasking(m.table, cName, cVal, rawColumns)
		sortKey := m.config.SortKey(m.table, cName)
		distKey := m.config.DistKey(m.table, cName)
		lengthKey := m.config.LengthKey(m.table, cName)

		if lengthKey {
			var length int
			if cVal != nil {
				length = len(*cVal)
			}
			extraColumns[cName+transformer.LengthColumnSuffix] = stringPtr(
				strconv.Itoa(length),
			)
		}

		if cVal == nil {
			columns[cName] = nil
		} else if unmasked {
			columns[cName] = cVal
		} else {
			columns[cName] = mask(*cVal, m.salt)
		}

		// Since we do not know what will happen in future and what value
		// can the column hold, if a row is defined as dependent or conditional
		//  non pii then value in the row is masked based on the condition ^
		// but its type is always a masked type(string).
		// This change is to enable that.
		if m.config.DependentNonPiiKey(m.table, cName) ||
			m.config.ConditionalNonPiiKey(m.table, cName) {
			unmasked = false
		}

		maskSchema[cName] = serializer.MaskInfo{
			Masked:    !unmasked,
			SortCol:   sortKey,
			DistCol:   distKey,
			LengthCol: lengthKey,
		}
	}

	for cName, cVal := range extraColumns {
		columns[cName] = cVal
		maskSchema[cName] = serializer.MaskInfo{
			LengthCol: false, // extra length column, don't want one more extra
		}
	}

	message.Value, err = json.Marshal(columns)
	if err != nil {
		return err
	}

	message.MaskSchema = maskSchema

	return nil
}
