package masker

import (
	"crypto/sha1"
	"fmt"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	"strconv"
	"strings"
)

const (
	MOBILE_KEYS_EXPOSED_LENGTH = 5
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

	rawColumns, ok := message.Value.(map[string]*string)
	if !ok {
		return fmt.Errorf(
			"Error converting message.Value, message: %+v\n", message)
	}

	columns := make(map[string]*string)
	extraColumns := make(map[string]*string)
	maskSchema := make(map[string]serializer.MaskInfo)

	for cName, cVal := range rawColumns {
		unmasked := m.config.PerformUnMasking(m.table, cName, cVal, rawColumns)
		sortKey := m.config.SortKey(m.table, cName)
		distKey := m.config.DistKey(m.table, cName)
		lengthKey := m.config.LengthKey(m.table, cName)
		mobileKey := m.config.MobileKey(m.table, cName)

		if lengthKey {
			var length int
			if cVal != nil {
				length = len(*cVal)
			}
			extraColumns[cName+transformer.LengthColumnSuffix] = stringPtr(
				strconv.Itoa(length),
			)
		}

		if mobileKey {
			var tMobile *string
			if cVal == nil {
				tMobile = nil
			} else {
				mobile := *cVal
				tMobile = stringPtr(
					mobile[:MOBILE_KEYS_EXPOSED_LENGTH],
				)
			}
			extraColumns[cName+transformer.MobileCoulmnSuffix] = tMobile
		}

		if cVal == nil || strings.TrimSpace(*cVal) == "" {
			columns[cName] = nil
		} else if unmasked {
			columns[cName] = cVal
		} else {
			columns[cName] = mask(*cVal, m.salt)
		}

		// This determines the type of the mask schema, the value is taken care
		// above so now we can decide what should be the type of the column
		// based on whether it is defined as any of the following keys
		// (overide for these)
		// 1. DependentNonPii 2. ConditionalNonPii
		if m.config.DependentNonPiiKey(m.table, cName) ||
			m.config.ConditionalNonPiiKey(m.table, cName) {
			unmasked = false
		}

		maskSchema[cName] = serializer.MaskInfo{
			Masked:    !unmasked,
			SortCol:   sortKey,
			DistCol:   distKey,
			LengthCol: lengthKey,
			MobileCol: mobileKey,
		}
	}

	for cName, cVal := range extraColumns {
		columns[cName] = cVal
		maskSchema[cName] = serializer.MaskInfo{
			LengthCol: false, // extra length column, don't want one more extra
			MobileCol: false, // extra mobile column, don't want one more extra
		}
	}

	message.Value = columns
	message.MaskSchema = maskSchema

	return nil
}