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

func NewMsgMasker(salt string, topic string, config MaskConfig) transformer.MessageTransformer {
	_, database, table := transformer.ParseTopic(topic)

	return &masker{
		salt:     salt,
		database: database,
		table:    table,
		config:   config,
	}
}

func Mask(data string, salt string) *string {
	val := fmt.Sprintf("%x", sha1.Sum(
		[]byte(data+salt),
	))

	return &val
}

func stringPtr(s string) *string {
	return &s
}

// addMissingCol as messages may not have all the columns but table schema has
// if this is not done maskSchema goes empty for such columns and cause problems
func addMissingColumn(rawColumns map[string]*string, cols []redshift.ColInfo) {
	for _, col := range cols {
		columnName := strings.ToLower(col.Name)
		_, ok := rawColumns[columnName]
		if !ok {
			rawColumns[columnName] = nil
		}
	}
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

	addMissingColumn(rawColumns, table.Columns)

	columns := make(map[string]*string)
	maskSchema := make(map[string]serializer.MaskInfo)
	extraMaskSchema := make(map[string]serializer.ExtraMaskInfo)
	extraColumnValue := make(map[string]*string)
	mappingPIIKeyTable := m.config.hasMappingPIIKey(m.table)

	for cName, cVal := range rawColumns {
		unmasked := m.config.PerformUnMasking(m.table, cName, cVal, rawColumns)
		sortKey := m.config.SortKey(m.table, cName)
		distKey := m.config.DistKey(m.table, cName)
		lengthKey := m.config.LengthKey(m.table, cName)
		mobileKey := m.config.MobileKey(m.table, cName)
		mappingPIIKey := m.config.MappingPIIKey(m.table, cName)
		dependentNonPiiKey := m.config.DependentNonPiiKey(m.table, cName)
		conditionalNonPiiKey := m.config.ConditionalNonPiiKey(m.table, cName)
		boolColumns := m.config.BoolColumns(m.table, cName, cVal)

		// extraColumns store the mask info for extra columns
		// extra columns are added for the following keys:
		// LengthKey, MobileKey, MappingPIIKey and Boolean keys
		if lengthKey {
			var length int
			if cVal != nil {
				length = len(*cVal)
			}
			extraColumnName := strings.ToLower(cName + transformer.LengthColumnSuffix)
			extraMaskSchema[extraColumnName] = serializer.ExtraMaskInfo{
				Masked:     false,
				ColumnType: redshift.RedshiftInteger,
				DefaultVal: "0",
			}
			extraColumnValue[extraColumnName] = stringPtr(strconv.Itoa(length))
		}
		if mobileKey {
			var tMobile *string
			if cVal == nil {
				tMobile = nil
			} else {
				mobile := *cVal
				exposedLength := MOBILE_KEYS_EXPOSED_LENGTH
				if len(mobile) < MOBILE_KEYS_EXPOSED_LENGTH {
					exposedLength = len(mobile)
				}
				tMobile = stringPtr(
					mobile[:exposedLength],
				)
			}
			extraColumnName := strings.ToLower(cName + transformer.MobileCoulmnSuffix)
			extraMaskSchema[extraColumnName] = serializer.ExtraMaskInfo{
				Masked:     false,
				ColumnType: redshift.RedshiftMobileColType,
			}
			extraColumnValue[extraColumnName] = tMobile
		}
		if mappingPIIKey {
			var hashedValue *string
			if cVal == nil || strings.TrimSpace(*cVal) == "" {
				hashedValue = nil
			} else {
				hashedValue = Mask(*cVal, m.salt)
			}
			extraColumnName := strings.ToLower(transformer.MappingPIIColumnPrefix + cName)
			extraMaskSchema[extraColumnName] = serializer.ExtraMaskInfo{
				Masked:     true,
				ColumnType: redshift.RedshiftMaskedDataType,
			}
			extraColumnValue[extraColumnName] = hashedValue
		}
		var boolColumnKey bool
		if len(boolColumns) > 0 {
			boolColumnKey = true
			for boolCol, boolVal := range boolColumns {
				extraMaskSchema[boolCol] = serializer.ExtraMaskInfo{
					Masked:     false,
					ColumnType: redshift.RedshiftBoolean,
				}
				extraColumnValue[boolCol] = boolVal
			}
		} // all extra columns handled

		// special case for mapping PII keys
		if mappingPIIKeyTable {
			unmasked = true
		}

		if cVal == nil || strings.TrimSpace(*cVal) == "" {
			columns[cName] = nil
		} else if unmasked {
			columns[cName] = cVal
		} else {
			columns[cName] = Mask(*cVal, m.salt)
		}

		// This has no meaning, and is not used, as StringMax is used
		// based on ConditionalNonPIICol and DependentNonPIICol
		// Just keeping it masked as majority of rows are expected to be that
		if conditionalNonPiiKey || dependentNonPiiKey {
			unmasked = false
		}

		maskSchema[cName] = serializer.MaskInfo{
			Masked: !unmasked,

			SortCol: sortKey,
			DistCol: distKey,

			LengthCol:              lengthKey,
			MobileCol:              mobileKey,
			MappingPIICol:          mappingPIIKey,
			ConditionalNonPIICol:   conditionalNonPiiKey,
			DependentNonPIICol:     dependentNonPiiKey,
			RegexPatternBooleanCol: boolColumnKey,
		}
	}

	// append the extra columns so that the data reaches s3
	for cName, cVal := range extraColumnValue {
		// send value in json only when it is not nil, so that NULL takes effect
		if cVal != nil {
			columns[cName] = cVal
		}
	}

	message.Value = columns
	message.MaskSchema = maskSchema
	message.ExtraMaskSchema = extraMaskSchema

	return nil
}
