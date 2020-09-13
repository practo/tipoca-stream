package redshift

import (
    "testing"
)

func TestLongTypeSchema(t *testing.T) {
    inputTable := Table{
        Name: "inputTable",
        Columns: []ColInfo{
            ColInfo{
                Name:       "kill_id",
                Type:       "long",
                DefaultVal: "",
                NotNull:    false,
                PrimaryKey: true,
            },
        },
        Meta: Meta{
            Schema: "schema",
        },
    }

    targetTable := Table{
        Name: "targetTable",
        Columns: []ColInfo{
            ColInfo{
                Name:       "kill_id",
                Type:       "character varying(65535)",
                DefaultVal: "",
                NotNull:    false,
                PrimaryKey: true,
            },
        },
        Meta: Meta{
            Schema: "schema",
        },
    }

    columnOps, err := CheckSchemas(inputTable, targetTable)
    if err != nil {
        t.Error(err)
    }

    if len(columnOps) > 0 {
        t.Error("Schema diff not expected")
    }
}
