package controllers

import (
	"context"
	"fmt"

	// tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	klog "github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
)

type releaser struct {
	schema     string
	redshifter *redshift.Redshift
}

func newReleaser(
	ctx context.Context,
	schema string,
	secret map[string]string,
) (
	*releaser,
	error,
) {

	var redshiftSecret map[string]string
	redshiftSecretKeys := []string{
		"redshiftHost",
		"redshiftPort",
		"redshiftDatabase",
		"redshiftUser",
		"redshiftPassword",
	}
	for _, key := range redshiftSecretKeys {
		value, err := secretByKey(secret, key)
		if err != nil {
			return nil, err
		}
		redshiftSecret[key] = value
	}

	config := redshift.RedshiftConfig{
		Schema:       schema,
		Host:         redshiftSecret["redshiftHost"],
		Port:         redshiftSecret["redshiftPort"],
		Database:     redshiftSecret["redshiftDatabases"],
		User:         redshiftSecret["redshiftUser"],
		Password:     redshiftSecret["redshiftPassword"],
		Timeout:      10,
		Stats:        true,
		MaxOpenConns: 3,
		MaxIdleConns: 3,
	}

	redshifter, err := redshift.NewRedshift(ctx, config)
	if err != nil {
		return nil, err
	}

	return &releaser{
		schema:     schema,
		redshifter: redshifter,
	}, nil
}

func tempTableName(table string) string {
	return table + "_ts_temp"
}

func (r *releaser) release(
	schema string,
	topic string,
	tableSuffix string,
	group *string,
) error {
	klog.Infof("releasing topic: %s", topic)
	_, _, table := transformer.ParseTopic(topic)
	tempTable := tempTableName(table)
	reloadedTable := table + tableSuffix

	tx, err := r.redshifter.Begin()
	if err != nil {
		return fmt.Errorf("Error creating database tx, err: %v\n", err)
	}

	// can ignore errors in dropping table
	r.redshifter.DropTable(tx, schema, tempTable)

	err = r.redshifter.RenameTable(tx, schema, table, tempTable)
	if err != nil {
		return err
	}

	err = r.redshifter.RenameTable(tx, schema, reloadedTable, table)
	if err != nil {
		return err
	}

	if group != nil {
		err = r.redshifter.GrantSchemaAccess(tx, schema, table, *group)
		if err != nil {
			return err
		}
	}

	// release
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("Error committing tx, err:%v\n", err)
	}
	klog.Infof("released topic in redshift: %s", topic)

	return nil
}
