package controllers

import (
	"context"
	"fmt"

	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/notify"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
)

type releaser struct {
	schema     string
	repo       string
	filePath   string
	version    string
	redshifter *redshift.Redshift
	notifier   notify.Notifier
}

func newReleaser(
	ctx context.Context,
	schema string,
	repo string,
	filePath string,
	version string,
	secret map[string]string,
) (
	*releaser,
	error,
) {

	redshiftSecret := make(map[string]string)
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
		Database:     redshiftSecret["redshiftDatabase"],
		User:         redshiftSecret["redshiftUser"],
		Password:     redshiftSecret["redshiftPassword"],
		Timeout:      10,
		Stats:        true,
		MaxOpenConns: 3,
		MaxIdleConns: 3,
	}

	redshifter, err := redshift.NewRedshift(ctx, config)
	if err != nil {
		return nil, fmt.Errorf(
			"Error creating redshift connecton, err: %v", err)
	}

	return &releaser{
		schema:     schema,
		redshifter: redshifter,
		repo:       repo,
		filePath:   filePath,
		version:    version,
		notifier:   makeNotifier(secret),
	}, nil
}

func makeNotifier(secret map[string]string) notify.Notifier {
	slackBotToken, err := secretByKey(secret, "slackBotToken")
	if err != nil {
		return nil
	}

	slackChannelID, err := secretByKey(secret, "slackChannelID")
	if err != nil {
		return nil
	}

	return notify.New(slackBotToken, slackChannelID)
}

func (r *releaser) release(
	schema string,
	topic string,
	tableSuffix string,
	group *string,
) error {
	klog.Infof("releasing topic: %s", topic)
	_, _, table := transformer.ParseTopic(topic)
	reloadedTable := table + tableSuffix

	tx, err := r.redshifter.Begin()
	if err != nil {
		return fmt.Errorf("Error creating database tx, err: %v\n", err)
	}

	tableExist, err := r.redshifter.TableExist(schema, table)
	if err != nil {
		return err
	}
	if tableExist {
		klog.V(3).Infof("drop table %v", table)
		err = r.redshifter.DropTable(tx, schema, table)
		if err != nil {
			return err
		}
	}

	klog.V(3).Infof("move table %v -> %v", reloadedTable, table)
	err = r.redshifter.RenameTable(tx, schema, reloadedTable, table)
	if err != nil {
		return err
	}

	if group != nil {
		klog.V(3).Infof("granting schema access for table: %v to group: %v", table, *group)
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

	// notify
	// TODO: make it generic for all git repos
	if r.notifier != nil {
		message := fmt.Sprintf(
			"Released *%s.%s*, <https://github.com/%s/blob/%s/%s | mask version>",
			schema,
			table,
			r.repo,
			r.version,
			r.filePath,
		)
		err = r.notifier.Notify(message)
		if err != nil {
			klog.Errorf("Error notifying, err: %v", err)
		}
	}

	return nil
}
