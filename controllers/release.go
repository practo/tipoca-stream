package controllers

import (
	"context"
	"fmt"
	"time"

	"database/sql"
	"github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/api/v1"
	"github.com/practo/tipoca-stream/pkg/notify"
	"github.com/practo/tipoca-stream/pkg/redshift"
	"github.com/practo/tipoca-stream/pkg/transformer"
)

type releaser struct {
	schema         string
	repo           string
	filePath       string
	currentVersion string
	desiredVersion string
	redshifter     *redshift.Redshift
	notifier       notify.Notifier
	rsk            *tipocav1.RedshiftSink
}

func newReleaser(
	repo string,
	filePath string,
	currentVersion string,
	desiredVersion string,
	secret map[string]string,
	rsk *tipocav1.RedshiftSink,
) (
	*releaser,
	error,
) {
	schema := rsk.Spec.Loader.RedshiftSchema
	redshifter, err := NewRedshiftConnection(secret, schema)
	if err != nil {
		return nil, err
	}
	return &releaser{
		schema:         schema,
		redshifter:     redshifter,
		repo:           repo,
		filePath:       filePath,
		currentVersion: currentVersion,
		desiredVersion: desiredVersion,
		notifier:       makeNotifier(secret),
		rsk:            rsk,
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

func (r *releaser) releaseTopic(
	ctx context.Context,
	tx *sql.Tx,
	schema string,
	topic string,
	tableSuffix string,
	group *string,
	status *status,
	patcher *statusPatcher,
) error {
	_, _, table := transformer.ParseTopic(topic)
	reloadedTable := table + tableSuffix

	tableExist, err := r.redshifter.TableExist(ctx, schema, table)
	if err != nil {
		return err
	}
	if tableExist {
		klog.V(4).Infof("drop table %v", table)
		err = r.redshifter.DropTableWithCascade(ctx, tx, schema, table)
		if err != nil {
			return err
		}
	}

	klog.V(4).Infof("move table %v -> %v", reloadedTable, table)
	err = r.redshifter.RenameTable(ctx, tx, schema, reloadedTable, table)
	if err != nil {
		return err
	}

	if group != nil {
		klog.V(4).Infof("granting schema access for table: %v to group: %v", table, *group)
		err = r.redshifter.GrantSchemaAccess(ctx, tx, schema, table, *group)
		if err != nil {
			return err
		}
	}

	statusCopy := status.deepCopy()

	// store info to cleanup dead consumer group after release
	tg := topicGroup(r.rsk, topic)
	if tg != nil {
		addDeadConsumerGroups(r.rsk, consumerGroupID(
			r.rsk.Name, r.rsk.Namespace, tg.ID, "-batcher"),
		)
		addDeadConsumerGroups(r.rsk, consumerGroupID(
			r.rsk.Name, r.rsk.Namespace, tg.ID, "-loader"),
		)
	}

	status.updateTopicsOnRelease(topic)
	status.updateTopicGroup(topic)
	status.deleteLoaderTopicGroupCurrentOffset(topic)
	status.updateMaskStatus()
	status.info()

	err = patcher.Patch(ctx, statusCopy.rsk, status.rsk, fmt.Sprintf("release %s", topic))
	if err != nil {
		// revert (patched later)
		status.overwrite(statusCopy)
		klog.V(2).Infof("rsk/%s reverted release for %s", status.rsk.Name, topic)
		return fmt.Errorf("Error patching rsk status, err: %v, release failed for :%s", err, topic)
	}

	patcher.allowMain = false

	// release
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("Error committing tx, err:%v\n", err)
	}
	klog.V(5).Infof("released topic in redshift: %s", topic)
	time.Sleep(3 * time.Second)

	return nil
}

func (r *releaser) release(
	ctx context.Context,
	schema string,
	topic string,
	tableSuffix string,
	group *string,
	status *status,
	patcher *statusPatcher,
) error {
	tx, err := r.redshifter.Begin(ctx)
	if err != nil {
		return fmt.Errorf("Error creating database tx, err: %v\n", err)
	}

	err = r.releaseTopic(ctx, tx, schema, topic, tableSuffix, group, status, patcher)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			klog.Errorf(
				"Error rolling back failed release tx for topic: %s, rollbackErr: %v",
				topic,
				rollbackErr,
			)
		}
		return err
	}

	return nil
}

func (r *releaser) notifyTopicRelease(
	schema string,
	topic string,
	tableSuffix string,
) {
	_, _, table := transformer.ParseTopic(topic)

	// notify
	// TODO: make it generic for all git repos
	if r.notifier == nil {
		return
	}
	sha := r.desiredVersion
	if len(r.desiredVersion) >= 6 {
		sha = r.desiredVersion[:6]
	}
	message := fmt.Sprintf(
		"Released table *%s.%s* with mask-version: <https://github.com/%s/blob/%s/%s | %s> and <https://github.com/%s/compare/%s...%s | mask-changes>.",
		schema,
		table,
		r.repo,
		r.desiredVersion,
		r.filePath,
		sha,
		r.repo,
		r.currentVersion,
		r.desiredVersion,
	)

	err := r.notifier.Notify(message)
	if err != nil {
		klog.Errorf("release notification failed, err: %v", err)
	}
}

type releaseCache struct {
	lastCacheRefresh *int64
}
