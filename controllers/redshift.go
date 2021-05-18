package controllers

import (
	"context"
	"fmt"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sync"
	"time"
)

type RedshiftCollector struct {
	redshifter *redshift.Redshift
}

func NewRedshiftCollector(client client.Client, secretName, secretNamespace string) (*RedshiftCollector, error) {
	secret := make(map[string]string)
	k8sSecret, err := getSecret(context.Background(), client, secretName, secretNamespace)
	if err != nil {
		return nil, fmt.Errorf("Error getting secret, %v", err)
	}
	for key, value := range k8sSecret.Data {
		secret[key] = string(value)
	}

	redshifter, err := NewRedshiftConnection(secret, "")
	if err != nil {
		return nil, err
	}

	return &RedshiftCollector{
		redshifter: redshifter,
	}, nil
}

func (r *RedshiftCollector) Collect(ctx context.Context, wg *sync.WaitGroup) error {
	defer wg.Done()
	// TODO: make it possible to remove below comment, create the view
	// expects the view to be present redshiftsink_operator.scan_query_total
	for {
		select {
		case <-ctx.Done():
			klog.V(2).Infof("ctx cancelled, bye collector")
			return nil
		case <-time.After(time.Second * 1800):
			err := r.redshifter.CollectQueryTotal(ctx)
			if err != nil {
				klog.Errorf("Redshift Collector shutdown due to error: %v", err)
				return err
			}
		}
	}

	return nil
}

func NewRedshiftConnection(
	secret map[string]string,
	schema string,
) (
	*redshift.Redshift,
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

	conn, err := redshift.NewRedshift(config)
	if err != nil {
		return nil, fmt.Errorf(
			"Error creating redshift connecton, config: %+v, err: %v",
			config, err)
	}

	return conn, nil
}
