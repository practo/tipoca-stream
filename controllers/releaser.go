package controllers

import (
	"context"
	// tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
)

type releaser struct {
	redshiftClient *redshift.Redshift
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

	redshiftClient, err := redshift.NewRedshift(ctx, config)
	if err != nil {
		return nil, err
	}

	return &releaser{
		redshiftClient: redshiftClient,
	}, nil
}

func (r *releaser) release(topic string) error {

	return nil
}
