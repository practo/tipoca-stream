package controllers

import (
    "context"
	// tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
)

type Releaser struct {
	client *redshift.Redshift
}

func NewReleaser(
	ctx context.Context,
	schema string,
	secret map[string]string,
) (
	*Releaser,
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
		value, err := getSecretByKey(secret, key)
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

	client, err := redshift.NewRedshift(ctx, config)
	if err != nil {
		return nil, err
	}

	return &Releaser{
		client: client,
	}, nil
}

func (r *Releaser) Release(topic string) error {
	return nil
}
