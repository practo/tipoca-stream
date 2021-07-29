package controllers

import (
	"context"
	"fmt"
	"github.com/practo/tipoca-stream/pkg/redshift"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewRedshiftConn(
	client client.Client,
	secretName,
	secretNamespace string,
	database *string,
) (
	*redshift.Redshift,
	error,
) {
	secret := make(map[string]string)
	k8sSecret, err := getSecret(context.Background(), client, secretName, secretNamespace)
	if err != nil {
		return nil, fmt.Errorf("Error getting secret, %v", err)
	}
	for key, value := range k8sSecret.Data {
		secret[key] = string(value)
	}
	if database != nil {
		secret["redshiftDatabase"] = *database
	}

	return NewRedshiftConnection(secret, "")
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
