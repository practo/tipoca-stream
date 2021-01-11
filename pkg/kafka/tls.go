package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
)

// TLSConfig stores the base64 encoded string for all the certificate and keys
// required for the TLS setup and authentication with kafka for the client
type TLSConfig struct {
	Enable   bool   `yaml:"enable"`
	UserCert string `yaml:"userCert"`
	UserKey  string `yaml:"userKey"`
	CACert   string `yaml:"caCert"`
}

func NewTLSConfig(configTLS TLSConfig) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	userCert, err := base64.StdEncoding.DecodeString(configTLS.UserCert)
	if err != nil {
		return &tlsConfig, err
	}

	userKey, err := base64.StdEncoding.DecodeString(configTLS.UserKey)
	if err != nil {
		return &tlsConfig, err
	}

	caCert, err := base64.StdEncoding.DecodeString(configTLS.CACert)
	if err != nil {
		return &tlsConfig, err
	}

	cert, err := tls.X509KeyPair([]byte(userCert), []byte(userKey))
	if err != nil {
		return &tlsConfig, fmt.Errorf(
			"Error making keyPair from userCert and userKey, err: %v",
			err,
		)
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(caCert))
	tlsConfig.RootCAs = caCertPool
	tlsConfig.BuildNameToCertificate()

	return &tlsConfig, nil
}
