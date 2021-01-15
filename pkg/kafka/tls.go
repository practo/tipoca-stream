package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
)

// TLSConfig stores the string for all the certificate and keys
// required for the TLS setup and authentication with kafka for the client
type TLSConfig struct {
	Enable   bool   `yaml:"enable"`
	UserCert string `yaml:"userCert"`
	UserKey  string `yaml:"userKey"`
	CACert   string `yaml:"caCert"`
}

func NewTLSConfig(configTLS TLSConfig) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	cert, err := tls.X509KeyPair(
		[]byte(configTLS.UserCert),
		[]byte(configTLS.UserKey),
	)
	if err != nil {
		return &tlsConfig, fmt.Errorf(
			"Error making keyPair from userCert and userKey, err: %v",
			err,
		)
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(configTLS.CACert))
	tlsConfig.RootCAs = caCertPool
	tlsConfig.BuildNameToCertificate()

	return &tlsConfig, nil
}
