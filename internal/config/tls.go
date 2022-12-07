package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	Server        bool
}

func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
	}
	// サーバはクライアントを、クライアントはサーバの証明書を検証できるよう証明書チェーンを設定
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(
			cfg.CertFile, cfg.KeyFile,
		)
		if err != nil {
			return nil, err
		}
	}

	if cfg.CAFile != "" {
		b, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM([]byte(b))
		if !ok {
			return nil, fmt.Errorf("failed to parse root certifications: %q", cfg.CAFile)
		}

		if cfg.Server {
			// INFO: サーバ側の場合
			//  サーバがクライアント証明書を検証する際に使用するルート認証局(ClientCAs)とサーバのポリシー(ClientAuth)を設定
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			// INFO: クライアント側の場合
			//   クライアントがサーバ証明書を検証する際に使用するルート認証局(RootCAs)を設定
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = cfg.ServerAddress
	}

	return tlsConfig, nil
}
