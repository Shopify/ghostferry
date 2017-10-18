package ghostferry

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
)

type TLSConfig struct {
	CertPath   string
	ServerName string

	tlsConfig *tls.Config
}

func (this *TLSConfig) BuildConfig() (*tls.Config, error) {
	if this.tlsConfig == nil {
		certPool := x509.NewCertPool()
		pem, err := ioutil.ReadFile(this.CertPath)
		if err != nil {
			return nil, err
		}

		if ok := certPool.AppendCertsFromPEM(pem); !ok {
			return nil, errors.New("unable to append pem")
		}

		this.tlsConfig = &tls.Config{
			RootCAs:    certPool,
			ServerName: this.ServerName,
		}
	}

	return this.tlsConfig, nil
}

type Config struct {
	SourceHost string
	SourcePort uint16
	SourceUser string
	SourcePass string

	TargetHost string
	TargetPort uint16
	TargetUser string
	TargetPass string

	DatabaseTargets map[string]string

	SourceTLS *TLSConfig
	TargetTLS *TLSConfig

	// Config for Ferry
	MaxWriteRetriesOnTargetDBError int
	TableFilter                    TableFilter

	// Config for BinlogStreamer
	MyServerId uint32

	// Config for DataIterator
	IterateChunksize        uint64
	MaxIterationReadRetries int
	NumberOfTableIterators  int
	AutomaticCutover        bool

	// Config for the ControlServer
	ServerBindAddr string
	WebBasedir     string

	// Config for Throttler
	MaxVariableLoad map[string]int64
}

func (c *Config) ValidateConfig() error {
	if c.SourceHost == "" {
		return fmt.Errorf("source host is empty")
	}

	if c.SourcePort == 0 {
		return fmt.Errorf("source port is not specified")
	}

	if c.SourceUser == "" {
		return fmt.Errorf("source user is empty")
	}

	if c.TargetHost == "" {
		return fmt.Errorf("target host is empty")
	}

	if c.TargetPort == 0 {
		return fmt.Errorf("target port is not specified")
	}

	if c.TargetUser == "" {
		return fmt.Errorf("target user is empty")
	}

	if c.MyServerId == 0 {
		return fmt.Errorf("MyServerId must be non 0")
	}

	if c.TableFilter == nil {
		return fmt.Errorf("Table filter function must be provided")
	}

	if c.MaxWriteRetriesOnTargetDBError == 0 {
		c.MaxWriteRetriesOnTargetDBError = 5
	}

	if c.IterateChunksize == 0 {
		c.IterateChunksize = 200
	}

	if c.NumberOfTableIterators == 0 {
		c.NumberOfTableIterators = 4
	}

	if c.MaxIterationReadRetries == 0 {
		c.MaxIterationReadRetries = 5
	}

	if c.ServerBindAddr == "" {
		c.ServerBindAddr = "0.0.0.0:8000"
	}

	if c.WebBasedir == "" {
		c.WebBasedir = "."
	}

	if c.MaxVariableLoad == nil {
		c.MaxVariableLoad = make(map[string]int64)
	}

	return nil
}
