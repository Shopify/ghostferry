package ghostferry

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/go-sql-driver/mysql"
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

type DatabaseConfig struct {
	Host      string
	Port      uint16
	User      string
	Pass      string
	Collation string
	Params    map[string]string

	TLS *TLSConfig
}

func (dbc *DatabaseConfig) MySQLConfig() (*mysql.Config, error) {
	cfg := &mysql.Config{
		User:      dbc.User,
		Passwd:    dbc.Pass,
		Net:       "tcp",
		Addr:      fmt.Sprintf("%s:%d", dbc.Host, dbc.Port),
		Collation: dbc.Collation,
		Params:    dbc.Params,
	}

	if dbc.TLS != nil {
		tlsConfig, err := dbc.TLS.BuildConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %v", err)
		}

		cfgName := fmt.Sprintf("%s@%s:%s", dbc.User, dbc.Host, dbc.Port)

		err = mysql.RegisterTLSConfig(cfgName, tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to register TLS config: %v", err)
		}

		cfg.TLSConfig = cfgName
	}

	return cfg, nil
}

func (c *DatabaseConfig) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("host is empty")
	}

	if c.Port == 0 {
		return fmt.Errorf("port is not specified")
	}

	if c.User == "" {
		return fmt.Errorf("user is empty")
	}

	return nil
}

type Config struct {
	Source DatabaseConfig
	Target DatabaseConfig

	DatabaseRewrites map[string]string
	TableRewrites    map[string]string

	// Config for Ferry
	MaxWriteRetriesOnTargetDBError int

	TableFilter TableFilter
	CopyFilter  CopyFilter

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
}

func (c *Config) ValidateConfig() error {
	if err := c.Source.Validate(); err != nil {
		return fmt.Errorf("source: %s", err)
	}

	if err := c.Target.Validate(); err != nil {
		return fmt.Errorf("target: %s", err)
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

	return nil
}
