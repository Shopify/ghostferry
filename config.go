package ghostferry

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

const (
	VerifierTypeChecksumTable  = "ChecksumTable"
	VerifierTypeIterative      = "Iterative"
	VerifierTypeInline         = "Inline"
	VerifierTypeNoVerification = "NoVerification"
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

func (c *DatabaseConfig) MySQLConfig() (*mysql.Config, error) {
	cfg := &mysql.Config{
		User:      c.User,
		Passwd:    c.Pass,
		Net:       "tcp",
		Addr:      fmt.Sprintf("%s:%d", c.Host, c.Port),
		Collation: c.Collation,
		Params:    c.Params,

		MultiStatements: true,
	}

	if c.TLS != nil {
		tlsConfig, err := c.TLS.BuildConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %v", err)
		}

		cfgName := fmt.Sprintf("%s@%s:%s", c.User, c.Host, c.Port)

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

	err := c.assertParamSet("time_zone", "'+00:00'")
	if err != nil {
		return err
	}

	err = c.assertParamSet("sql_mode", "'STRICT_ALL_TABLES,NO_BACKSLASH_ESCAPES'")
	if err != nil {
		return err
	}

	return nil
}

func (c *DatabaseConfig) SqlDB(logger *logrus.Entry) (*sql.DB, error) {
	dbCfg, err := c.MySQLConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build database config: %s", err)
	}

	if logger != nil {
		logger.WithField("dsn", MaskedDSN(dbCfg)).Info("connecting to database")
	}

	return sql.Open("mysql", dbCfg.FormatDSN())
}

func (c *DatabaseConfig) assertParamSet(param, value string) error {
	if c.Params == nil {
		c.Params = make(map[string]string)
	}

	if c.Params[param] != "" && c.Params[param] != value {
		return fmt.Errorf("%s must be set to %s", param, value)
	}
	c.Params[param] = value

	return nil
}

type InlineVerifierConfig struct {
	// The maximum expected downtime during cutover, in the format of
	// time.ParseDuration.
	MaxExpectedDowntime string

	// The interval at which the periodic binlog reverification occurs, in the
	// format of time.ParseDuration.
	VerifyBinlogEventsInterval string

	// The maximum number of iterations to perform reverify in VerifyBeforeCutover
	FinalReverifyMaxIterations int

	verifyBinlogEventsInterval time.Duration
	maxExpectedDowntime        time.Duration
}

func (c *InlineVerifierConfig) Validate() error {
	var err error
	if c.MaxExpectedDowntime != "" {
		c.maxExpectedDowntime, err = time.ParseDuration(c.MaxExpectedDowntime)
		if err != nil {
			return err
		}
	}

	if c.VerifyBinlogEventsInterval == "" {
		c.VerifyBinlogEventsInterval = "1s"
	}

	c.verifyBinlogEventsInterval, err = time.ParseDuration(c.VerifyBinlogEventsInterval)
	if err != nil {
		return err
	}

	if c.FinalReverifyMaxIterations == 0 {
		c.FinalReverifyMaxIterations = 30
	}

	return nil
}

type IterativeVerifierConfig struct {
	// List of tables that should be ignored by the IterativeVerifier.
	IgnoredTables []string

	// List of columns that should be ignored by the IterativeVerifier.
	// This is in the format of table_name -> [list of column names]
	IgnoredColumns map[string][]string

	// The number of concurrent verifiers. Note that a single table can only be
	// assigned to one goroutine and currently multiple goroutines per table
	// is not supported.
	Concurrency int

	// The maximum expected downtime during cutover, in the format of
	// time.ParseDuration.
	MaxExpectedDowntime string

	// Map of the table and column identifying the compression type
	// (if any) of the column. This is used during verification to ensure
	// the data was successfully copied as some compression algorithms can
	// output different compressed data with the same input data.
	//
	// The data structure is a map of table names to a map of column names
	// to the compression algorithm.
	// ex: {books: {contents: snappy}}
	//
	// Currently supported compression algorithms are:
	//	1. Snappy (https://google.github.io/snappy/) as "SNAPPY"
	//
	// Optional: defaults to empty map/no compression
	//
	// Note that the IterativeVerifier is in the process of being deprecated.
	// If this is specified, ColumnCompressionConfig should also be filled out in
	// the main Config.
	TableColumnCompression TableColumnCompressionConfig
}

func (c *IterativeVerifierConfig) Validate() error {
	if c.MaxExpectedDowntime != "" {
		_, err := time.ParseDuration(c.MaxExpectedDowntime)
		if err != nil {
			return err
		}
	}

	if c.Concurrency == 0 {
		c.Concurrency = 4
	}

	return nil
}

// SchemaName => TableName => ColumnName => CompressionAlgorithm
// Example: blog1 => articles => body => snappy
//          (SELECT body FROM blog1.articles => returns compressed blob)
type ColumnCompressionConfig map[string]map[string]map[string]string

func (c ColumnCompressionConfig) CompressedColumnsFor(schemaName, tableName string) map[string]string {
	tableConfig, found := c[schemaName]
	if !found {
		return nil
	}

	columnsConfig, found := tableConfig[tableName]
	if !found {
		return nil
	}

	return columnsConfig
}

type Config struct {
	// Source database connection configuration
	//
	// Required
	Source *DatabaseConfig

	// Target database connection configuration
	//
	// Required
	Target *DatabaseConfig

	// Map database name on the source database (key of the map) to a
	// different name on the target database (value of the associated key).
	// This allows one to move data and change the database name in the
	// process.
	//
	// Optional: defaults to empty map/no rewrites
	DatabaseRewrites map[string]string

	// Map the table name on the source database to a different name on
	// the target database. See DatabaseRewrite.
	//
	// Optional: defaults to empty map/no rewrites
	TableRewrites map[string]string

	// The maximum number of retries for writes if the writes failed on
	// the target database.
	//
	// Optional: defaults to 5.
	DBWriteRetries int

	// Filter out the databases/tables when detecting the source databases
	// and tables.
	//
	// Required
	TableFilter TableFilter

	// Filter out unwanted data/events from being copied.
	//
	// Optional: defaults to nil/no filter.
	CopyFilter CopyFilter

	// The server id used by Ghostferry to connect to MySQL as a replication
	// slave. This id must be unique on the MySQL server. If 0 is specified,
	// a random id will be generated upon connecting to the MySQL server.
	//
	// Optional: defaults to an automatically generated one
	MyServerId uint32

	// The maximum number of binlog events to write at once. Note this is a
	// maximum: if there are not a lot of binlog events, they will be written
	// one at a time such the binlog streamer lag is as low as possible. This
	// batch size will only be hit if there is a log of binlog at the same time.
	//
	// Optional: defaults to 100
	BinlogEventBatchSize int

	// The batch size used to iterate the data during data copy. This batch size
	// is always used: if this is specified to be 100, 100 rows will be copied
	// per iteration.
	//
	// With the current implementation of Ghostferry, we need to lock the rows
	// we select. This means, the larger this number is, the longer we need to
	// hold this lock. On the flip side, the smaller this number is, the slower
	// the copy will likely be.
	//
	// Optional: defaults to 200
	DataIterationBatchSize uint64

	// The maximum number of retries for reads if the reads fail on the source
	// database.
	//
	// Optional: defaults to 5
	DBReadRetries int

	// This specify the number of concurrent goroutines, each iterating over
	// a single table.
	//
	// At this point in time, parallelize iteration within a single table. This
	// may be possible to add to the future.
	//
	// Optional: defaults to 4
	DataIterationConcurrency int

	// This specifies if Ghostferry will pause before cutover or not.
	//
	// Optional: defaults to false
	AutomaticCutover bool

	// This specifies whether or not Ferry.Run will handle SIGINT and SIGTERM
	// by dumping the current state to stdout and the error HTTP callback.
	// The dumped state can be used to resume Ghostferry.
	DumpStateOnSignal bool

	// Config for the ControlServer
	ServerBindAddr string
	WebBasedir     string

	// Report progress via an HTTP callback. The Payload field of the callback
	// will be sent to the server as the CustomPayload field in the Progress
	// struct The unit of ProgressReportFrequency is in milliseconds.
	ProgressCallback        HTTPCallback
	ProgressReportFrequency int

	// The state to resume from as dumped by the PanicErrorHandler.
	// If this is null, a new Ghostferry run will be started. Otherwise, the
	// reconciliation process will start and Ghostferry will resume after that.
	StateToResumeFrom *SerializableState

	// The verifier to use during the run. Valid choices are:
	// ChecksumTable
	// Iterative
	// NoVerification
	//
	// If it is left blank, the Verifier member variable on the Ferry will be
	// used. If that member variable is nil, no verification will be done.
	VerifierType string

	// Only useful if VerifierType == Iterative.
	// This specifies the configurations to the IterativeVerifier.
	//
	// This option is in the process of being deprecated.
	IterativeVerifierConfig IterativeVerifierConfig

	// Only useful if VerifierType == Inline.
	// This specifies the configurations to the InlineVerifierConfig.
	InlineVerifierConfig InlineVerifierConfig

	// For old versions mysql<5.6.2, MariaDB<10.1.6 which has no related var
	// Make sure you have binlog_row_image=FULL when turning on this
	SkipBinlogRowImageCheck bool

	// This config is necessary for inline verification for a special case of
	// Ghostferry:
	//
	// - If you are copying a table where the data is already partially on the
	//   target through some other means.
	//   - Specifically, the PK of this row on both the source and the target are
	//     the same. Thus, INSERT IGNORE will skip copying this row, leaving the
	//     data on the target unchanged.
	//   - If the data on the target is already identical to the source, then
	//     verification will pass and all is well.
	// - However, if this data is compressed with a non-determinstic algorithm
	//   such as snappy, the compressed blob may not be equal even when the
	//   uncompressed data is equal.
	// - This column signals to the InlineVerifier that it needs to decompress
	//   the data to compare identity.
	//
	// Note: a similar option exists in IterativeVerifier. However, the
	// IterativeVerifier is being deprecated and this will be the correct place
	// to specify it if you don't need the IterativeVerifier.
	ColumnCompressionConfig ColumnCompressionConfig
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

	if c.StateToResumeFrom != nil && c.StateToResumeFrom.GhostferryVersion != VersionString {
		return fmt.Errorf("StateToResumeFrom version mismatch: resume = %s, current = %s", c.StateToResumeFrom.GhostferryVersion, VersionString)
	}

	if c.VerifierType == VerifierTypeIterative {
		if err := c.IterativeVerifierConfig.Validate(); err != nil {
			return fmt.Errorf("IterativeVerifierConfig invalid: %v", err)
		}
	} else if c.VerifierType == VerifierTypeInline {
		if err := c.InlineVerifierConfig.Validate(); err != nil {
			return fmt.Errorf("InlineVerifierConfig invalid: %v", err)
		}
	}

	if c.DBWriteRetries == 0 {
		c.DBWriteRetries = 5
	}

	if c.DataIterationBatchSize == 0 {
		c.DataIterationBatchSize = 200
	}

	if c.BinlogEventBatchSize == 0 {
		c.BinlogEventBatchSize = 100
	}

	if c.DataIterationConcurrency == 0 {
		c.DataIterationConcurrency = 4
	}

	if c.DBReadRetries == 0 {
		c.DBReadRetries = 5
	}

	if c.ServerBindAddr == "" {
		c.ServerBindAddr = "0.0.0.0:8000"
	}

	if c.WebBasedir == "" {
		c.WebBasedir = "."
	}

	return nil
}
