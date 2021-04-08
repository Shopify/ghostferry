package ghostferry

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

const (
	VerifierTypeChecksumTable  = "ChecksumTable"
	VerifierTypeIterative      = "Iterative"
	VerifierTypeInline         = "Inline"
	VerifierTypeNoVerification = "NoVerification"

	DefaultNet          = "tcp"
	DefaultMarginalia   = "application:ghostferry"
	MySQLNumParamsLimit = 1<<16 - 1 // see num_params https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html
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
	Net       string
	User      string
	Pass      string
	Collation string
	Params    map[string]string
	TLS       *TLSConfig

	// ReadTimeout is used to configure the MySQL client timeout for waiting for data from server.
	// Timeout is in seconds. Defaults to 0, which means no timeout.
	ReadTimeout uint64

	// WriteTimeout is used to configure the MySQL client timeout for writing data to server.
	// Timeout is in seconds. Defaults to 0, which means no timeout.
	WriteTimeout uint64

	// SQL annotations used to differentiate Ghostferry's DMLs
	// against other actor's. This will default to the defaultMarginalia
	// constant above if not set.
	//
	// This is used to ensure any changes to the Target during the move process
	// are performed only by Ghostferry (until cutover). Otherwise, the modification
	// will be identified as data corruption and fail the move.
	Marginalia string
}

func (c *DatabaseConfig) MySQLConfig() (*mysql.Config, error) {
	var addr string

	if c.Net == "unix" {
		addr = c.Host
	} else {
		addr = fmt.Sprintf("%s:%d", c.Host, c.Port)
	}

	cfg := &mysql.Config{
		User:                 c.User,
		Passwd:               c.Pass,
		Net:                  c.Net,
		Addr:                 addr,
		Collation:            c.Collation,
		Params:               c.Params,
		AllowNativePasswords: true,
		MultiStatements:      true,
	}

	if c.ReadTimeout != 0 {
		cfg.ReadTimeout = time.Duration(c.ReadTimeout) * time.Second
	}

	if c.WriteTimeout != 0 {
		cfg.WriteTimeout = time.Duration(c.WriteTimeout) * time.Second
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

	if c.Net == "" {
		c.Net = DefaultNet
	} else if c.Net == "unix" {
		c.Port = 0
	} else if c.Net != "tcp" && c.Net != "unix" {
		return fmt.Errorf("net is unknown (valid modes: tcp, unix)")
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

	if c.Marginalia == "" {
		c.Marginalia = DefaultMarginalia
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

	return sql.Open("mysql", dbCfg.FormatDSN(), c.Marginalia)
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
	// time.ParseDuration. If nothing is specified, the InlineVerifier will not
	// try to estimate the downtime and will always allow cutover.
	MaxExpectedDowntime string

	// The interval at which the periodic binlog reverification occurs, in the
	// format of time.ParseDuration. Default: 1s.
	VerifyBinlogEventsInterval string

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
	} else {
		c.maxExpectedDowntime = time.Duration(0)
	}

	if c.VerifyBinlogEventsInterval == "" {
		c.VerifyBinlogEventsInterval = "1s"
	}

	c.verifyBinlogEventsInterval, err = time.ParseDuration(c.VerifyBinlogEventsInterval)
	if err != nil {
		return err
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

// SchemaName => TableName => ColumnName => struct{}{}
// These columns will be ignored during InlineVerification
type ColumnIgnoreConfig map[string]map[string]map[string]struct{}

func (c ColumnIgnoreConfig) IgnoredColumnsFor(schemaName, tableName string) map[string]struct{} {
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

// SchemaName => TableName => IndexName
// These indices will be forced for queries in InlineVerification
type ForceIndexConfig map[string]map[string]string

func (c ForceIndexConfig) IndexFor(schemaName, tableName string) string {
	tableConfig, found := c[schemaName]
	if !found {
		return ""
	}

	index, found := tableConfig[tableName]
	if !found {
		return ""
	}

	return index
}

// CascadingPaginationColumnConfig to configure pagination columns to be
// used. The term `Cascading` to denote that greater specificity takes
// precedence.
type CascadingPaginationColumnConfig struct {
	// PerTable has greatest specificity and takes precedence over the other options
	PerTable map[string]map[string]string // SchemaName => TableName => ColumnName

	// FallbackColumn is a global default to fallback to and is less specific than the
	// default, which is the Primary Key
	FallbackColumn string
}

// PaginationColumnFor is a helper function to retrieve the column name to paginate by
func (c *CascadingPaginationColumnConfig) PaginationColumnFor(schemaName, tableName string) (string, bool) {
	if c == nil {
		return "", false
	}

	tableConfig, found := c.PerTable[schemaName]
	if !found {
		return "", false
	}

	column, found := tableConfig[tableName]
	if !found {
		return "", false
	}

	return column, true
}

// FallbackPaginationColumnName retreives the column name specified as a fallback when the Primary Key isn't suitable for pagination
func (c *CascadingPaginationColumnConfig) FallbackPaginationColumnName() (string, bool) {
	if c == nil || c.FallbackColumn == "" {
		return "", false
	}

	return c.FallbackColumn, true
}

type DataIterationBatchSizePerTableOverride struct {
	// Lower limit for rowSize, if a rowSize <= MinRowSize, ControlPoints[MinRowSize] will be used
	MinRowSize int
	// Upper limit for rowSize, if a rowSize >= MaxRowSize, ControlPoints[MaxRowSize] will be used
	MaxRowSize int
	// Map of rowSize  => batchSize used to calculate batchSize for new rowSizes, results stored in TableOverride
	ControlPoints map[int]uint64
	// Map of schemaName(source schema) => tableName => batchSize to override default values for certain tables
	TableOverride map[string]map[string]uint64
}

func (d *DataIterationBatchSizePerTableOverride) Validate() error {
	if d != nil {
		if _, found := d.ControlPoints[d.MinRowSize]; !found {
			return fmt.Errorf("must provide batch size for MinRowSize")
		}
		if _, found := d.ControlPoints[d.MaxRowSize]; !found {
			return fmt.Errorf("must provide batch size for MaxRowSize")
		}
		if d.TableOverride == nil {
			d.TableOverride = map[string]map[string]uint64{}
		}
	}
	return nil
}

func (d *DataIterationBatchSizePerTableOverride) UpdateBatchSizes(db *sql.DB, tables TableSchemaCache) error {
	schemaTablesMap := map[string][]string{}

	for _, table := range tables {
		if _, found := d.TableOverride[table.Schema][table.Name]; found {
			continue
		}
		schemaTablesMap[table.Schema] = append(schemaTablesMap[table.Schema], table.Name)
	}

	for schemaName, tableNames := range schemaTablesMap {
		if _, found := d.TableOverride[schemaName]; !found {
			d.TableOverride[schemaName] = map[string]uint64{}
		}
		query := fmt.Sprintf(
			`SELECT T1.TABLE_NAME, AVG_ROW_LENGTH, MAX_BATCH_SIZE
					FROM information_schema.TABLES T1
						JOIN (SELECT TABLE_NAME, TABLE_SCHEMA, floor(%d / count(*)) MAX_BATCH_SIZE
						FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME in ('%s')
						GROUP BY table_name) T2 USING (TABLE_SCHEMA, TABLE_NAME);`,
			MySQLNumParamsLimit,
			schemaName,
			strings.Join(tableNames, `', '`),
		)
		rows, err := db.Query(query)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var avgRowLength, maxBatchSize int
			var tableName string
			err = rows.Scan(&tableName, &avgRowLength, &maxBatchSize)
			if err != nil {
				return err
			}
			// MySQL has an upper limit of 2^16 for num_params for prepared statements. This will cause an error when
			// we attempt to execute a INSERT statement with > 65535 parameters.
			// If batchSize > 65535/num_columns, we will use 65535/num_columns
			batchSize := d.CalculateBatchSize(avgRowLength)
			if batchSize >= maxBatchSize {
				batchSize = maxBatchSize
			}
			d.TableOverride[schemaName][tableName] = uint64(batchSize)
		}

	}
	return nil
}

func (d *DataIterationBatchSizePerTableOverride) CalculateBatchSize(rowSize int) int {
	if batchSize, found := d.ControlPoints[rowSize]; found {
		return int(batchSize)
	}
	if rowSize >= d.MaxRowSize {
		return int(d.ControlPoints[d.MaxRowSize])
	}
	if rowSize <= d.MinRowSize {
		return int(d.ControlPoints[d.MinRowSize])
	}

	closestPointLessThanRowSize := d.MinRowSize
	closestPointGreaterThanRowSize := d.MaxRowSize
	for size := range d.ControlPoints {
		if size < rowSize && rowSize-size < rowSize-closestPointLessThanRowSize {
			closestPointLessThanRowSize = size
		}
		if size > rowSize && size-rowSize < closestPointGreaterThanRowSize-rowSize {
			closestPointGreaterThanRowSize = size
		}
	}
	return linearInterpolation(
		rowSize, closestPointLessThanRowSize,
		int(d.ControlPoints[closestPointLessThanRowSize]),
		closestPointGreaterThanRowSize,
		int(d.ControlPoints[closestPointGreaterThanRowSize]),
	)
}

func linearInterpolation(x, x0, y0, x1, y1 int) int {
	return y0*(x1-x)/(x1-x0) + y1*(x-x0)/(x1-x0)
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

	// This optional config uses different data points to calculate
	// batch size per table using linear interpolation
	DataIterationBatchSizePerTableOverride *DataIterationBatchSizePerTableOverride

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

	// This specifies whether or not Ghostferry will dump the current state to stdout
	// before exiting due to an error.
	//
	// Optional: defaults to false
	DumpStateToStdoutOnError bool

	// This excludes schema cache from the state dump in both the HTTP callback
	// and the stdout dumping. This may save a lot of space if you don't need
	// to deal with schema migrations.
	DoNotIncludeSchemaCacheInStateDump bool

	// Config for the ControlServer
	ServerBindAddr string
	WebBasedir     string

	// TODO: refactor control server config out of the base ferry at some point
	// This adds optional buttons in the web ui that runs a script located at the
	// path specified.
	// The format is "script name" => ["path to script", "arg1", "arg2"]. The script name
	// will be displayed on the web ui.
	ControlServerCustomScripts map[string][]string

	// Report progress via an HTTP callback. The Payload field of the callback
	// will be sent to the server as the CustomPayload field in the Progress
	// struct. The unit of ProgressReportFrequency is in milliseconds.
	ProgressCallback        HTTPCallback
	ProgressReportFrequency int

	// Report state via an HTTP callback. The SerializedState struct will be
	// sent as the Payload parameter. The unit of StateReportFrequency is
	// in milliseconds.
	StateCallback        HTTPCallback
	StateReportFrequency int


	// Report error via an HTTP callback. The Payload field will contain the ErrorType,
	// ErrorMessage and the StateDump.
	ErrorCallback HTTPCallback

	// Report when ghostferry is entering cutover
	CutoverLock   			HTTPCallback

	// Report when ghostferry is finished cutover
	CutoverUnlock 			HTTPCallback

	// If the callback returns a non OK status, these two values configure the number of times Ferry should attempt to
	// retry acquiring the cutover lock, and for how long the Ferry should wait
	// before attempting another lock acquisition
	// MaxCutoverRetries default is 1 retry
	// CutoverRetryWaitSeconds default is 1 second
	MaxCutoverRetries       int
	CutoverRetryWaitSeconds int

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
	//   - Specifically, the PaginationKey of this row on both the source and the target are
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
	CompressedColumnsForVerification ColumnCompressionConfig

	// This config is also for inline verification for the same special case of
	// Ghostferry as documented with the CompressedColumnsForVerification option:
	//
	// - If you're copying a table where the data is partially already on the
	//   the target through some other means.
	// - A difference in a particular column could be acceptable.
	//   - An example would be a table with a data field and a created_at field.
	//     Maybe the created_at field is not important for data integrity as long
	//     as the data field is correct.
	// - Putting the column in this config will cause the InlineVerifier to skip
	//   this column for verification.
	IgnoredColumnsForVerification ColumnIgnoreConfig

	// Map an index to a table, will add `FORCE INDEX (index_name)` to the fingerprint SELECT query.
	// Index hinting might be necessary if you are running into slow queries during copy on your target.
	//
	// Example:
	//
	// "ForceIndexForVerification": {
	//   "blog": {
	//     "users": "ix_users_some_id"
	//   }
	// }
	//
	ForceIndexForVerification ForceIndexConfig

	// Ghostferry requires a single numeric column to paginate over tables. Inferring that column is done in the following exact order:
	// 1. Use the PerTable pagination column, if configured for a table. Fail if we cannot find this column in the table.
	// 2. Use the table's primary key column as the pagination column. Fail if the primary key is not numeric or is a composite key without a FallbackColumn specified.
	// 3. Use the FallbackColumn pagination column, if configured. Fail if we cannot find this column in the table.
	CascadingPaginationColumnConfig *CascadingPaginationColumnConfig

	// SkipTargetVerification is used to enable or disable target verification during moves.
	// This feature is currently only available while using the InlineVerifier.
	//
	// This does so by inspecting the annotations (configured as Marginalia in the DatabaseConfig above)
	// and will fail the move unless all applicable DMLs (as identified by the sharding key) sent to the
	// Target were sent from Ghostferry.
	//
	// NOTE:
	// The Target database must be configured with binlog_rows_query_log_events
	// set to "ON" for this to function properly. Ghostferry not allow the move
	// process to begin if this is enabled and the above option is set to "OFF".
	//
	// Required: defaults to false
	SkipTargetVerification bool

	// During initialization, Ghostferry will raise an error if any
	// foreign key constraints are detected in the source database.
	//
	// This check can be bypassed by setting this value to true.
	//
	// WARNING:
	// Using Ghostferry with foreign keys is highly discouraged and
	// disabling this check makes no guarantees of the success of the run.
	//
	// Required: defaults to false
	SkipForeignKeyConstraintsCheck bool

	// EnableRowBatchsize is used to enable or disable the calculation of number of bytes written for each row batch.
	//
	// Optional: Defaults to false.
	//
	// NOTE:
	// Turning off the EnableRowBatchSize flag would show the NumBytes written per RowBatch to be zero
	// in the Progress. This behaviour is perfectly okay and doesn't mean there are no rows being written
	// to the target DB.
	EnableRowBatchSize bool
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

	if err := c.DataIterationBatchSizePerTableOverride.Validate(); err != nil {
		return fmt.Errorf("DataIterationBatchSizePoints invalid: %s", err)
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

	if c.MaxCutoverRetries == 0 {
		c.MaxCutoverRetries = 1
	}

	if c.CutoverRetryWaitSeconds == 0 {
		c.CutoverRetryWaitSeconds = 1
	}

	return nil
}
