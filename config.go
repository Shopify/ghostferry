package ghostferry

import "fmt"

type Config struct {
	SourceHost string
	SourcePort uint16
	SourceUser string
	SourcePass string

	TargetHost string
	TargetPort uint16
	TargetUser string
	TargetPass string

	ApplicableDatabases map[string]bool
	ApplicableTables    map[string]bool

	// Config for Ferry
	MaxWriteRetriesOnTargetDBError int

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
