package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/copydb"
	"github.com/sirupsen/logrus"
)

func usage() {
	fmt.Printf("ghostferry-copydb built with ghostferry %s\n", ghostferry.VersionString)
	fmt.Printf("Usage: %s [OPTIONS] path/to/config/file.json\n", os.Args[0])
	flag.PrintDefaults()
}

var verbose bool
var dryrun bool
var stateFilePath string

func init() {
	flag.BoolVar(&verbose, "verbose", false, "Show verbose logging output")
	flag.BoolVar(&dryrun, "dryrun", false, "Do not actually perform the move, just connect and check settings")
	flag.StringVar(&stateFilePath, "resumestate", "", "Path to the state dump JSON file to resume Ghostferry with")
}

func errorAndExit(msg string) {
	fmt.Fprintf(os.Stderr, "error: %s\n", msg)
	os.Exit(1)
}

func main() {
	flag.Parse()
	if flag.NArg() == 0 {
		usage()
		os.Exit(1)
	}

	configFilePath := flag.Arg(0)
	if _, err := os.Stat(configFilePath); os.IsNotExist(err) {
		errorAndExit(fmt.Sprintf("%s does not exist", configFilePath))
	}

	if verbose {
		logrus.SetLevel(logrus.DebugLevel)
	}
	logger := logrus.WithField("tag", "ghostferry-copydb")

	// Default values for configurations
	config := &copydb.Config{
		Config: &ghostferry.Config{
			Source: &ghostferry.DatabaseConfig{
				Port: 3306,
				User: "ghostferry",
			},

			Target: &ghostferry.DatabaseConfig{
				Port: 3306,
				User: "ghostferry",
			},

			MyServerId:        99399,
			AutomaticCutover:  false,
		},
	}

	// Open and parse configurations
	f, err := os.Open(configFilePath)
	if err != nil {
		errorAndExit(fmt.Sprintf("failed to open config file: %v", err))
	}

	parser := json.NewDecoder(f)
	err = parser.Decode(&config)
	if err != nil {
		errorAndExit(fmt.Sprintf("failed to parse config file: %v", err))
	}

	// if the state file is not provided as command-line argument, check if we
	// are tracking state in a file according to the config. If so, and the file
	// exists, read from there
	runTargetInitialization := true
	if stateFilePath == "" {
		// unlike when provided as command-line argument, we don't require the
		// file to exist - it simply means that we will want to serialize to
		// this file on shutdown and *try* to resume from there on restart
		//
		// If absence of the file should be treated as error, the caller can set
		// the filename in the config *and* specify it as command line, in which
		// case we'll resume from the CLI argument and write to the config file,
		// but fail if the former does not exist
		if _, err := os.Stat(config.Config.StateFilename); !os.IsNotExist(err) {
			stateFilePath = config.Config.StateFilename
			logger.Infof("Reading state information from file specified in config: %s", stateFilePath)
		} else {
			logger.Infof("Skip reading state file specified in config: %s does not exist", config.Config.StateFilename)
		}
	} else if _, err := os.Stat(stateFilePath); os.IsNotExist(err) {
		errorAndExit(fmt.Sprintf("%s does not exist", stateFilePath))
	} else {
		logger.Infof("Reading state information from file specified on command-line: %s", stateFilePath)
	}
	if stateFilePath != "" {
		logger.Debugf("Reading state information from %s", stateFilePath)
		f, err := os.Open(stateFilePath)
		if err != nil {
			errorAndExit(fmt.Sprintf("failed to open state file: %v", err))
		}

		parser := json.NewDecoder(f)
		err = parser.Decode(&config.Config.StateToResumeFrom)
		if err != nil {
			errorAndExit(fmt.Sprintf("failed to parse state file: %v", err))
		}

		logger.Debugf("Parsing state file %s successful", stateFilePath)
		runTargetInitialization = false
	}

	err = config.InitializeAndValidateConfig()
	if err != nil {
		errorAndExit(fmt.Sprintf("failed to validate config: %v", err))
	}

	ferry := copydb.NewFerry(config)

	err = ferry.Initialize()
	if err != nil {
		errorAndExit(fmt.Sprintf("failed to initialize ferry: %v", err))
	}

	err = ferry.Start()
	if err != nil {
		errorAndExit(fmt.Sprintf("failed to start ferry: %v", err))
	}

	if dryrun {
		fmt.Println("exiting due to dryrun")
		return
	}

	if runTargetInitialization {
		logger.Debugf("Initializing target database tables")
		err = ferry.CreateDatabasesAndTables()
		if err != nil {
			errorAndExit(fmt.Sprintf("failed to create databases and tables: %v", err))
		}
	} else {
		logger.Debugf("Skip initializing target database tables: resuming state")
	}

	ferry.Run()
}
