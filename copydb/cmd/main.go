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

			ControlServerConfig: &ghostferry.ControlServerConfig{
				Enabled: true,
			},

			AutomaticCutover:       false,
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

	if stateFilePath != "" {
		if _, err := os.Stat(stateFilePath); os.IsNotExist(err) {
			errorAndExit(fmt.Sprintf("%s does not exist", stateFilePath))
		}

		f, err := os.Open(stateFilePath)
		if err != nil {
			errorAndExit(fmt.Sprintf("failed to open state file: %v", err))
		}

		resumeState := &ghostferry.SerializableState{}
		parser := json.NewDecoder(f)
		err = parser.Decode(&resumeState)
		if err != nil {
			errorAndExit(fmt.Sprintf("failed to parse state file: %v", err))
		}

		config.StateToResumeFrom = resumeState
	}

	err = config.InitializeAndValidateConfig()
	if err != nil {
		errorAndExit(fmt.Sprintf("failed to validate config: %v", err))
	}

	ferry := copydb.NewFerry(config)

	err = ferry.Initialize()
	if err != nil {
		// This is not a good idea to reach deep within ferry from copydb. The entire ErrorHandler needs
		// refactoring which is defined in this issue - https://github.com/Shopify/ghostferry/issues/284
		ferry.Ferry.ErrorHandler.ReportError("ferry.initialize", err)
		errorAndExit(fmt.Sprintf("failed to initialize ferry: %v", err))
	}

	err = ferry.Start()
	if err != nil {
		ferry.Ferry.ErrorHandler.ReportError("ferry.start", err)
		errorAndExit(fmt.Sprintf("failed to start ferry: %v", err))
	}

	if dryrun {
		fmt.Println("exiting due to dryrun")
		return
	}

	if config.StateToResumeFrom == nil {
		err = ferry.CreateDatabasesAndTables()
		if err != nil {
			ferry.Ferry.ErrorHandler.ReportError("ferry.createDatabasesAndTables", err)
			errorAndExit(fmt.Sprintf("failed to create databases and tables: %v", err))
		}
	}

	ferry.Run()
}
