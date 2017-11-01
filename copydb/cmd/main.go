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

func init() {
	flag.BoolVar(&verbose, "verbose", false, "Show verbose logging output")
	flag.BoolVar(&dryrun, "dryrun", false, "Do not actually perform the move, just connect and check settings")
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
		Config: ghostferry.Config{
			Source: ghostferry.DatabaseConfig{
				Port: 3306,
				User: "ghostferry",
			},

			Target: ghostferry.DatabaseConfig{
				Port: 3306,
				User: "ghostferry",
			},

			MyServerId:       99399,
			AutomaticCutover: false,
		},
	}

	// Open and parse configurations
	f, err := os.Open(configFilePath)
	if err != nil {
		errorAndExit(fmt.Sprintf("failed to open file: %v", err))
	}

	parser := json.NewDecoder(f)
	err = parser.Decode(&config)
	if err != nil {
		errorAndExit(fmt.Sprintf("failed to parse config file: %v", err))
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

	err = ferry.CreateDatabasesAndTables()
	if err != nil {
		errorAndExit(fmt.Sprintf("failed to create databases and tables: %v", err))
	}

	ferry.Run()
}
