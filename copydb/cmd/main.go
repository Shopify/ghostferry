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
	fmt.Printf("Usage: %s [OPTIONS] path/to/config/file.json\n", os.Args[0])
	flag.PrintDefaults()
}

var verbose bool

func init() {
	flag.BoolVar(&verbose, "verbose", false, "Show verbose logging output")
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
	config := &ghostferry.Config{
		SourceHost: "",
		SourcePort: 3306,
		SourceUser: "ghostferry",
		SourcePass: "",

		TargetHost: "",
		TargetPort: 3306,
		TargetUser: "ghostferry",
		TargetPass: "",

		MyServerId:       99399,
		AutomaticCutover: false,
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

	err = config.ValidateConfig()
	if err != nil {
		errorAndExit(fmt.Sprintf("failed to validate config: %v", err))
	}

	if len(config.ApplicableDatabases) == 0 {
		errorAndExit("failed to validate config: no applicable databases specified")
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

	ferry.Run()
}
