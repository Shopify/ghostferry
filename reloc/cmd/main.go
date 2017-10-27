package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/reloc"
)

var configPath string
var printVersion bool

func usage() {
	fmt.Printf("reloc built with ghostferry %s+%s\n", ghostferry.VersionNumber, ghostferry.VersionCommit)
	fmt.Println()
	fmt.Printf("Usage: %s < conf.json \n", os.Args[0])
	fmt.Printf("    or %s -config-path conf.json \n", os.Args[0])
	fmt.Println()
	flag.PrintDefaults()
}

func init() {
	flag.StringVar(&configPath, "config-path", "", "Specify path to config (or provide it on stdin)")
	flag.BoolVar(&printVersion, "version", false, "Print version and exit")
}

func main() {
	flag.Parse()

	if printVersion {
		fmt.Printf("%s+%s", ghostferry.VersionNumber, ghostferry.VersionCommit)
		os.Exit(0)
	}

	config := parseConfig()

	fmt.Printf("reloc built with ghostferry %s+%s\n", ghostferry.VersionNumber, ghostferry.VersionCommit)
	fmt.Printf("will move tenant %s=%d\n", config.ShardingKey, config.ShardingValue)

	err := reloc.InitializeMetrics("reloc", config.StatsDAddress)
	if err != nil {
		errorAndExit(fmt.Sprintf("failed to initialize metrics: %v", err))
	}

	ferry, err := reloc.NewFerry(config)
	if err != nil {
		errorAndExit(fmt.Sprintf("failed to create ferry: %v", err))
	}

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

func errorAndExit(msg string) {
	fmt.Fprintf(os.Stderr, "error: %s\n", msg)
	os.Exit(1)
}

func parseConfig() *reloc.Config {
	config := &reloc.Config{
		Config: ghostferry.Config{AutomaticCutover: true},

		ShardingValue: -1,
	}

	var data []byte
	var err error

	if configPath == "" {
		stat, _ := os.Stdin.Stat()
		if (stat.Mode() & os.ModeCharDevice) != 0 {
			usage()
			errorAndExit("there is no config on stdin")
		}

		data, err = ioutil.ReadAll(os.Stdin)
		if err != nil {
			usage()
			errorAndExit("could not read config from stdin")
		}
	} else {
		data, err = ioutil.ReadFile(configPath)
		if err != nil {
			usage()
			errorAndExit(fmt.Sprintf("could not read config from file %s", configPath))
		}
	}

	err = json.Unmarshal(data, config)
	if err != nil {
		errorAndExit(fmt.Sprintf("failed to parse config: %v", err))
	}

	if config.MyServerId != 0 {
		errorAndExit("specifying MyServerId option manually is dangerous and disallowed")
	}

	if config.ShardingKey == "" {
		errorAndExit("missing ShardingKey config")
	}

	if config.ShardingValue == -1 {
		errorAndExit("missing ShardingValue config")
	}

	if config.SourceDB == "" {
		errorAndExit("missing SourceDB config")
	}

	if config.TargetDB == "" {
		errorAndExit("missing TargetDB config")
	}

	if config.StatsDAddress == "" {
		config.StatsDAddress = "127.0.0.1:8125"
	}

	return config
}
