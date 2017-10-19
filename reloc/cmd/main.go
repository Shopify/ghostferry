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

var shardingKey string
var shardingValue int64
var sourceDb string
var targetDb string
var configPath string
var printVersion bool

func usage() {
	fmt.Printf("reloc built with ghostferry %s+%s\n", ghostferry.VersionNumber, ghostferry.VersionCommit)
	fmt.Println()
	fmt.Printf("Usage: %s -sharding-key <key> -sharding-value <value> -source-db <dbname> -target-db <dbname> < conf.json \n", os.Args[0])
	fmt.Printf("    or %s -sharding-key <key> -sharding-value <value> -source-db <dbname> -target-db <dbname> -config-path conf.json \n", os.Args[0])
	fmt.Println()
	flag.PrintDefaults()
}

func init() {
	flag.StringVar(&shardingKey, "sharding-key", "", "[Required] Defines the sharding key to be used for copying")
	flag.Int64Var(&shardingValue, "sharding-value", -1, "[Required] Defines the value of the sharding key to filter on")
	flag.StringVar(&sourceDb, "source-db", "", "[Required] Defines the source shard database name")
	flag.StringVar(&targetDb, "target-db", "", "[Required] Defines the target shard database name")
	flag.StringVar(&configPath, "config-path", "", "[Required] Specify path to config (or provide it on stdin)")
	flag.BoolVar(&printVersion, "version", false, "Print version and exit")
}

func main() {
	flag.Parse()

	if printVersion {
		fmt.Printf("%s+%s", ghostferry.VersionNumber, ghostferry.VersionCommit)
		os.Exit(0)
	}

	assertRequiredFlagsPresent()
	config := parseConfig()

	fmt.Printf("reloc built with ghostferry %s+%s\n", ghostferry.VersionNumber, ghostferry.VersionCommit)
	fmt.Printf("will move tenant %s=%d\n", shardingKey, shardingValue)

	ferry, err := reloc.NewFerry(shardingKey, shardingValue, sourceDb, targetDb, config)
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

func parseConfig() *ghostferry.Config {
	config := &ghostferry.Config{
		AutomaticCutover: false,
		MyServerId:       99399,
	}

	var data []byte
	var err error

	if configPath == "" {
		stat, _ := os.Stdin.Stat()
		if (stat.Mode() & os.ModeCharDevice) != 0 {
			errorAndExit("there is no config on stdin")
		}

		data, err = ioutil.ReadAll(os.Stdin)
		if err != nil {
			errorAndExit("could not read config from stdin")
		}
	} else {
		data, err = ioutil.ReadFile(configPath)
		if err != nil {
			errorAndExit(fmt.Sprintf("could not read config from file %s", configPath))
		}
	}

	err = json.Unmarshal(data, config)
	if err != nil {
		errorAndExit(fmt.Sprintf("failed to parse config: %v", err))
	}

	return config
}

func assertRequiredFlagsPresent() {
	if shardingKey == "" {
		fmt.Println("Missing sharding-key argument\n")
		usage()
		os.Exit(1)
	}

	if shardingValue == -1 {
		fmt.Println("Missing sharding-value argument\n")
		usage()
		os.Exit(1)
	}

	if sourceDb == "" {
		fmt.Println("Missing source-db argument\n")
		usage()
		os.Exit(1)
	}

	if targetDb == "" {
		fmt.Println("Missing target-db argument\n")
		usage()
		os.Exit(1)
	}
}
