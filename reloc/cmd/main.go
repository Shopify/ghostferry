package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Shopify/ghostferry"
)

var shardingKey string
var shardingValue int64
var printVersion bool

func usage() {
	fmt.Printf("reloc built with ghostferry %s+%s\n", ghostferry.VersionNumber, ghostferry.VersionCommit)
	fmt.Printf("Usage: %s -sharding-key <key> -sharding-value <value>\n", os.Args[0])
	flag.PrintDefaults()
}

func init() {
	flag.StringVar(&shardingKey, "sharding-key", "", "[Required] Defines the sharding key to be used for copying")
	flag.Int64Var(&shardingValue, "sharding-value", -1, "[Required] Defines the value of the sharding key to filter on")
	flag.BoolVar(&printVersion, "version", false, "Print version and exit")
}

func main() {
	flag.Parse()

	if printVersion {
		fmt.Printf("%s+%s", ghostferry.VersionNumber, ghostferry.VersionCommit)
		os.Exit(0)
	}

	assertRequiredFlagsPresent()

	fmt.Printf("reloc built with ghostferry %s+%s\n", ghostferry.VersionNumber, ghostferry.VersionCommit)
}

func errorAndExit(msg string) {
	fmt.Fprintf(os.Stderr, "error: %s\n", msg)
	os.Exit(1)
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
}
