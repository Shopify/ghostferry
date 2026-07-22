package main

import (
	"errors"

	tf "github.com/Shopify/ghostferry/test/lib/go/integrationferry"
)

func ddlEventHandler(schemaName, tableName string, query []byte) error {
	return errors.New("Query event")
}

func AfterInitialize(f *tf.IntegrationFerry) error {
	f.Ferry.BinlogStreamer.DDLEventHandler = ddlEventHandler
	return nil
}

func main() {
	c := tf.RunCallbacks{
		AfterInitialize: AfterInitialize,
	}
	f := tf.Setup(&c) /* pass in initializers */

	err := tf.Run(f)
	if err != nil {
		panic(err)
	}
}
