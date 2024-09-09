package main

import (
	"errors"
	"fmt"

	"github.com/Shopify/ghostferry"
	tf "github.com/Shopify/ghostferry/test/lib/go/integrationferry"
	"github.com/go-mysql-org/go-mysql/replication"
)

func queryEventHandler(ev *replication.BinlogEvent, query []byte, es *ghostferry.BinlogEventState) ([]byte, error) {
	fmt.Println("queryEventHandler")
	query = ev.Event.(*replication.QueryEvent).Query
	return query, errors.New("Query event")
}

func AfterInitialize(f *tf.IntegrationFerry) error {
	f.Ferry.BinlogStreamer.AddBinlogEventHandler(replication.QUERY_EVENT, queryEventHandler)
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
