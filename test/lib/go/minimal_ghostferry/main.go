package main

import (
	tf "github.com/Shopify/ghostferry/test/lib/go/integrationferry"
)

func main() {
	c := tf.RunCallbacks{}
	f := tf.Setup(&c) /* pass in initializers */

	err := tf.Run(f)
	if err != nil {
		panic(err)
	}
}
