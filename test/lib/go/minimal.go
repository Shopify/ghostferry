package main

import (
	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/test/lib/go/integrationferry"
	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.DebugLevel)

	config, err := integrationferry.NewStandardConfig()
	if err != nil {
		panic(err)
	}

	f := &integrationferry.IntegrationFerry{
		Ferry: &ghostferry.Ferry{
			Config: config,
		},
	}

	err = f.Main()
	if err != nil {
		panic(err)
	}
}
