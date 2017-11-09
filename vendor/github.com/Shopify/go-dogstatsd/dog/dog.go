package dog

import (
	"errors"
	"github.com/Shopify/go-dogstatsd"
	"time"
)

var client *dogstatsd.Client

// Configure instantiates the global client.
func Configure(addr string, namespace string, tags []string) (err error) {
	if client != nil {
		client.Close()
	}

	context := &dogstatsd.Context{Namespace: namespace, Tags: tags}
	if client, err = dogstatsd.New(addr, context); err != nil {
		return
	}
	return
}

var ErrUncontextured = errors.New("connection has not been contextured; call dog.Configure()")

func Event(title string, text string, tags []string) error {
	if client == nil {
		return ErrUncontextured
	}
	return client.Event(title, text, tags)
}

func Gauge(name string, value float64, tags []string, rate float64) error {
	if client == nil {
		return ErrUncontextured
	}
	return client.Gauge(name, value, tags, rate)
}

func Count(name string, value int64, tags []string, rate float64) error {
	if client == nil {
		return ErrUncontextured
	}
	return client.Count(name, value, tags, rate)
}

func Histogram(name string, value float64, tags []string, rate float64) error {
	if client == nil {
		return ErrUncontextured
	}
	return client.Histogram(name, value, tags, rate)
}

func Timer(name string, duration time.Duration, tags []string, rate float64) error {
	if client == nil {
		return ErrUncontextured
	}
	return client.Timer(name, duration, tags, rate)
}

func Set(name string, value string, tags []string, rate float64) error {
	if client == nil {
		return ErrUncontextured
	}
	return client.Set(name, value, tags, rate)
}
