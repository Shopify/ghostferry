// Copyright 2013 Ooyala, Inc.

package dogstatsd

import (
	"log"
	"net"
	"reflect"
	"testing"
	"time"
)

var dogstatsdTests = []struct {
	GlobalNamespace string
	GlobalTags      []string
	Method          string
	Metric          string
	Value           interface{}
	Tags            []string
	Rate            float64
	Expected        string
}{
	{"", nil, "Gauge", "test.gauge", 1.0, nil, 1.0, "test.gauge:1|g"},
	{"", nil, "Gauge", "test.gauge", 1.0, nil, 0.999999, "test.gauge:1|g|@0.999999"},
	{"", nil, "Gauge", "test.gauge", 1.0, []string{"tagA"}, 1.0, "test.gauge:1|g|#tagA"},
	{"", nil, "Gauge", "test.gauge", 1.0, []string{"tagA", "tagB"}, 1.0, "test.gauge:1|g|#tagA,tagB"},
	{"", nil, "Gauge", "test.gauge", 1.0, []string{"tagA"}, 0.999999, "test.gauge:1|g|@0.999999|#tagA"},
	{"", nil, "Count", "test.count", int64(1), []string{"tagA"}, 1.0, "test.count:1|c|#tagA"},
	{"", nil, "Count", "test.count", int64(-1), []string{"tagA"}, 1.0, "test.count:-1|c|#tagA"},
	{"", nil, "Histogram", "test.histogram", 2.3, []string{"tagA"}, 1.0, "test.histogram:2.3|h|#tagA"},
	{"", nil, "Set", "test.set", "uuid", []string{"tagA"}, 1.0, "test.set:uuid|s|#tagA"},
	{"", nil, "Timer", "test.timer", 44876 * time.Microsecond, []string{"tagA"}, 1.0, "test.timer:44.876|ms|#tagA"},
	{"flubber.", nil, "Set", "test.set", "uuid", []string{"tagA"}, 1.0, "flubber.test.set:uuid|s|#tagA"},
	{"", []string{"tagC"}, "Set", "test.set", "uuid", []string{"tagA"}, 1.0, "test.set:uuid|s|#tagC,tagA"},
}

func TestClient(t *testing.T) {
	addr := "localhost:1201"
	server := newServer(t, addr)
	defer server.Close()

	mainClient, err := New(addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mainClient.Close()

	for _, tt := range dogstatsdTests {
		client := Clone(mainClient, &Context{Namespace: tt.GlobalNamespace, Tags: tt.GlobalTags})

		method := reflect.ValueOf(client).MethodByName(tt.Method)
		e := method.Call([]reflect.Value{
			reflect.ValueOf(tt.Metric),
			reflect.ValueOf(tt.Value),
			reflect.ValueOf(tt.Tags),
			reflect.ValueOf(tt.Rate)})[0]
		errInter := e.Interface()
		if errInter != nil {
			t.Fatal(errInter.(error))
		}

		message := serverRead(t, server)
		if message != tt.Expected {
			t.Errorf("Expected: %s. Actual: %s", tt.Expected, message)
		}
	}
}

func TestCloneConcurrently(t *testing.T) {
	addr := "localhost:1201"
	server := newServer(t, addr)
	defer server.Close()

	context := &Context{Tags: []string{"global"}}
	client, err := New(addr, context)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		contextA := context.Clone()
		contextA.Namespace = "a."
		contextA.Tags = append(contextA.Tags, "a")

		clonedClient := Clone(client, contextA)
		clonedClient.Count("a", 1, nil, 1.0)
	}()

	go func() {
		contextB := context.Clone()
		contextB.Namespace = "b."
		contextB.Tags = append(contextB.Tags, "b")

		go func() {
			contextC := contextB.Clone()
			contextC.Namespace = "c."
			contextC.Tags = append(contextC.Tags, "c")

			clonedClient := Clone(client, contextC)
			clonedClient.Count("c", 1, nil, 1.0)
		}()

		clonedClient := Clone(client, contextB)
		clonedClient.Count("b", 1, nil, 1.0)
	}()

	client.Count("master", 1, nil, 1.0)

	messages := make([]string, 0, 4)
	messages = append(messages, serverRead(t, server))
	messages = append(messages, serverRead(t, server))
	messages = append(messages, serverRead(t, server))
	messages = append(messages, serverRead(t, server))

	assertMessageReceived := func(messages []string, message string) {
		for _, msg := range messages {
			if msg == message {
				return
			}
		}
		t.Errorf("Did not receive message: %s", message)
	}

	assertMessageReceived(messages, "master:1|c|#global")
	assertMessageReceived(messages, "a.a:1|c|#global,a")
	assertMessageReceived(messages, "b.b:1|c|#global,b")
	assertMessageReceived(messages, "c.c:1|c|#global,b,c")
}

func TestEvent(t *testing.T) {
	addr := "localhost:1201"
	server := newServer(t, addr)
	defer server.Close()
	client := newClient(t, addr)

	err := client.Event("title", "text", []string{"tag1", "tag2"})
	if err != nil {
		t.Fatal(err)
	}

	message := serverRead(t, server)
	expected := "_e{5,4}:title|text|#tag1,tag2"
	if message != expected {
		t.Errorf("Expected: %s. Actual: %s", expected, message)
	}
}

func serverRead(t *testing.T, server *net.UDPConn) string {
	bytes := make([]byte, 1024)
	n, _, err := server.ReadFrom(bytes)
	if err != nil {
		t.Fatal(err)
	}
	return string(bytes[:n])
}

func newClient(t *testing.T, addr string) *Client {
	client, err := New(addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newServer(t *testing.T, addr string) *net.UDPConn {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}

	server, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	return server
}

// This example shows the basic usage of the library.
func Example() {
	// Create the client with a given context.
	c, err := New("127.0.0.1:8125", &Context{
		Namespace: "flubber.",                  // Prefix for every metric name
		Tags:      []string{"zone:us-east-1a"}, // Apended to every metric
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Use the client to send metrics of different kinds.
	err = c.Timer("request.duration", 2*time.Second, nil, 1)
	err = c.Count("mysql.queries", 1, nil, 0.01)
	err = c.Set("unique_logins", "UUIDv4", nil, 1.0)
	err = c.Gauge("queue_size", 213, nil, 1)
	err = c.Histogram("message_size", 345, nil, 0.01)
	err = c.Event("Final event", "This is the final event we are sending", []string{"final:true"})
}

// This example shows how to use clone to create multiplke clients
// with different contexts, while reusing the same UDP socket.
// This allows you to construct a tree of clients for different contexts,
// all reusing the same connection. The connection will only be closed
// by calling Close on the root client that was created using New; the
// connection will be left alone for any other client that gets closed.
// By using Context Clone to cerate a new context from an existing one,
// it is thread-safe to use.
func ExampleClone() {
	context := &Context{Tags: []string{"global"}}

	c1, err := New("127.0.0.1:8125", context)
	if err != nil {
		log.Fatal(err)
	}
	defer c1.Close()

	go func() {
		newContext := context.Clone()
		newContext.Tags = append(newContext.Tags, "context:a")
		c2 := Clone(c1, newContext)
		defer c2.Close()

		err = c2.Count("mysql.queries", 1, []string{"call:1"}, 0.01) // Tags: global,context:a,call:1
	}()

	go func() {
		newContext := context.Clone()
		newContext.Tags = append(newContext.Tags, "context:b")
		c3 := Clone(c1, newContext)
		defer c3.Close()

		err = c3.Count("mysql.queries", 1, []string{"call:2"}, 0.01) // Tags: global,context:b,call:2
	}()

	err = c1.Count("mysql.queries", 1, []string{"call:master"}, 0.01) // Tags: global,call:master
}
