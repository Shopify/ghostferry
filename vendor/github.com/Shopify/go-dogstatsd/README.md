## Overview

Package dogstatsd provides a Go DogStatsD client. DogStatsD extends StatsD, adding tags,
events, sets, and histograms. The documentation for DogStatsD is here:
http://docs.datadoghq.com/guides/dogstatsd/

## Get the code

    $ go get github.com/Shopify/go-dogstatsd

## Usage

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

API documentation can be find on [godoc.org](http://godoc.org/github.com/Shopify/go-dogstatsd)

## Development

Run the tests with:

    $ go test -race ./...

## License

go-dogstatsd is released under the [MIT license](http://www.opensource.org/licenses/mit-license.php).
