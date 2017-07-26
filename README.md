Ghostferry
==========

Ghostferry is a library that enables you to selectively copy data from one mysql instance to another with minimal amount of downtime.

It is inspired by Github's [gh-ost](https://github.com/github/gh-ost),
although instead of copying data from and to the same database, Ghostferry
copies data from one database to another and have the ability to only
partially copy data.

There is an example application called ghostferry-copydb included (under the
`copydb` directory) that demonstrates this library by copying an entire
database from one machine to another.

Overview of How it Works
------------------------

Ghostferry has a bunch of components that enables it to copy data.

### BinlogStreamer ###

The `BinlogStreamer` streams the binlog of the source database. A filter can be
given to it to filter for only certain binlog events that are received.

### DataIterator ###

The DataIterator iterates through tables on the source database. It does so by
reading the primary key from the smallest entry to the maximum entry in
batches. A custom `WHERE` can be given to only select some entries from the
database for copying.

### Ferry (Overall Coordinator) ###

`Ferry` coordinates the overall run of Ghostferry. It starts and manages the
BinlogStreamer and the DataIterator. 

It also stores the current position of the run in a local database so it can
be resumed upon a restart. 

Ferry serves as a proxy to DataIterator and BinlogStreamer as one should not
directly invoke the DataIterator and BinlogStreamer.

### ControlServer ###

The ControlerServer exposes the progress of the copy and exposes methods to
throttle/pause/resume/stop the data copying and binlog streaming process. This
provides the controls necessary for a human operator to manually throttle the
copying operation. Additionally, it provides the control interface for stopping
the binlog streaming process after the final cut over steps are performed.

Development Setup
-----------------

Install:

- Have Vagrant installed
- Clone the repo
- `vagrant up`
- `vagrant ssh`
- `cd go/src/github.com/Shopify/ghostferry`
- `ci/02-setup-mysql.sh`

Run tests:

- `go test ./test`

Test copydb:

- `make && build/ghostferry-copydb -verbose examples/copydb/conf.json`
