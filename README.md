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

Talk to us on IRC at [irc.freenode.net #ghostferry](https://webchat.freenode.net/?channels=#ghostferry).

- Documentations: https://shopify.github.io/ghostferry
- Code documentations: https://godoc.org/github.com/Shopify/ghostferry

Overview of How it Works
------------------------

An overview of Ghostferry's high-level design is expressed in the TLA+
specification, under the `tlaplus` directory. It maybe good to consult with
that as it has a concise definition. However, the specification might not be
entirely correct as proofs remain elusive.

On a high-level, Ghostferry is broken into several components, enabling it to
copy data. This is detailed in docs/source/technicaloverview.rst.

A more detailed documentation is coming soon.

Development Setup
-----------------

Install:

- Have Docker installed
- Clone the repo
- `docker-compose up -d mysql-1 mysql-2`

Run tests:

- `make test`

Test copydb:

- `make copydb && ghostferry-copydb -verbose examples/copydb/conf.json`
