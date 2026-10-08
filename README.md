Ghostferry
==========

Ghostferry is a library that enables you to selectively copy data from one mysql instance to another with minimal amount of downtime.

It is inspired by Github's [gh-ost](https://github.com/github/gh-ost),
although instead of copying data from and to the same database, Ghostferry
copies data from one database to another and has the ability to only
partially copy data.

There is an example application called ghostferry-copydb included (under the
`copydb` directory) that demonstrates this library by copying an entire
database from one machine to another.

Talk to us on IRC at [irc.freenode.net #ghostferry](https://webchat.freenode.net/?channels=#ghostferry).

- **Tutorial and General Documentations**: https://shopify.github.io/ghostferry
- Code documentations: https://pkg.go.dev/github.com/Shopify/ghostferry
  (versioned API docs; for guides tracking `main`, the source in this
  repository is authoritative)

Overview of How it Works
------------------------

A simplified model of Ghostferry's high-level copy algorithm is written in
[TLA+](https://en.wikipedia.org/wiki/TLA%2B) under the `tlaplus` directory,
together with a TLC model configuration in `tlaplus/ghostferry.toolbox`. It is
a small finite model with explicitly stated simplifying assumptions (see the
comment at the top of `tlaplus/ghostferry.tla`); model checking it is not a
proof of correctness of the current Go implementation.

On a high-level, Ghostferry is broken into several components, enabling it to
copy data. This is documented at
https://shopify.github.io/ghostferry/main/technicaloverview.html

Documentation
-------------

The documentation is written in Markdown under `docs/` and can be read
directly on GitHub, starting at the [Documentation source](docs/index.md). The
published site is built with [Jekyll](https://jekyllrb.com/) and the
[Just the Docs](https://just-the-docs.com/) theme; its settings, page titles
and navigation order live in `docs/_config.yml`.

Internal contributors get the gems from `dev up`, then can run `dev docs`
(live preview) or `dev docs-build` (build plus link check). Otherwise:

```bash
bundle install
bundle exec jekyll build --source docs --destination build/docs
bundle exec htmlproofer build/docs --disable-external --allow-missing-href --no-enforce-https --swap-urls '^/ghostferry/main/:/'
bundle exec jekyll serve --source docs --destination build/docs --host 127.0.0.1 --port 4000
```

The build writes the site to `build/docs/`; `htmlproofer` fails on broken
internal links or anchors. The live preview is served at
http://127.0.0.1:4000/ghostferry/main/. None of these commands deploy anything.

The [Changelog](docs/changelog.md) page is populated at build time from the
root `CHANGELOG.md`, which is the only file to edit for release notes;
`docs/changelog.md` is just a landing page for readers browsing the source on
GitHub. Build and serve with the commands above. The Jekyll watcher only
watches `docs/`, so restart `dev docs` / `jekyll serve` after editing the root
`CHANGELOG.md`.

Development Setup
-----------------

### Installation

#### Prerequisites

- Go 1.26.2 (the `go` directive in `go.mod` is authoritative), Git, Make and
  a MySQL client, to build and run `ghostferry-copydb`.
- For the tests and the documentation site additionally: Ruby 3.4.8
  (`.ruby-version`), Bundler 4.0.10 (`Gemfile.lock`), a C compiler toolchain
  and the MySQL client development libraries needed to compile the `mysql2`
  gem. Run `bundle install` without excluding the test, development or docs
  groups; `test/test_helper.rb` loads `pry-byebug` from the development group
  unless `CI` is set.
- Docker (or Podman with `podman-compose`) for the local MySQL servers.

Go and Ruby versions are pinned in `.tool-versions`, which both
[mise](https://mise.jdx.dev/) and [asdf](https://asdf-vm.com/) read.

#### For Internal Contributors

`dev up`

#### For External Contributors

Install Go and Ruby with mise or asdf from the repository root:

```sh
mise install    # or: asdf install
```

Without a version manager, any Go 1.21 or newer also works: because `go.mod`
requires Go 1.26.2, the `go` command downloads and uses that toolchain itself.

Install the MySQL client and its development libraries (needed by the `mysql2`
gem), for example `brew install mysql-client` on macOS or
`apt install default-mysql-client default-libmysqlclient-dev` on Debian/Ubuntu,
then install the gems:

```sh
bundle install
```

Homebrew's `mysql-client` is keg-only; if `mysql2` cannot find it, run
`bundle config set build.mysql2 --with-mysql-config="$(brew --prefix mysql-client)/bin/mysql_config"`
first.

Start two disposable MySQL 8.0 servers from the repository root:

```sh
docker compose -f docker-compose_8.0.yml up -d mysql-1 mysql-2
# or: podman-compose -f docker-compose_8.0.yml up -d mysql-1 mysql-2
```

They listen on ports 29291 (source) and 29292 (target) with a passwordless
`root` account. They are throwaway test servers, not a template for production
credentials. Wait until both accept connections:

```sh
mysql --protocol=tcp -u root -P 29291 -e 'SELECT 1'
mysql --protocol=tcp -u root -P 29292 -e 'SELECT 1'
```

Build `ghostferry-copydb` into the first `GOPATH` entry's `bin` directory:

```sh
export GOPATH="$(go env GOPATH)"
export PATH="${GOPATH%%:*}/bin:$PATH"
make copydb
```

Run the binary from the repository root: its web UI templates are loaded from
`webui/` below `ControlServerConfig.WebBasedir`, which defaults to `.`.
Debian packages built by `make copydb-deb` instead compile in the base
directory `/usr/share/ghostferry` and install `webui/` beneath it; like `.`
for source builds, the base directory is the parent of `webui/`, not the
`webui` directory itself. Packaged builds are published on the project's
[GitHub Releases](https://github.com/Shopify/ghostferry/releases) page; most
of them are prereleases (see [Releasing new version](#releasing-new-version)).

Testing
---------------

Export `MYSQL_VERSION=8.0` when running tests against the MySQL 8.0 servers
above.

#### Run all tests

- `make test`

#### Run example copydb usage

`examples/copydb/conf.json` copies the `abc` database created by the
[copydb tutorial](docs/tutorialcopydb.md): seed the source with the tutorial's
SQL first, and make sure the target has no `abc` tables for a fresh run. Then,
from the repository root:

```sh
ghostferry-copydb -verbose examples/copydb/conf.json
```

This example uses the `Inline` verifier, binds the UI to
`127.0.0.1:8000` and adds two Custom Script buttons. It sets
`"SkipTargetVerification": true`, which disables target-write monitoring; the
tutorial intentionally keeps the protected default.

For a more detailed walkthrough, see the
[documentation](https://shopify.github.io/ghostferry).

### Ruby Integration Tests

Kindly take note of following options:

- `DEBUG=1`: To see more detailed debug output by `Ghostferry` live, as opposed
  to only when the test fails. This is helpful for debugging hanging test.

Examples:

Run all tests

`bundle exec rake test`

Run a single file

`bundle exec rake test TEST=test/integration/trivial_test.rb`

or

`bundle exec ruby -Itest test/integration/trivial_test.rb`

Run a specific test

`DEBUG=1 bundle exec ruby -Itest test/integration/trivial_test.rb -n 'TrivialIntegrationTest#test_logged_query_omits_columns'`

Releasing new version
---------------------

### Canary

Tag your commit with `canary/*` and push, i.e.

```bash
git tag --sign --message="Initial support for UUIDs as pagination keys" canary/v1.1.2-uuid-pagination-keys-alpha-1
git push origin --tags
```

This creates a GitHub prerelease named after the tag.

### Production

Every push to the `main` branch creates a GitHub **prerelease** named
`release-<first seven characters of the commit SHA>`.

Remember to update `VERSION` in `Makefile` along with the root `CHANGELOG.md`
prior to releases.
