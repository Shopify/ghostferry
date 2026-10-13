<a name="tutorialcopydb"></a>

# Tutorial for ghostferry-copydb

This tutorial aims to provide you with a first look on how to operate
ghostferry-copydb to copy data from one database to another so you can have
some experience with actually running Ghostferry. A production run of a data
migration will be largely similar, although you will have to consider how to
appropriately perform the cutover operations with respect to the applications
accessing the database. Recommendations on how to run copydb in production can
be found in [Running `ghostferry-copydb` in production](copydbinprod.md).

## Setup and Seed MySQL

In this tutorial, we will be using two disposable test databases that we set up
locally and we will not consider the application. You need Git, Make, a MySQL
client, Go 1.27.2 (the `go` directive in `go.mod` is authoritative) and Docker
or Podman. Clone the Ghostferry repository and start the MySQL 8.0 test
instances:

```console
$ git clone https://github.com/Shopify/ghostferry.git
$ cd ghostferry
$ docker compose -f docker-compose_8.0.yml up -d mysql-1 mysql-2
```

With Podman, use `podman-compose -f docker-compose_8.0.yml up -d mysql-1 mysql-2`
instead. These servers listen on ports 29291 and 29292 and have a `root`
account without a password. They are throwaway local test servers, not an
example of production credentials.

Without Docker or Podman, you can set up two MySQL instances on localhost ports
29291 and 29292 yourself. Both need binary logging enabled with
`binlog_format=ROW`, `binlog_row_image=FULL` and
`binlog_rows_query_log_events=ON`. Ghostferry checks the source's settings
during initialization; the target needs `binlog_rows_query_log_events=ON`
because Ghostferry monitors the target's binary log for unexpected writes (see
[TargetVerifier](verifiers.md#targetverifier)). A dry run does not check every
target setting.

Wait until both MySQL instances accept connections from the MySQL console:

```console
$ mysql --protocol=tcp -u root -P 29291 -e 'SELECT 1'
$ mysql --protocol=tcp -u root -P 29292 -e 'SELECT 1'
```

We will be moving data from the 29291 server to the 29292 server. To do this,
we must first create some test data on the 29291 server to be copied over:

```console
# export LC_CTYPE=C # Only need this if you're mac
echo "CREATE DATABASE abc;" > /tmp/n1create.sql
echo "CREATE TABLE abc.table1 (id bigint(20) AUTO_INCREMENT, data varchar(16), primary key(id));" >> /tmp/n1create.sql
echo "CREATE TABLE abc.table2 (id bigint(20) AUTO_INCREMENT, data TEXT, primary key(id));" >> /tmp/n1create.sql
for i in `seq 1 350`; do
  echo "INSERT INTO abc.table1 (id, data) VALUES (${i}, '$(cat /dev/urandom | tr -cd 'a-z0-9' | head -c 16)');" >> /tmp/n1create.sql
  echo "INSERT INTO abc.table2 (id, data) VALUES (${i}, '$(cat /dev/urandom | tr -cd 'a-z0-9' | head -c 16)');" >> /tmp/n1create.sql
done
mysql --protocol=tcp -u root -P 29291 < /tmp/n1create.sql
rm /tmp/n1create.sql
```

This created two tables under the database `abc`. We will be moving
`table1` to 29292 while not copying `abc.table2`.

## (Mirrors Production) Create Ghostferry Users

We then need to create a user with the appropriate permissions for Ghostferry
to connect with, to perform the move on both servers. For this move, we
neglect SSL connections to MySQL and thus do not require SSL for the user. In
production, you may want to enable that.

On the source server, the minimum permissions required are:

```console
mysql> CREATE USER 'ghostferry'@'%' IDENTIFIED BY 'ghostferry';
mysql> GRANT SELECT ON `abc`.* TO 'ghostferry'@'%';
mysql> GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'ghostferry'@'%';
```

The above example grants the permission to only the `abc` database. You can
grant it to more or all databases in your production environment as needed.

On the target server, the minimum permissions required are:

```console
mysql> CREATE USER 'ghostferry'@'%' IDENTIFIED BY 'ghostferry';
mysql> GRANT INSERT, UPDATE, DELETE, CREATE, SELECT ON *.* TO 'ghostferry'@'%';
mysql> GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'ghostferry'@'%';
```

We grant permission to all databases because we assume that the `abc`
database does not exist on the target and Ghostferry will create it
automatically. The replication privileges on the target are needed because, by
default, Ghostferry streams the target's binary log to detect writes to the
target that it did not make itself.

## (Mirrors Production) Install ghostferry-copydb

We then need to obtain the ghostferry-copydb binary on the server on which we
want to execute Ghostferry on. Note that all the data moved will go through
this server over its network so make sure the production server is
appropriately picked. For the present tutorial, Ghostferry will simply live on
the same machine.

Build ghostferry-copydb from the repository root:

```console
$ export GOPATH="$(go env GOPATH)"
$ export PATH="${GOPATH%%:*}/bin:$PATH"
$ make copydb
```

This installs `ghostferry-copydb` into the `bin` directory of the first
`GOPATH` entry. Run it from the repository root throughout this tutorial: the
web UI templates are loaded from `webui/` below
`ControlServerConfig.WebBasedir`, which defaults to the current directory.
Debian packages instead compile in the base directory `/usr/share/ghostferry`
and install `webui/` beneath it (the base directory is always the parent of
`webui/`). Packaged builds are published on the project's
[GitHub Releases](https://github.com/Shopify/ghostferry/releases) page; most of
them are prereleases built from `main` or canary tags.

## (Mirrors Production) Setup Ghostferry Run Configuration

We will need to provide ghostferry-copydb with a configuration file such that
it knows how to connect to the databases and what to copy. This is a json file
which should look like the following:

```json
{
  "Source": {
    "Host": "127.0.0.1",
    "Port": 29291,
    "User": "ghostferry",
    "Pass": "ghostferry",
    "Collation": "utf8mb4_unicode_ci",
    "Params": {
      "charset": "utf8mb4"
    }
  },

  "Target": {
    "Host": "127.0.0.1",
    "Port": 29292,
    "User": "ghostferry",
    "Pass": "ghostferry",
    "Collation": "utf8mb4_unicode_ci",
    "Params": {
      "charset": "utf8mb4"
    }
  },

  "Databases": {
    "Whitelist": ["abc"]
  },

  "Tables": {
    "Blacklist": ["table2"]
  },

  "VerifierType": "ChecksumTable",

  "ControlServerConfig": {
    "ServerBindAddr": "127.0.0.1:8000"
  }
}
```

Save this file to a file called `examplerun.json` in the repository root.

Note that in the example above, the Collation and charsets are set. If you
setup your own MySQL instances, you might need to change these values.  We are
also using the `Whitelist` and `Blacklist` to ensure that we only copy
`abc.table1` from the source to the target. For more information about this
configuration file, see [Running `ghostferry-copydb` in production](copydbinprod.md).

`ControlServerConfig.ServerBindAddr` limits the web UI to the local machine.
When it is not configured, the UI listens on `0.0.0.0:8000`, which is reachable
from other hosts on the network.

Lastly, we have selected a data verifier to be available to use during the
run. Specifically, we selected the ChecksumTable verifier as the amount of data
copied will be small. Independently of that choice, Ghostferry monitors the
target for unexpected writes by default. For more information about the
verifiers, see [Verifiers](verifiers.md).

## (Mirrors Production) Validate Ghostferry Configuration

Before actually running Ghostferry, it is good practise to validate the
configuration you specified. ghostferry-copydb has a dryrun flag that will try
to use the configuration you have to connect to the database. It will also scan
the tables according to the black/whitelist specified and print it out in the
debug logs:

```console
$ ghostferry-copydb -dryrun -verbose examplerun.json
```

The verbose flag gives slightly more debug information in case there are any
issues. The exact log wording and fields depend on the configured logging
backend, but a successful dry run of this tutorial shows:

- a `table schemas cached` log entry listing only `abc.table1`;
- binlog streaming starting for both the source and the target connection;
- `exiting due to dryrun` as the last line on stdout.

No tables or rows are copied during a dry run. If a table you want to move is
not in the cached list, the whitelist/blacklist configuration is incorrect.

## (Mirrors Production) Starting Ghostferry Run

To start the ghostferry run, simply perform the same command as before except
without the dryrun flag. You can also turn off the verbose flag, although it
may be good practise to leave it on and write the logs to a file so the move
can be audited at a later time. Run it in its own terminal and use other
terminals for the MySQL and web UI steps below:

```console
$ ghostferry-copydb -verbose examplerun.json >examplerun.log 2>&1
```

This command merges stdout and stderr into one log file. If you want to be able
to resume an interrupted run, stdout must instead be captured separately from
the logs, as described in
[Interrupt and resuming `ghostferry-copydb`](copydbinterruptresume.md).

To confirm that Ghostferry indeed copies changes to the source table, we can
manually insert a row into `abc.table1` during the run, while the UI shows it
waiting for cutover:

```console
$ mysql --protocol=tcp -u root -P 29291
mysql> INSERT INTO abc.table1 (id, data) VALUES (351, "helloworld");
```

## (Mirrors Production) Monitoring Ghostferry Run via Web UI

Once the run starts, the built-in web server listens on the configured
`ControlServerConfig.ServerBindAddr`. Browse to <http://127.0.0.1:8000> to view
it; there you should find controls to:

- Pause/Unpause: pauses/resumes table iteration and the application of binlog
  events to the target. It does not stop writes to the source, and it does not
  immediately stop every binlog streamer.
- Allow Automatic Cutover: lets ghostferry-copydb proceed with cutover once
  the row copy is complete and the binlog streamer has nearly caught up. It does
  not stop application writes itself: you must stop writes to the source before
  pressing it. ghostferry-copydb then records the source's current binlog
  position, applies the remaining events up to it and stops streaming. The
  configuration fields `CutoverLock` and `CutoverUnlock` can name HTTP
  callbacks that copydb calls at the start and at the end of this procedure, and
  `ControlServerConfig.CustomScripts` adds separate buttons that run scripts on
  demand. `CutoverUnlock` is called before any operator-triggered final
  verification, so it does not mean that the data has been verified.
- Run Verification: shown when the run is neither starting nor copying and no
  verification is in progress. It runs the ChecksumTable verifier we specified
  earlier to compare the copied tables on the source and target. Only run it
  once the source binlog streaming has finished (after cutover), while the
  source is still read only and before anything else writes to the target.
  (The Inline verifier's final verification can only be run once and rejects
  any source binlog events arriving after it started.)

While the run is not done, the page refreshes itself every 60 seconds. Once the
run is done it no longer refreshes; use the Manual Refresh link to see
verification progress.

For this tutorial, the run should be very short so thus you might miss most of
the copying states. Take a look around and refresh a couple times to get
familiar with the UI.

## (Mirrors Production) Perform Cutover

In the default configuration, cutover is triggered manually. During cutover,
you must stop writes to the data on the source database: the application must
stop its writers and let in-flight transactions finish. For the purpose of this
tutorial, we lock the source and set it to read only. Even though we have no
applications writing to the source in this case, let's do it anyway so we get
into the habit of thinking of this step.

Open a dedicated interactive session and keep it open until the final
verification below has succeeded; the lock is released when the session ends:

```console
$ mysql --protocol=tcp -u root -P 29291
mysql> FLUSH TABLES WITH READ LOCK;
mysql> SET GLOBAL read_only = ON;
```

See the [MySQL `FLUSH TABLES WITH READ LOCK`
documentation](https://dev.mysql.com/doc/refman/8.0/en/flush.html#flush-tables-with-read-lock)
for what the lock does and does not block. Ghostferry does not need a
`FLUSH BINARY LOGS`: that statement only rotates the binary log. When cutover
starts, Ghostferry reads the source's current binlog position and applies all
events up to it. In production, account for privileged writers, replication
and the application's own write paths rather than relying on this tutorial's
procedure alone.

If you run Ghostferry from a source that is a replica, you need to set
`RunFerryFromReplica` together with `SourceReplicationMaster` and
`ReplicatedMasterPositionQuery` in the config json. See
[Running `ghostferry-copydb` in production](copydbinprod.md) and
<https://pkg.go.dev/github.com/Shopify/ghostferry/copydb#Config> for more
details (the API documentation is versioned; the source in this repository is
authoritative for `main`).

We can then go back to the web ui and click the Allow Automatic Cutover button.
In a second or two the ghostferry binlog streaming process should stop. Refresh
the page until you see the state to be done. Done means that copying and
binlog streaming have completed; it does not mean that the final verification
has passed. The process and its web UI keep running.

## (Mirrors Production) Verify Source and Target Data are Identical

ghostferry-copydb does not run the final verification automatically. With the
source still locked, click the Run Verification button in the web ui to perform
the verification in the background. Use Manual Refresh until Verified Correct
shows `true` and no error is reported. Only after that should applications be
allowed to write to the target. Verified Correct covers the tables and columns
the selected verifier compares; see [Verifiers](verifiers.md) for its limits.

Additionally, since we manually inserted a row earlier, we should be able to
find it via:

```console
$ mysql --protocol=tcp -u root -P 29292
mysql> SELECT * FROM abc.table1 WHERE id = 351;
```

## Finishing Ghostferry Run and Next Steps

At this point, the data on the source and target are verified identical and
Ghostferry will no longer propagate data from 29291 to 29292. In a production
situation, you can now notify all applications using the source database to use
the target database.

Because the servers are disposable local fixtures, restore the source in the
session that still holds the lock:

```console
mysql> UNLOCK TABLES;
mysql> SET GLOBAL read_only = OFF;
```

Do not copy this step into a production cutover: there, the old source should
stay closed to application writes once they have moved to the target.

The control server UI will stay up indefinitely. To stop it, simply press
CTRL+C to interrupt the ghostferry-copydb process.

To run Ghostferry in production, you should read through
[Running `ghostferry-copydb` in production](copydbinprod.md).
If you need to interrupt and resume Ghostferry, you should also read through
[Interrupt and resuming `ghostferry-copydb`](copydbinterruptresume.md).
