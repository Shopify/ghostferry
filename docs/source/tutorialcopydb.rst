.. _tutorialcopydb:

==============================
Tutorial for ghostferry-copydb
==============================

This tutorial aims to provide you with a first look on how to operate
ghostferry-copydb to copy data from one database to another so you can have
some experience with actually running Ghostferry. A production run of a data
migration will be largely similar, although you will have to consider how to
appropriately perform the cutover operations with respect to the applications
accessing the database. Recommendations on how to run copydb in production can
be found in :ref:`copydbinprod`.

Setup and Seed MySQL
--------------------

In this tutorial, we will be using two test databases that we setup locally and
we will not consider the application. With git, clone the Ghostferry repository
and create the test MySQL instances:

.. code-block:: shell-session

  $ git clone https://github.com/Shopify/ghostferry.git
  $ cd ghostferry
  $ docker-compose up -d mysql-1 mysql-2

Users without docker-compose can either install it on their machine or manually
setup two localhost MySQL instances available at port 29291 and 29292 with FULL
image row based replication.

Confirm that you can access both MySQL instances with the MySQL console:

.. code-block:: shell-session

  # mysql --protocol=tcp -u root -P 29291
  # mysql --protocol=tcp -u root -P 29292

We will be moving data from the 29291 server to the 29292 server. To do this,
we must first create some test data on the 29291 server to be copied over:

.. code-block:: shell-session

  # echo "CREATE DATABASE abc;" >> /tmp/n1create.sql
  # echo "CREATE TABLE abc.table1 (id bigint(20) AUTO_INCREMENT, data varchar(16), primary key(id));" >> /tmp/n1create.sql
  # echo "CREATE TABLE abc.table2 (id bigint(20) AUTO_INCREMENT, data TEXT, primary key(id));" >> /tmp/n1create.sql
  # for i in `seq 1 350`; do
      echo "INSERT INTO abc.table1 (id, data) VALUES (${i}, '$(cat /dev/urandom | tr -cd 'a-z0-9' | head -c 16)');" >> /tmp/n1create.sql
      echo "INSERT INTO abc.table2 (id, data) VALUES (${i}, '$(cat /dev/urandom | tr -cd 'a-z0-9' | head -c 16)');" >> /tmp/n1create.sql
    done
  # mysql --protocol=tcp -u root -P 29291 < /tmp/n1create.sql
  # rm /tmp/n1create.sql

This created two tables under the database ``abc``. We will be moving
``table1`` to 29292 while not copying 29291.

(Mirrors Production) Create Ghostferry Users
--------------------------------------------

We then need to create an user with the appropriate permissions for Ghostferry
to connect with to perform the move with on both server. For this move, we
neglect SSL connections to MySQL and thus do not require SSL for the user. In
production, you may want to enable that.

On the source server, the minimum permissions required are:

.. code-block:: shell-session

  mysql> CREATE USER 'ghostferry'@'%' IDENTIFIED BY 'ghostferry';
  mysql> GRANT SELECT ON `abc`.* TO 'ghostferry'@'%';
  mysql> GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'ghostferry'@'%';

The above example grants the permission to only the ``abc`` database. You can
grant it to more or all databases in your production environment as needed.

On the target server, the minimum permissions required are:

.. code-block:: shell-session

  mysql> CREATE USER 'ghostferry'@'%' IDENTIFIED BY 'ghostferry';
  mysql> GRANT INSERT, UPDATE, DELETE, CREATE, SELECT ON *.* TO 'ghostferry'@'%';

We grant permission to all databases because we assume that the ``abc``
database does not exist on the target and Ghostferry will create it
automatically.

(Mirrors Production) Install ghostferry-copydb
----------------------------------------------

We then need to obtain the ghostferry-copydb binary on the server on which we
want to execute Ghostferry on. Note that all the data moved will go through
this server over its network so make sure the production server is
appropriately picked. For the present tutorial, Ghostferry will simply live on
the same machine.

To download the latest binaries, you currently have to compile copydb with
Go 1.9 via ``make copydb`` after cloning the repository.

For testing purposes, you can also use `this unofficial PPA
<https://launchpad.net/~shuhao/+archive/ubuntu/ghostferry-unofficial>`_ (see
`this PR <https://github.com/Shopify/ghostferry/pull/15>`_ as well) to obtain a
version of ghostferry-copydb. Note the unofficial PPA for ghsotferry-copydb is
not supported and you should not use it in production.

(Mirrors Production) Setup Ghostferry Run Configuration
-------------------------------------------------------

We will need to provide ghostferry-copydb with a configuration file such that
it knows how to connect to the databases and what to copy. This is a json file
which should look like the following:

.. code-block:: json

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

    "VerifierType": "ChecksumTable"
  }

Save this file to a file called ``examplerun.json``.

Note that in the example above, the Collation and charsets are set. If you
setup your own MySQL instances, you might need to change these values.  We are
also using the ``Whitelist`` and ``Blacklist`` to ensure that we only copy
``abc.table1`` from the source to the target. For more information about this
configuration file, see :ref:`copydbinprod`.

Lastly, we have enabled verification to be available to use during the run.
Specifically, we enabled the ChecksumTable verifier as the amount of data
copied will be small. For more information about the verifiers, see
:ref:`verifiers`.

(Mirrors Production) Validate Ghostferry Configuration
------------------------------------------------------

Before actually running Ghostferry, it is good practise to validate the
configuration you specified. ghostferry-copydb has a dryrun flag that will try
to use the configuration you have to connect to the database. It will also scan
the tables according to the black/whitelist specified and print it out in the
debug logs:

.. code-block:: shell-session

  $ ghostferry-copydb -dryrun -verbose examplerun.json

The verbose flag gives slightly more debug information in case there are any
issues. In this case, there should not be any issues as we setup the database
according to the tutorial and the output should be something like this
(simplified for readibility in the tutorial):

.. code-block:: text

  [...]
  INFO[0000] connecting to the source database             dsn="ghostferry:<masked>@[...]" tag=ferry
  INFO[0000] connecting to the target database             dsn="ghostferry:<masked>@[...]" tag=ferry
  [...]
  INFO[0000] found binlog position, starting synchronization  file=[...] pos=[...] tag=binlog_streamer
  [...]
  DEBU[0000] loading tables from database                  database=abc tag=table_schema_cache
  DEBU[0000] fetching table schema                         database=abc table=table1 tag=table_schema_cache
  DEBU[0000] fetching table schema                         database=abc table=table2 tag=table_schema_cache
  DEBU[0000] caching table schema                          database=abc table=table1 tag=table_schema_cache
  INFO[0000] table schemas cached                          tables="[abc.table1]" tag=table_schema_cache
  exiting due to dryrun

Note the last INFO line shows which tables will be moved as we cache their
schemas in the memory. If there is a table you want to move and it does not
show up there, it means the whitelist/blacklist configuration is incorrect.

(Mirrors Production) Starting Ghostferry Run
--------------------------------------------

To start the ghostferry run, simply perform the same command as before except
without the dryrun flag. You can also turn off the verbose flag, although it
may be good practise to leave it on and redirect stdout to a file so the move
can be audited at a later time. We will do this here for good practise:

.. code-block:: shell-session

  $ ghostferry-copydb -verbose examplerun.json 2&>examplerun.log

To confirm that Ghostferry indeed copies changes to the source table, we can
manually insert a row into ``abc.table1`` during the run

.. code-block:: shell-session

  # mysql --protocol=tcp -u root -P 29291
  mysql> INSERT INTO abc.table1 (id, data) VALUES (351, "helloworld");

(Mirrors Production) Monitoring Ghostferry Run via Web UI
---------------------------------------------------------

Once the run starts, a built-in webserver is started at port 8000 by default.
This can be changed in the configuration json. Simply browse to
http://localhost:8000 to view this server and in there you should find controls
to:

- Pause/Unpause: allows you to pause/unpause the data copy and binlog streaming
  process.
- Allow automatic cutover: You should only press this button after you set the
  source database to read only. In its current implementation, it will simply
  allow ghostferry-copydb to finish all its processes in a correct manner,
  assuming that there are no more writes to the source database and all pending
  writes have been flushed to the binlog. In a future implementation, we may
  allow external scripts (configured via the json configuration) to be
  automatically executed with the push of this button so you can perform
  operations you need to perform during cutover.
- Run Verification: This button is only available during the Wait-For-Cutover
  and Done phase of the move. It will run the ChecksumTable verifier we
  specified earlier ensure the data are identical on the source and target. You
  should only run this while the source is read only and when the target is not
  yet written to.

The page will refresh itself every 60 seconds.

For this tutorial, the run should be very short so thus you might miss most of
the copying states. Take a look around and refresh a couple times to get
familiar with the UI.

(Mirrors Production) Perform Cutover
------------------------------------

In the default configuration, cutover is triggered manually. During cutover,
you must stop writes to the data on the source database. For the purpose of
this tutorial, we will set the source database to read only. Even though we
have no applications writing to the source in this case, let's do it anyway so
we get into the habit of thinking of this step:

.. code-block:: shell-session

  # mysql --protocol=tcp -u root -P 29291
  mysql> FLUSH TABLES WITH READ LOCK; -- Ensure all writes are done
  mysql> SET GLOBAL read_only = ON;   -- Sets the database to read only
  mysql> FLUSH BINARY LOGS            -- Ensure all writes are record in binlog

The last step ``FLUSH BINARY LOGS`` is not necessarily required if you run your
MySQL server with ``sync_binlog=1``. If you're running Ghostferry from a source
that is a replica, you need to also use turn option ``RunFerryFromReplica`` on
in the config json as well as other options. See
`<https://godoc.org/github.com/Shopify/ghostferry/copydb#Config>`_ for more
details.

We can then go back to the web ui and click the Allow Automatic Cutover button.
In a second or two the ghostferry binlog streaming process should stop. Refresh
the page until you see the state to be DONE.

(Mirrors Production) Verify Source and Target Data are Identical
----------------------------------------------------------------

At this point, the data on the source and target should be identical. To
confirm this is the case, click the Run Verification button in the web ui to
perform the verification in the background. Refresh the page a couple of times
until it tells you the verification was successful.

Additionally, since we manually inserted a row earlier, we should be able to
find it via:

.. code-block:: shell-session

  # mysql --protocol=tcp -u root -P 29292
  mysql> SELECT * FROM abc.table1 WHERE id = 351;

Finishing Ghostferry Run and Next Steps
---------------------------------------

At this point, the data on the source and target are verified identical and
Ghostferry will no longer propagate data from 29291 to 29292. In a production
situation, you can now notify all applications using the source database to use
the target database.

The control server UI will stay up indefinitely, to stop it, simply press
CTRL+C to interrupt the ghostferry-copydb process.
