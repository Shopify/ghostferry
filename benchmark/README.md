Ghostferry benchmark setup
==========================

A benchmark "library" is provided with `./ghostferry_benchmark.rb`. An example
benchmark case can be seen with `./studies/batch_size.rb`, which measures the
copy speed for several row sizes and batch sizes.

Some modifications are needed to make this benchmark work for other studies.

Prerequisites
-------------

Follow the Development Setup in the repository's [README](../README.md):

- Start the local MySQL 8.0 servers from the repository root:

  ```sh
  docker compose -f docker-compose_8.0.yml up -d mysql-1 mysql-2
  # or: podman-compose -f docker-compose_8.0.yml up -d mysql-1 mysql-2
  ```

  The harness connects to them as passwordless `root` on ports 29291 (source)
  and 29292 (target).

  **Warning:** the harness drops and recreates the `benchmark` schema on the
  source and drops it on the target. Only point it at disposable servers.

- Build `ghostferry-copydb` and put it on your `PATH`; the harness runs the
  `ghostferry-copydb` found there:

  ```sh
  export GOPATH="$(go env GOPATH)"
  export PATH="${GOPATH%%:*}/bin:$PATH"
  make copydb
  ```

- Install the gems with `bundle install`, including the test and development
  groups of the root `Gemfile` (`mysql2`, `webrick`, `tqdm`).

- Ports 8000 (the Ghostferry web UI) and 8001 (the harness's progress callback
  server) must be free on 127.0.0.1.

Running the batch size study
----------------------------

From the `benchmark/` directory:

```sh
bundle exec ruby studies/batch_size.rb
```

For each row size, the study seeds `benchmark.t` on the source, then for each
batch size wipes the target, runs `ghostferry-copydb` for 15 seconds, stops it
and computes the average copy speed from the progress callbacks. The generated
configuration sets `ControlServerConfig.WebBasedir` to the repository root, so
the web UI is found although the study runs from `benchmark/`.

Outputs, relative to `benchmark/`:

- `out/rs=<row size>-bs=<batch size>/conf.json`: the Ghostferry configuration;
- `out/rs=<row size>-bs=<batch size>/ghostferry.log`: Ghostferry's output;
- `out/rs=<row size>-bs=<batch size>/progress.json.log`: the progress callbacks;
- `out/rs=<row size>-bs=<batch size>/rows_written.csv`: time taken, rows
  written and state per progress callback;
- `studies/batch_size_benchmark.csv`: `row size,batch size,rows/s` per case.

Analysis notebook
-----------------

`studies/batch_size_vs_row_size.ipynb` analyses the checked-in historical
results in `studies/benchmark.csv`; it is not fed automatically by
`studies/batch_size.rb`. It needs Jupyter, NumPy, Matplotlib and SciPy, and must
be run with `benchmark/studies` as its working directory. To analyse new results,
change its CSV input, plot ranges and interpolation domain to match your study.
