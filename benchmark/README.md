# BemiDB Bechmark

We use the standardized TPC-H benchmark to compare PostgreSQL with BemiDB.
This benchmark measures the performance of databases that handle large volumes of data and perform business-oriented ad-hoc queries (OLAP).

![TPC-H database structure](/img/tpc-h_database_structure.png)

## Running the TPC-H Benchmark

### PostgreSQL

Download and unzip `TPC-H_generated_data_s*.zip` from the latest release into the "benchmark/data" directory and then set up a local PostgreSQL database:

```sh
make pg-init
make pg-up
make pg-create
```

Run the benchmark queries with PostgreSQL:

```sh
make pg-benchmark
```

Run the benchmark queries with indexed PostgreSQL:

```sh
make pg-index
make pg-benchmark
```

### BemiDB

Set up a local BemiDB database:

```sh
make sync
make up
```

Run the benchmark queries with BemiDB:

```sh
make benchmark
```

## Generating the TPC-H Data

Install the TPC-H benchmark kit:

```sh
make tpch-install MACHINE=MACOS # MACHINE=LINUX for Linux
make tpch-generate SCALE_FACTOR=1
```
