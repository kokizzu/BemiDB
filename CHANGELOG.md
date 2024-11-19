# Changelog

#### [v0.12.0](https://github.com/BemiHQ/BemiDB/compare/v0.11.0...v0.12.0) - 2024-11-19

- Add support for Postgres `pg_snapshot` type

#### [v0.11.0](https://github.com/BemiHQ/BemiDB/compare/v0.10.0...v0.11.0) - 2024-11-18

- Add support for Postgres `xid` and `xid8` types

#### [v0.10.0](https://github.com/BemiHQ/BemiDB/compare/v0.9.0...v0.10.0) - 2024-11-16

- Add support for Postgres network address types
- **Breaking change**: Rename `--iceberg-path` flag to `--storage-path`

#### [v0.9.0](https://github.com/BemiHQ/BemiDB/compare/v0.8.0...v0.9.0) - 2024-11-15

- Allow syncing multiple PG databases into the same BemiDB database using schema prefixes
- **Breaking change**: Read default `AWS_*` env vars if they exist instead of `BEMIDB_AWS_*`
- **Breaking change**: Rename `--storage-type` flag value from `AWS_S3` to just `S3`
- **Breaking change**: Rename `--interval` flag name to `--pg-sync-interval`

#### [v0.8.0](https://github.com/BemiHQ/BemiDB/compare/v0.7.0...v0.8.0) - 2024-11-14

- Migrate arrays from REPEATED to LIST parquet format to allow NULLs
- Fix null count stats in generated Parquet files

#### [v0.7.0](https://github.com/BemiHQ/BemiDB/compare/v0.6.0...v0.7.0) - 2024-11-14

- Add support for non-`public` schemas

#### [v0.6.0](https://github.com/BemiHQ/BemiDB/compare/v0.5.1...v0.6.0) - 2024-11-14

- Add support for Postgres geometric data types

#### [v0.5.1](https://github.com/BemiHQ/BemiDB/compare/v0.5.0...v0.5.1) - 2024-11-12

- Add Postgres sync intervals

#### [v0.5.0](https://github.com/BemiHQ/BemiDB/compare/v0.4.4...v0.5.0) - 2024-11-12

- Add support for native array types

#### [v0.4.4](https://github.com/BemiHQ/BemiDB/compare/v0.4.3...v0.4.4) - 2024-11-12

- Fix arm64 MacOS path with linked libc++

#### [v0.4.3](https://github.com/BemiHQ/BemiDB/compare/v0.4.2...v0.4.3) - 2024-11-12

- Fix serializing Postgres `tsvector` type

#### [v0.4.2](https://github.com/BemiHQ/BemiDB/compare/v0.4.1...v0.4.2) - 2024-11-12

- Fix converting Postgres timestamps without timezones

#### [v0.4.1](https://github.com/BemiHQ/BemiDB/compare/v0.4.0...v0.4.1) - 2024-11-12

- Fix syncing camel-cased Postgres tables

#### [v0.4.0](https://github.com/BemiHQ/BemiDB/compare/v0.3.2...v0.4.0) - 2024-11-11

- Serialize user-defined types as strings

#### [v0.3.2](https://github.com/BemiHQ/BemiDB/compare/v0.3.1...v0.3.2) - 2024-11-11

- Fix Postgres `bigint` type conversion to Iceberg and Parquet

#### [v0.3.1](https://github.com/BemiHQ/BemiDB/compare/v0.3.0...v0.3.1) - 2024-11-10

- Fix the binary compilation for arm64 MacOS with linked libc++abi

#### [v0.3.0](https://github.com/BemiHQ/BemiDB/compare/v0.2.0...v0.3.0) - 2024-11-10

- Make Postgres `COPY` command work with remotely running Postgres [#8](https://github.com/BemiHQ/BemiDB/pull/8)

#### [v0.2.0](https://github.com/BemiHQ/BemiDB/compare/v0.1.0...v0.2.0) - 2024-11-08

- Bump DuckDB version to 1.1.3
- Compile the binary for arm64 MacOS with linked libc++
- Fix loading tables with a relative Iceberg path on a local disk

#### v0.1.0 - 2024-11-06

- Create initial version
