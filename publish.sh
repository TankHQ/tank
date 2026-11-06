#!/bin/sh
set -eu

# Increase the workspace version in Cargo.toml before publishing.
cd -- "$(dirname -- "$0")"

# Publish dependencies first; chDB depends on the ClickHouse driver.
for package in \
    tank-core \
    tank-macros \
    tank \
    tank-tests \
    tank-clickhouse \
    tank-chdb \
    tank-duckdb \
    tank-mongodb \
    tank-mysql \
    tank-postgres \
    tank-scylladb \
    tank-sqlite \
    tank-valkey
do
    cargo publish -p "$package" "$@"
done
