# BeckDB - Log Structured KV store

BeckDB is a bitcask-inspired non-relational database. It aims to provide low latency and high throughput read/writes by leveraging an in-memory key-value store and and append-only file for durability.

## Architecture

![Architecture](./architecture.png)

## Setup

## Usage

## Considerations

-   The data and hints file are all kept in the data directory with extensions, `xx.log` and `xx.hint` respectively
-   Keys are stored as strings with values being stored as byte slice to allow for any value type.

## References

-   Checkout the bitcask paper to know more about the underlying storage engine powering BeckDB
