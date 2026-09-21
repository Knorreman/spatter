# spatter-core

Core types for [Spatter](https://github.com/Knorreman/spatter), an experimental
Spark-style RDD engine in Rust. This crate contains partition splitting,
local-master parsing, lineage identifiers, dependency types and errors.

Applications normally depend on `spatter`, which re-exports its public core
types. Both workspace crates currently use version 0.1.0 and declare Rust 1.74
as their minimum supported version. The repository CI checks that toolchain.

License: Apache-2.0.
