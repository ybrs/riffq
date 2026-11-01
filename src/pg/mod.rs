//! `PostgreSQL` wire-protocol support types shared across the server.
//!
//! Kept as its own module so the mapping between Arrow and `PostgreSQL`
//! stays independent of the connection handling in `lib.rs`.
pub mod arrow_map;

pub use arrow_map::arrow_type_to_pgwire;
