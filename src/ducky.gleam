//// Native DuckDB driver for Gleam.
////
//// Provides a type-safe, ergonomic interface to DuckDB for both
//// Erlang and JavaScript runtimes.
////
//// ## Quick Start
////
//// ```gleam
//// import ducky
//// import gleam/io
//// import gleam/string
////
//// pub fn main() {
////   let assert Ok(conn) = ducky.connect(":memory:")
////   let assert Ok(result) = ducky.query(conn, "SELECT 42 as answer")
////   io.println(string.inspect(result))
//// }
//// ```

import ducky/connection
import ducky/error
import ducky/query
import ducky/types

// Re-export core types and functions
pub type Connection =
  connection.Connection

pub type Error =
  error.Error

pub type Value =
  types.Value

pub type Row =
  types.Row

pub type DataFrame =
  types.DataFrame

pub const connect = connection.connect

pub const close = connection.close

pub const with_connection = connection.with_connection

pub const transaction = connection.transaction

pub const query = query.query

pub const query_params = query.query_params
