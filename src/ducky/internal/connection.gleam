//// Internal connection management implementation.
////
//// Handles the opaque Connection type and lifecycle operations.

import ducky/internal/ffi
import gleam/dynamic.{type Dynamic}
import gleam/result

/// An opaque connection to a DuckDB database.
pub opaque type Connection {
  Connection(native: ffi.NativeConnection, path: String)
}

/// Returns the native connection handle for FFI calls.
pub fn native(connection: Connection) -> ffi.NativeConnection {
  connection.native
}

/// Returns the database path for a connection.
pub fn path(connection: Connection) -> String {
  connection.path
}

/// Opens a connection to a DuckDB database.
/// Returns Dynamic error for caller to decode.
pub fn do_connect(db_path: String) -> Result(Connection, Dynamic) {
  ffi.connect(db_path)
  |> result.map(fn(nat) { Connection(native: nat, path: db_path) })
}

/// Closes a database connection.
/// Returns Dynamic error for caller to decode.
pub fn do_close(connection: Connection) -> Result(Nil, Dynamic) {
  ffi.close(connection.native)
  |> result.map(fn(_) { Nil })
}

/// Executes a query for transaction management.
/// Returns Dynamic error for caller to decode.
pub fn execute_raw(connection: Connection, sql: String) -> Result(Nil, Dynamic) {
  ffi.execute_query(connection.native, sql, [])
  |> result.map(fn(_) { Nil })
}
