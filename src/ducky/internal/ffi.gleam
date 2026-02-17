//// Foreign function interface bindings.
////
//// All @external declarations live here: DuckDB NIF and Erlang stdlib.
//// Do not use directly; use the public API instead.

import gleam/dynamic.{type Dynamic}

/// Opaque reference to a native connection resource.
pub type NativeConnection

/// Opaque reference to a native prepared statement resource.
pub type NativeStatement

/// Opens a connection to a DuckDB database.
@external(erlang, "ducky_nif", "connect")
pub fn connect(path: String) -> Result(NativeConnection, Dynamic)

/// Closes a database connection.
@external(erlang, "ducky_nif", "close")
pub fn close(conn: NativeConnection) -> Result(Dynamic, Dynamic)

/// Executes a SQL query with optional parameter binding.
///
/// Parameters are bound to `?` placeholders via prepared statements.
/// Pass an empty list for non-parameterized queries.
///
/// Returns {columns, rows} where:
/// - columns is a list of column names
/// - rows is a list of rows (each row is a list of dynamic values)
@external(erlang, "ducky_nif", "execute_query")
pub fn execute_query(
  conn: NativeConnection,
  sql: String,
  params: List(Dynamic),
) -> Result(#(List(String), List(List(Dynamic))), Dynamic)

/// Prepares a SQL statement for repeated execution.
///
/// Validates the SQL and returns a statement handle that can be
/// executed multiple times with different parameters.
@external(erlang, "ducky_nif", "prepare")
pub fn prepare(
  conn: NativeConnection,
  sql: String,
) -> Result(NativeStatement, Dynamic)

/// Executes a prepared statement with parameters.
///
/// Returns {columns, rows} like execute_query.
@external(erlang, "ducky_nif", "execute_prepared")
pub fn execute_prepared(
  stmt: NativeStatement,
  params: List(Dynamic),
) -> Result(#(List(String), List(List(Dynamic))), Dynamic)

/// Finalizes a prepared statement, releasing resources.
@external(erlang, "ducky_nif", "finalize")
pub fn finalize(stmt: NativeStatement) -> Result(Dynamic, Dynamic)

/// Bulk-appends rows via DuckDB's appender API.
@external(erlang, "ducky_nif", "append_rows")
pub fn append_rows(
  conn: NativeConnection,
  table: String,
  rows: List(List(Dynamic)),
) -> Result(Int, Dynamic)

/// Health check to verify NIF is loaded.
@external(erlang, "ducky_nif", "health_check")
pub fn health_check() -> String

/// Converts an atom to a string.
@external(erlang, "erlang", "atom_to_binary")
pub fn atom_to_string(atom: Dynamic) -> String

/// Converts a string to an atom.
@external(erlang, "erlang", "binary_to_atom")
pub fn binary_to_atom(s: String) -> Dynamic

/// Converts a list to a tuple.
@external(erlang, "erlang", "list_to_tuple")
pub fn list_to_tuple(items: List(Dynamic)) -> Dynamic
