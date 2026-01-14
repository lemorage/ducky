//// Database connection management.

import ducky/error.{type Error}
import ducky/internal/error_decoder
import ducky/internal/ffi
import gleam/result

/// An opaque connection to a DuckDB database.
pub opaque type Connection {
  Connection(native: ffi.NativeConnection, path: String)
}

/// Opens a connection to a DuckDB database.
///
/// Must call `close()` when done. Use `with_connection()` instead
/// for automatic cleanup.
///
/// ```gleam
/// connect(":memory:")
/// // => Ok(Connection(...))
///
/// connect("data.duckdb")
/// // => Ok(Connection(...))
/// ```
pub fn connect(path: String) -> Result(Connection, Error) {
  case path {
    "" -> Error(error.ConnectionFailed("path cannot be empty"))
    _ -> {
      ffi.connect(path)
      |> result.map(fn(native) { Connection(native: native, path: path) })
      |> result.map_error(error_decoder.decode_nif_error)
    }
  }
}

/// Closes a database connection.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok(conn) = connect(":memory:")
/// let assert Ok(_) = close(conn)
/// ```
///
/// ## Errors
///
/// Returns an error if the connection cannot be closed.
pub fn close(connection: Connection) -> Result(Nil, Error) {
  ffi.close(connection.native)
  |> result.map(fn(_) { Nil })
  |> result.map_error(error_decoder.decode_nif_error)
}

/// Returns the database path for a connection.
pub fn path(connection: Connection) -> String {
  connection.path
}

/// Returns the native connection handle for FFI calls.
///
/// This is an internal function for use by other modules in the ducky package.
pub fn native(connection: Connection) -> ffi.NativeConnection {
  connection.native
}

/// Executes operations with automatic connection cleanup.
///
/// Connection closes automatically on success or error.
///
/// ```gleam
/// use conn <- with_connection(":memory:")
/// query.query(conn, "SELECT 42")
/// ```
pub fn with_connection(
  path: String,
  callback: fn(Connection) -> Result(a, Error),
) -> Result(a, Error) {
  use conn <- result.try(connect(path))
  let result = callback(conn)
  let _ = close(conn)
  result
}

/// Executes operations within a transaction.
///
/// Commits on success, rolls back on error.
///
/// ```gleam
/// transaction(conn, fn(conn) {
///   use _ <- result.try(query.query(conn, "UPDATE accounts ..."))
///   query.query(conn, "SELECT * FROM accounts")
/// })
/// ```
pub fn transaction(
  conn: Connection,
  callback: fn(Connection) -> Result(a, Error),
) -> Result(a, Error) {
  use _ <- result.try(
    ffi.execute_query(conn.native, "BEGIN TRANSACTION", [])
    |> result.map(fn(_) { Nil })
    |> result.map_error(error_decoder.decode_nif_error),
  )

  case callback(conn) {
    Ok(value) -> {
      use _ <- result.try(
        ffi.execute_query(conn.native, "COMMIT", [])
        |> result.map(fn(_) { Nil })
        |> result.map_error(error_decoder.decode_nif_error),
      )
      Ok(value)
    }
    Error(err) -> {
      let _ =
        ffi.execute_query(conn.native, "ROLLBACK", [])
        |> result.map(fn(_) { Nil })
      Error(err)
    }
  }
}
