//// Query execution and result handling.

import ducky/connection.{type Connection}
import ducky/error.{type Error}
import ducky/internal/error_decoder
import ducky/internal/ffi
import ducky/types.{type DataFrame, type Value}
import gleam/dynamic
import gleam/dynamic/decode
import gleam/list
import gleam/result

/// Executes a SQL query and returns structured results.
///
/// The query runs on a dirty scheduler to avoid blocking the BEAM.
/// Large result sets are streamed to prevent memory exhaustion.
///
/// ## Examples
///
/// ```gleam
/// query(conn, "SELECT id, name FROM users WHERE active = true")
/// // => Ok(DataFrame(columns: ["id", "name"], rows: [...]))
/// ```
pub fn query(conn: Connection, sql: String) -> Result(DataFrame, Error) {
  ffi.execute_query(connection.native(conn), sql, [])
  |> result.map(decode_dataframe)
  |> result.map_error(error_decoder.decode_nif_error)
}

/// Executes a parameterized SQL query with bound parameters to prevent SQL injection.
///
/// ## Examples
///
/// ```gleam
/// query_params(conn, "SELECT * FROM users WHERE id = ? AND age > ?", [
///   types.Integer(42),
///   types.Integer(18),
/// ])
/// // => Ok(DataFrame(...))
/// ```
///
/// ## Security
///
/// Always use this function when including user input in queries:
/// ```gleam
/// // UNSAFE - SQL injection risk
/// query(conn, "SELECT * FROM users WHERE name = '" <> user_input <> "'")
///
/// // SAFE - parameters are properly escaped
/// query_params(conn, "SELECT * FROM users WHERE name = ?", [types.Text(user_input)])
/// ```
pub fn query_params(
  conn: Connection,
  sql: String,
  params: List(types.Value),
) -> Result(DataFrame, Error) {
  let dynamic_params = list.map(params, value_to_dynamic)

  ffi.execute_query(connection.native(conn), sql, dynamic_params)
  |> result.map(decode_dataframe)
  |> result.map_error(error_decoder.decode_nif_error)
}

/// Decodes a dynamic value from the NIF into a typed Value.
fn decode_value(dyn: dynamic.Dynamic) -> Value {
  // Check if this is the null atom (special case for SQL NULL)
  case dynamic.classify(dyn) {
    "Atom" -> types.Null
    _ -> {
      let value_decoder =
        decode.one_of(decode.bool |> decode.map(types.Boolean), or: [
          decode.int |> decode.map(types.Integer),
          decode.float |> decode.map(types.Double),
          decode.string |> decode.map(types.Text),
          decode.bit_array |> decode.map(types.Blob),
        ])

      // Run the decoder, fallback to Null if all decoders fail
      decode.run(dyn, value_decoder)
      |> result.unwrap(or: types.Null)
    }
  }
}

/// Converts a Value to a Dynamic for passing to the NIF.
fn value_to_dynamic(value: Value) -> dynamic.Dynamic {
  case value {
    types.Null -> dynamic.nil()
    types.Boolean(b) -> dynamic.bool(b)
    types.TinyInt(i) -> dynamic.int(i)
    types.SmallInt(i) -> dynamic.int(i)
    types.Integer(i) -> dynamic.int(i)
    types.BigInt(i) -> dynamic.int(i)
    types.Float(f) -> dynamic.float(f)
    types.Double(f) -> dynamic.float(f)
    types.Text(s) -> dynamic.string(s)
    types.Blob(bits) -> dynamic.bit_array(bits)
    // Complex types not yet supported as parameters
    types.Timestamp(_)
    | types.Date(_)
    | types.Time(_)
    | types.Interval(_)
    | types.List(_) -> dynamic.nil()
  }
}

/// Decodes raw NIF result into a DataFrame.
fn decode_dataframe(
  result: #(List(String), List(List(dynamic.Dynamic))),
) -> DataFrame {
  let #(columns, rows) = result
  let decoded_rows =
    rows
    |> list.map(fn(row) {
      let values = list.map(row, decode_value)
      types.Row(values: values)
    })
  types.DataFrame(columns: columns, rows: decoded_rows)
}
