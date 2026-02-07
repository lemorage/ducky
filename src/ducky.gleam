//// Native DuckDB driver for Gleam.
////
//// ## Quick Start
////
//// ```gleam
//// import ducky
//// import gleam/int
//// import gleam/io
////
//// pub fn main() {
////   use conn <- ducky.with_connection(":memory:")
////
////   // Create our duck pond
////   let assert Ok(_) = ducky.query(conn, "
////     CREATE TABLE ducks (name TEXT, quack_volume INT, is_rubber BOOLEAN)
////   ")
////   let assert Ok(_) = ducky.query(conn, "
////     INSERT INTO ducks VALUES
////       ('Sir Quacksalot', 95, false),
////       ('Duck Norris', 100, false),
////       ('Mallard Fillmore', 72, false),
////       ('Squeaky', 0, true)
////   ")
////
////   // Find the loudest quacker
////   let assert Ok(result) = ducky.query(conn, "
////     SELECT name, quack_volume FROM ducks
////     WHERE is_rubber = false
////     ORDER BY quack_volume DESC LIMIT 1
////   ")
////
////   case result.rows {
////     [ducky.Row([ducky.Text(name), ducky.Integer(volume)])] ->
////       io.println(name <> " wins at " <> int.to_string(volume) <> " decibels!")
////     _ -> io.println("The pond is empty...")
////   }
//// }
//// // => Duck Norris wins at 100 decibels!
//// ```

import ducky/internal/connection
import ducky/internal/ffi
import gleam/dict.{type Dict}
import gleam/dynamic
import gleam/dynamic/decode
import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/string

/// Errors that can occur during DuckDB operations.
pub type Error {
  /// Connection to database failed.
  ConnectionFailed(reason: String)
  /// SQL query has syntax errors.
  QuerySyntaxError(message: String)
  /// Operation timed out.
  Timeout(duration_ms: Int)
  /// Type conversion failed.
  TypeMismatch(expected: String, got: String)
  /// Unsupported parameter type in query.
  UnsupportedParameterType(type_name: String)
  /// Generic error from DuckDB.
  DatabaseError(message: String)
}

/// A value from a DuckDB result set.
pub type Value {
  Null
  Boolean(Bool)
  TinyInt(Int)
  SmallInt(Int)
  Integer(Int)
  BigInt(Int)
  Float(Float)
  Double(Float)
  Decimal(String)
  Text(String)
  Blob(BitArray)
  Timestamp(Int)
  Date(Int)
  Time(Int)
  Interval(months: Int, days: Int, nanos: Int)
  List(List(Value))
  Array(List(Value))
  Map(Dict(String, Value))
  Struct(Dict(String, Value))
  Union(tag: String, value: Value)
}

/// A single row from a query result.
pub type Row {
  Row(values: List(Value))
}

/// A complete query result with column metadata.
pub type DataFrame {
  DataFrame(columns: List(String), rows: List(Row))
}

/// An opaque connection to a DuckDB database.
pub opaque type Connection {
  Connection(internal: connection.Connection)
}

/// Opens a connection to a DuckDB database.
///
/// Must call `close()` when done. Use `with_connection()` instead
/// for automatic cleanup.
///
/// ## Examples
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
    "" -> Error(ConnectionFailed("path cannot be empty"))
    _ ->
      connection.do_connect(path)
      |> result.map(fn(internal) { Connection(internal: internal) })
      |> result.map_error(decode_nif_error)
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
pub fn close(conn: Connection) -> Result(Nil, Error) {
  connection.do_close(conn.internal)
  |> result.map_error(decode_nif_error)
}

/// Returns the database path for a connection.
pub fn path(conn: Connection) -> String {
  connection.path(conn.internal)
}

/// Executes operations with automatic connection cleanup.
///
/// Connection closes automatically on success or error.
///
/// ## Examples
///
/// ```gleam
/// use conn <- with_connection(":memory:")
/// query(conn, "SELECT 42")
/// ```
pub fn with_connection(
  db_path: String,
  callback: fn(Connection) -> Result(a, Error),
) -> Result(a, Error) {
  use conn <- result.try(connect(db_path))
  let res = callback(conn)
  let _ = close(conn)
  res
}

/// Executes operations within a transaction.
///
/// Commits on success, rolls back on error.
///
/// ## Examples
///
/// ```gleam
/// transaction(conn, fn(conn) {
///   use _ <- result.try(query(conn, "UPDATE accounts ..."))
///   query(conn, "SELECT * FROM accounts")
/// })
/// ```
pub fn transaction(
  conn: Connection,
  callback: fn(Connection) -> Result(a, Error),
) -> Result(a, Error) {
  use _ <- result.try(
    connection.execute_raw(conn.internal, "BEGIN TRANSACTION")
    |> result.map_error(decode_nif_error),
  )

  case callback(conn) {
    Ok(value) -> {
      use _ <- result.try(
        connection.execute_raw(conn.internal, "COMMIT")
        |> result.map_error(decode_nif_error),
      )
      Ok(value)
    }
    Error(err) -> {
      let _ = connection.execute_raw(conn.internal, "ROLLBACK")
      Error(err)
    }
  }
}

/// Executes a SQL query and returns structured results.
///
/// The query runs on a dirty scheduler to avoid blocking the BEAM.
/// Results are loaded into memory. For large datasets, use LIMIT/OFFSET
/// or filter in SQL to reduce memory usage.
///
/// ## Examples
///
/// ```gleam
/// query(conn, "SELECT id, name FROM users WHERE active = true")
/// // => Ok(DataFrame(columns: ["id", "name"], rows: [...]))
///
/// // For large datasets, paginate:
/// query(conn, "SELECT * FROM users LIMIT 1000 OFFSET 0")
/// ```
pub fn query(conn: Connection, sql: String) -> Result(DataFrame, Error) {
  ffi.execute_query(connection.native(conn.internal), sql, [])
  |> result.map(decode_dataframe)
  |> result.map_error(decode_nif_error)
}

/// Executes a parameterized SQL query with bound parameters to prevent SQL injection.
///
/// ## Examples
///
/// ```gleam
/// query_params(conn, "SELECT * FROM users WHERE id = ? AND age > ?", [
///   Integer(42),
///   Integer(18),
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
/// query_params(conn, "SELECT * FROM users WHERE name = ?", [Text(user_input)])
/// ```
pub fn query_params(
  conn: Connection,
  sql: String,
  params: List(Value),
) -> Result(DataFrame, Error) {
  use dynamic_params <- result.try(list.try_map(params, value_to_dynamic))

  ffi.execute_query(connection.native(conn.internal), sql, dynamic_params)
  |> result.map(decode_dataframe)
  |> result.map_error(decode_nif_error)
}

/// Get a value from a row by column index.
///
/// ## Examples
///
/// ```gleam
/// let row = Row([Integer(1), Text("Alice")])
/// get(row, 0)
/// // => Some(Integer(1))
///
/// get(row, 5)
/// // => None
/// ```
pub fn get(row: Row, index: Int) -> Option(Value) {
  case row {
    Row(values) -> list_at(values, index)
  }
}

/// Get a field value from a struct by field name.
///
/// Returns None if the value is not a Struct or the field does not exist.
///
/// ## Examples
///
/// ```gleam
/// let person = Struct(dict.from_list([#("name", Text("Alice")), #("age", Integer(30))]))
/// field(person, "name")
/// // => Some(Text("Alice"))
///
/// field(person, "unknown")
/// // => None
/// ```
pub fn field(value: Value, name: String) -> Option(Value) {
  case value {
    Struct(fields) -> dict.get(fields, name) |> option.from_result
    _ -> option.None
  }
}

fn list_at(items: List(a), index: Int) -> Option(a) {
  case items, index {
    [], _ -> option.None
    [first, ..], 0 -> option.Some(first)
    [_, ..rest], n if n > 0 -> list_at(rest, n - 1)
    _, _ -> option.None
  }
}

/// Creates a tagged tuple {tag, value} for NIF parameter encoding.
fn make_tagged(tag: String, value: dynamic.Dynamic) -> dynamic.Dynamic {
  ffi.list_to_tuple([ffi.binary_to_atom(tag), value])
}

/// Creates an interval 4-tuple {interval, months, days, nanos}.
fn make_interval_tuple(months: Int, days: Int, nanos: Int) -> dynamic.Dynamic {
  ffi.list_to_tuple([
    ffi.binary_to_atom("interval"),
    dynamic.int(months),
    dynamic.int(days),
    dynamic.int(nanos),
  ])
}

/// Converts a Value to a Dynamic for passing to the NIF.
fn value_to_dynamic(value: Value) -> Result(dynamic.Dynamic, Error) {
  case value {
    Null -> Ok(dynamic.nil())
    Boolean(b) -> Ok(dynamic.bool(b))
    TinyInt(i) -> Ok(dynamic.int(i))
    SmallInt(i) -> Ok(dynamic.int(i))
    Integer(i) -> Ok(dynamic.int(i))
    BigInt(i) -> Ok(dynamic.int(i))
    Float(f) -> Ok(dynamic.float(f))
    Double(f) -> Ok(dynamic.float(f))
    Text(s) -> Ok(dynamic.string(s))
    Blob(bits) -> Ok(dynamic.bit_array(bits))
    Timestamp(micros) -> Ok(make_tagged("timestamp", dynamic.int(micros)))
    Date(days) -> Ok(make_tagged("date", dynamic.int(days)))
    Time(micros) -> Ok(make_tagged("time", dynamic.int(micros)))
    Interval(months, days, nanos) ->
      Ok(make_interval_tuple(months, days, nanos))
    Decimal(s) -> Ok(make_tagged("decimal", dynamic.string(s)))
    List(_) | Array(_) | Map(_) | Struct(_) | Union(_, _) ->
      Error(UnsupportedParameterType(
        "List, Array, Map, Struct, and Union types cannot be used as query parameters",
      ))
  }
}

/// Decodes raw NIF result into a DataFrame.
fn decode_dataframe(
  raw: #(List(String), List(List(dynamic.Dynamic))),
) -> DataFrame {
  let #(columns, rows) = raw
  let decoded_rows =
    rows
    |> list.map(fn(row) {
      let values = list.map(row, decode_value)
      Row(values: values)
    })
  DataFrame(columns: columns, rows: decoded_rows)
}

/// Decodes a dynamic value from the NIF into a typed Value.
fn decode_value(dyn: dynamic.Dynamic) -> Value {
  let classification = dynamic.classify(dyn)
  case classification {
    "Atom" -> Null
    "Dict" -> decode_struct(dyn)
    "List" -> decode_list(dyn)
    "Array" -> decode_tagged_tuple(dyn)
    _ -> {
      let value_decoder =
        decode.one_of(decode.bool |> decode.map(Boolean), or: [
          decode.int |> decode.map(Integer),
          decode.float |> decode.map(Double),
          decode.string |> decode.map(Text),
          decode.bit_array |> decode.map(Blob),
        ])

      decode.run(dyn, value_decoder)
      |> result.unwrap(or: Null)
    }
  }
}

/// Decodes a list with recursive element decoding.
fn decode_list(dyn: dynamic.Dynamic) -> Value {
  let decoder = decode.list(decode.dynamic)

  case decode.run(dyn, decoder) {
    Ok(elements) -> {
      let decoded_elements = list.map(elements, decode_value)
      List(decoded_elements)
    }
    Error(_) -> Null
  }
}

/// Decodes an Erlang map into a Struct with recursive value decoding.
fn decode_struct(dyn: dynamic.Dynamic) -> Value {
  let decoder =
    decode.dict(decode.string, decode.dynamic)
    |> decode.map(fn(fields) {
      let decoded_fields =
        fields
        |> dict.to_list
        |> list.map(fn(pair) {
          let #(key, val) = pair
          #(key, decode_value(val))
        })
        |> dict.from_list

      Struct(decoded_fields)
    })

  decode.run(dyn, decoder)
  |> result.unwrap(or: Null)
}

/// Decodes tagged tuples sent as Erlang arrays for various types.
fn decode_tagged_tuple(dyn: dynamic.Dynamic) -> Value {
  let tag_decoder = {
    use tag_dynamic <- decode.subfield([0], decode.dynamic)
    decode.success(tag_dynamic)
  }

  case decode.run(dyn, tag_decoder) {
    Ok(tag_dynamic) -> {
      let tag = case dynamic.classify(tag_dynamic) {
        "Atom" -> ffi.atom_to_string(tag_dynamic)
        _ -> ""
      }

      case tag {
        "decimal" -> decode_decimal_value(dyn)
        "array" -> decode_array_value(dyn)
        "map" -> decode_map_value(dyn)
        "union" -> decode_union_value(dyn)
        "timestamp" | "date" | "time" -> decode_temporal_tuple(dyn)
        "interval" -> decode_interval_tuple(dyn)
        _ -> Null
      }
    }
    Error(_) -> Null
  }
}

/// Decodes a decimal tagged tuple {decimal, "string"}.
fn decode_decimal_value(dyn: dynamic.Dynamic) -> Value {
  let decoder = {
    use value <- decode.subfield([1], decode.string)
    decode.success(value)
  }
  case decode.run(dyn, decoder) {
    Ok(value) -> Decimal(value)
    Error(_) -> Null
  }
}

/// Decodes an array tagged tuple {array, [elements]}.
fn decode_array_value(dyn: dynamic.Dynamic) -> Value {
  let decoder = {
    use elements <- decode.subfield([1], decode.list(decode.dynamic))
    decode.success(elements)
  }
  case decode.run(dyn, decoder) {
    Ok(elements) -> {
      let decoded_elements = list.map(elements, decode_value)
      Array(decoded_elements)
    }
    Error(_) -> Null
  }
}

/// Decodes a map tagged tuple {map, %{key => value}}.
fn decode_map_value(dyn: dynamic.Dynamic) -> Value {
  let decoder = {
    use entries <- decode.subfield(
      [1],
      decode.dict(decode.string, decode.dynamic),
    )
    decode.success(entries)
  }
  case decode.run(dyn, decoder) {
    Ok(entries) -> {
      let decoded_entries =
        entries
        |> dict.to_list
        |> list.map(fn(pair) {
          let #(key, val) = pair
          #(key, decode_value(val))
        })
        |> dict.from_list
      Map(decoded_entries)
    }
    Error(_) -> Null
  }
}

/// Decodes a union tagged tuple {union, tag_string, value}.
fn decode_union_value(dyn: dynamic.Dynamic) -> Value {
  let decoder = {
    use tag <- decode.subfield([1], decode.string)
    use value <- decode.subfield([2], decode.dynamic)
    decode.success(#(tag, value))
  }
  case decode.run(dyn, decoder) {
    Ok(#(tag, value)) -> Union(tag: tag, value: decode_value(value))
    Error(_) -> Null
  }
}

/// Decodes temporal tagged tuples (timestamp, date, time).
fn decode_temporal_tuple(dyn: dynamic.Dynamic) -> Value {
  let decoder = {
    use tag_dynamic <- decode.subfield([0], decode.dynamic)
    use value <- decode.subfield([1], decode.int)

    let tag = case dynamic.classify(tag_dynamic) {
      "Atom" -> ffi.atom_to_string(tag_dynamic)
      "String" ->
        decode.run(tag_dynamic, decode.string)
        |> result.unwrap(or: "")
      _ -> ""
    }

    decode.success(#(tag, value))
  }

  case decode.run(dyn, decoder) {
    Ok(#(tag, value)) ->
      case tag {
        "timestamp" -> Timestamp(value)
        "date" -> Date(value)
        "time" -> Time(value)
        _ -> Null
      }
    Error(_) -> Null
  }
}

/// Decodes interval 4-tuple {interval, months, days, nanos}.
fn decode_interval_tuple(dyn: dynamic.Dynamic) -> Value {
  let decoder = {
    use months <- decode.subfield([1], decode.int)
    use days <- decode.subfield([2], decode.int)
    use nanos <- decode.subfield([3], decode.int)
    decode.success(#(months, days, nanos))
  }

  case decode.run(dyn, decoder) {
    Ok(#(months, days, nanos)) -> Interval(months, days, nanos)
    Error(_) -> Null
  }
}

fn error_from_tag(tag: String, message: String) -> Error {
  case tag {
    "connection_failed" -> ConnectionFailed(message)
    "query_syntax_error" -> QuerySyntaxError(message)
    "unsupported_parameter_type" -> UnsupportedParameterType(message)
    "database_error" -> DatabaseError(message)
    _ -> DatabaseError("[" <> tag <> "] " <> message)
  }
}

fn error_tuple_decoder() -> decode.Decoder(#(String, String)) {
  use error_type_dyn <- decode.subfield([0], decode.dynamic)
  use message <- decode.subfield([1], decode.string)

  let error_type = case dynamic.classify(error_type_dyn) {
    "Atom" -> ffi.atom_to_string(error_type_dyn)
    _ -> "unknown"
  }

  decode.success(#(error_type, message))
}

/// Decodes an error from the NIF layer.
fn decode_nif_error(err: dynamic.Dynamic) -> Error {
  let decoder = decode.at([1], error_tuple_decoder())

  case decode.run(err, decoder) {
    Ok(#(tag, msg)) -> error_from_tag(tag, msg)
    Error(_) -> fallback_decode(err)
  }
}

/// Fallback decoder for unexpected error formats.
fn fallback_decode(err: dynamic.Dynamic) -> Error {
  case decode.run(err, error_tuple_decoder()) {
    Ok(#(tag, msg)) -> error_from_tag(tag, msg)
    Error(_) -> DatabaseError("Unknown error: " <> string.inspect(err))
  }
}
