//// Native DuckDB driver for Gleam.
////
//// ## Quick Start
////
//// ```gleam
//// import ducky
//// import gleam/dynamic/decode
//// import gleam/io
//// import gleam/list
//// import gleam/result
////
//// pub type Duck {
////   Duck(name: String, quack_db: Int)
//// }
////
//// pub fn main() {
////   use conn <- ducky.with_connection(":memory:")
////
////   use _ <- result.try(
////     ducky.sql("CREATE TABLE ducks (name TEXT, quack_db INT)")
////     |> ducky.run(conn),
////   )
////   use _ <- result.try(
////     ducky.sql("INSERT INTO ducks VALUES
////       ('Sir Quacksalot', 95), ('Duck Norris', 100)")
////     |> ducky.run(conn),
////   )
////
////   let decoder = {
////     use name <- decode.field(0, decode.string)
////     use quack_db <- decode.field(1, decode.int)
////     decode.success(Duck(name:, quack_db:))
////   }
////
////   use result <- result.map(
////     ducky.sql("SELECT name, quack_db FROM ducks ORDER BY quack_db DESC")
////     |> ducky.returning(decoder)
////     |> ducky.run(conn),
////   )
////
////   list.each(result.rows, fn(d) { io.println(d.name) })
//// }
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
  /// Unsupported parameter type in query.
  UnsupportedParameterType(type_name: String)
  /// Statement has been finalized and cannot be used.
  StatementFinalized
  /// Row decoder returned errors.
  DecodeFailed(errors: List(decode.DecodeError))
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

/// Typed query result. `a` is the decoded row type.
pub type Returned(a) {
  Returned(count: Int, rows: List(a))
}

/// Column-oriented result. Each inner list corresponds to one column.
pub type Columnar {
  Columnar(names: List(String), data: List(List(Value)))
}

/// An opaque query builder. The phantom `a` tracks the decoded row type.
///
/// Construct with `sql`, configure with `parameter`/`returning`,
/// terminate with `run` or `as_columns`.
pub opaque type Query(a) {
  Query(
    statement: String,
    params: List(Value),
    decoder: fn(List(dynamic.Dynamic)) -> Result(a, Error),
  )
}

/// An opaque connection to a DuckDB database.
pub opaque type Connection {
  Connection(internal: connection.Connection)
}

/// An opaque prepared statement for repeated execution.
///
/// Prepared statements allow you to compile a SQL query once and execute it
/// multiple times with different parameters, avoiding repeated parsing overhead.
pub opaque type Statement {
  Statement(native: ffi.NativeStatement)
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

/// Useful for logging which database a connection points to.
pub fn path(conn: Connection) -> String {
  connection.path(conn.internal)
}

/// Opens a connection, runs the callback, then closes. Preferred over connect/close.
///
/// ## Examples
///
/// ```gleam
/// use conn <- with_connection(":memory:")
/// ducky.sql("SELECT 42") |> ducky.run(conn)
/// ```
pub fn with_connection(
  db_path: String,
  callback: fn(Connection) -> Result(a, Error),
) -> Result(a, Error) {
  use conn <- result.try(connect(db_path))
  let res = callback(conn)
  // Intentionally ignore close errors to preserve the callback result
  let _ = close(conn)
  res
}

/// Wraps a callback in BEGIN/COMMIT. Rolls back on error.
///
/// ## Examples
///
/// ```gleam
/// transaction(conn, fn(conn) {
///   use _ <- result.try(ducky.sql("UPDATE accounts ...") |> ducky.run(conn))
///   ducky.sql("SELECT * FROM accounts") |> ducky.run(conn)
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
      // Intentionally ignore rollback errors to preserve the original error
      let _ = connection.execute_raw(conn.internal, "ROLLBACK")
      Error(err)
    }
  }
}

/// Prepares a SQL statement for repeated execution.
///
/// Validates the SQL syntax immediately and returns a statement handle.
/// Use `execute` to run the statement with parameters.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok(stmt) = prepare(conn, "INSERT INTO users (name, age) VALUES (?, ?)")
/// let assert Ok(_) = execute(stmt, [text("Alice"), int(30)])
/// let assert Ok(_) = execute(stmt, [text("Bob"), int(25)])
/// let assert Ok(_) = finalize(stmt)
/// ```
///
/// ## Performance
///
/// DuckDB caches parsed query plans internally, so repeated executions
/// with different parameters benefit from the cached plan. This can
/// provide speedups for bulk operations.
pub fn prepare(conn: Connection, sql: String) -> Result(Statement, Error) {
  ffi.prepare(connection.native(conn.internal), sql)
  |> result.map(fn(native) { Statement(native: native) })
  |> result.map_error(decode_nif_error)
}

/// Runs a prepared statement. For hot loops with repeated bindings.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok(stmt) = prepare(conn, "SELECT * FROM users WHERE age > ?")
/// let assert Ok(result) = execute(stmt, [int(18)])
/// // result.rows contains matching users
/// ```
pub fn execute(
  stmt: Statement,
  params: List(Value),
) -> Result(Returned(Row), Error) {
  use dynamic_params <- result.try(list.try_map(params, value_to_dynamic))

  use raw <- result.try(
    ffi.execute_prepared(stmt.native, dynamic_params)
    |> result.map_error(decode_nif_error),
  )

  let #(_columns, raw_rows) = raw
  use decoded_rows <- result.try(list.try_map(raw_rows, decode_raw_row))

  Ok(Returned(count: list.length(decoded_rows), rows: decoded_rows))
}

/// Finalizes a prepared statement, releasing its resources.
///
/// After finalization, the statement cannot be used again.
/// For automatic cleanup, prefer `with_statement`.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok(stmt) = prepare(conn, "SELECT 1")
/// // ... use the statement ...
/// let assert Ok(_) = finalize(stmt)
/// ```
pub fn finalize(stmt: Statement) -> Result(Nil, Error) {
  ffi.finalize(stmt.native)
  |> result.map(fn(_) { Nil })
  |> result.map_error(decode_nif_error)
}

/// Executes operations with a prepared statement, ensuring cleanup.
///
/// The statement is automatically finalized when the callback returns,
/// regardless of success or failure.
///
/// ## Examples
///
/// ```gleam
/// use stmt <- with_statement(conn, "INSERT INTO users (name) VALUES (?)")
/// list.try_each(names, fn(name) {
///   execute(stmt, [text(name)])
///   |> result.map(fn(_) { Nil })
/// })
/// ```
pub fn with_statement(
  conn: Connection,
  sql: String,
  callback: fn(Statement) -> Result(a, Error),
) -> Result(a, Error) {
  use stmt <- result.try(prepare(conn, sql))
  let res = callback(stmt)
  // Intentionally ignore finalize errors to preserve the callback result
  let _ = finalize(stmt)
  res
}

/// Bypasses SQL parsing. Atomic: all rows succeed or none commit.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok(count) = append(conn, "users", [
///   [int(1), text("Alice")],
///   [int(2), text("Bob")],
/// ])
/// // count == 2
/// ```
pub fn append(
  conn: Connection,
  table: String,
  rows: List(List(Value)),
) -> Result(Int, Error) {
  case rows {
    [] -> Ok(0)
    _ -> {
      use dynamic_rows <- result.try(
        list.try_map(rows, fn(row) { list.try_map(row, value_to_dynamic) }),
      )

      ffi.append_rows(connection.native(conn.internal), table, dynamic_rows)
      |> result.map_error(decode_nif_error)
    }
  }
}

/// Builds a query value. No I/O until `run` or `as_columns`.
///
/// ## Examples
///
/// ```gleam
/// ducky.sql("SELECT * FROM users")
/// |> ducky.run(conn)
/// ```
pub fn sql(statement: String) -> Query(Row) {
  Query(statement: statement, params: [], decoder: decode_raw_row)
}

/// Binds one parameter to the next `?` placeholder.
///
/// ## Examples
///
/// ```gleam
/// ducky.sql("SELECT * FROM users WHERE id = ?")
/// |> ducky.parameter(ducky.int(42))
/// |> ducky.run(conn)
/// ```
pub fn parameter(query: Query(a), value: Value) -> Query(a) {
  // Params stored in reverse; run/as_columns reverse before execution.
  Query(..query, params: [value, ..query.params])
}

/// Binds multiple parameters at once, in order.
///
/// ## Examples
///
/// ```gleam
/// ducky.sql("SELECT * FROM users WHERE id = ? AND age > ?")
/// |> ducky.parameters([ducky.int(42), ducky.int(18)])
/// |> ducky.run(conn)
/// ```
pub fn parameters(query: Query(a), values: List(Value)) -> Query(a) {
  // Params stored in reverse; run/as_columns reverse before execution.
  Query(..query, params: list.append(list.reverse(values), query.params))
}

/// Sets a decoder for typed rows. Constrained to `Query(Row)`.
/// Can only be called once; the compiler enforces this.
///
/// ## Examples
///
/// ```gleam
/// let decoder = {
///   use name <- decode.field(0, decode.string)
///   use age <- decode.field(1, decode.int)
///   decode.success(#(name, age))
/// }
///
/// ducky.sql("SELECT name, age FROM users")
/// |> ducky.returning(decoder)
/// |> ducky.run(conn)
/// // => Ok(Returned(count: 1, rows: [#("Alice", 30)]))
/// ```
pub fn returning(query: Query(Row), decoder: decode.Decoder(b)) -> Query(b) {
  Query(statement: query.statement, params: query.params, decoder: fn(raw_row) {
    let row_tuple = ffi.list_to_tuple(raw_row)
    decode.run(row_tuple, decoder)
    |> result.map_error(fn(errors) { DecodeFailed(errors: errors) })
  })
}

/// Executes the query. Use `sql` for one-shot queries; `prepare`/`execute` for hot loops.
///
/// ## Examples
///
/// ```gleam
/// ducky.sql("SELECT 1 AS x")
/// |> ducky.run(conn)
/// // => Ok(Returned(count: 1, rows: [Row([Integer(1)])]))
/// ```
pub fn run(query: Query(a), conn: Connection) -> Result(Returned(a), Error) {
  let params = list.reverse(query.params)
  use dynamic_params <- result.try(list.try_map(params, value_to_dynamic))

  use raw <- result.try(
    ffi.execute_query(
      connection.native(conn.internal),
      query.statement,
      dynamic_params,
    )
    |> result.map_error(decode_nif_error),
  )

  let #(_columns, raw_rows) = raw
  use decoded_rows <- result.try(list.try_map(raw_rows, query.decoder))

  Ok(Returned(count: list.length(decoded_rows), rows: decoded_rows))
}

/// Returns columns instead of rows. DuckDB's columnar strength exposed.
///
/// ## Examples
///
/// ```gleam
/// ducky.sql("SELECT name, age FROM users")
/// |> ducky.as_columns(conn)
/// // => Ok(Columnar(names: ["name", "age"], data: [[Text("Alice")], [Integer(30)]]))
/// ```
pub fn as_columns(
  query: Query(Row),
  conn: Connection,
) -> Result(Columnar, Error) {
  let params = list.reverse(query.params)
  use dynamic_params <- result.try(list.try_map(params, value_to_dynamic))

  use raw <- result.try(
    ffi.execute_query_columns(
      connection.native(conn.internal),
      query.statement,
      dynamic_params,
    )
    |> result.map_error(decode_nif_error),
  )

  use decoded <- result.try(
    list.try_map(raw, fn(col) {
      let #(name, values) = col
      use decoded_values <- result.map(list.try_map(values, decode_value))
      #(name, decoded_values)
    }),
  )

  let names = list.map(decoded, fn(col) { col.0 })
  let data = list.map(decoded, fn(col) { col.1 })
  Ok(Columnar(names: names, data: data))
}

/// Looks up a column by name. Returns None if not found.
///
/// ## Examples
///
/// ```gleam
/// ducky.column(cols, "name")
/// // => Some([Text("Alice"), Text("Bob")])
/// ```
pub fn column(columnar: Columnar, name: String) -> Option(List(Value)) {
  column_lookup(columnar.names, columnar.data, name)
}

fn column_lookup(
  names: List(String),
  data: List(List(Value)),
  target: String,
) -> Option(List(Value)) {
  case names, data {
    [], _ -> option.None
    _, [] -> option.None
    [n, ..], [col, ..] if n == target -> option.Some(col)
    [_, ..rest_names], [_, ..rest_data] ->
      column_lookup(rest_names, rest_data, target)
  }
}

fn decode_raw_row(raw_row: List(dynamic.Dynamic)) -> Result(Row, Error) {
  use values <- result.map(list.try_map(raw_row, decode_value))
  Row(values: values)
}

/// Raw microseconds since epoch.
/// NIF returns temporals as {type_tag, value} tuples.
pub fn timestamp_decoder() -> decode.Decoder(Int) {
  use value <- decode.subfield([1], decode.int)
  decode.success(value)
}

/// Raw days since epoch.
pub fn date_decoder() -> decode.Decoder(Int) {
  use value <- decode.subfield([1], decode.int)
  decode.success(value)
}

/// Raw microseconds since midnight.
pub fn time_decoder() -> decode.Decoder(Int) {
  use value <- decode.subfield([1], decode.int)
  decode.success(value)
}

/// Returns #(months, days, nanos). NIF sends {interval, m, d, n}.
pub fn interval_decoder() -> decode.Decoder(#(Int, Int, Int)) {
  use months <- decode.subfield([1], decode.int)
  use days <- decode.subfield([2], decode.int)
  use nanos <- decode.subfield([3], decode.int)
  decode.success(#(months, days, nanos))
}

/// Lossless string. Avoids floating-point representation issues.
pub fn decimal_decoder() -> decode.Decoder(String) {
  use value <- decode.subfield([1], decode.string)
  decode.success(value)
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

/// Creates an integer parameter value.
pub fn int(value: Int) -> Value {
  Integer(value)
}

/// Creates a float parameter value.
pub fn float(value: Float) -> Value {
  Double(value)
}

/// Creates a text parameter value.
pub fn text(value: String) -> Value {
  Text(value)
}

/// Creates a blob parameter value.
pub fn blob(value: BitArray) -> Value {
  Blob(value)
}

/// Creates a boolean parameter value.
pub fn bool(value: Bool) -> Value {
  Boolean(value)
}

/// Creates a null parameter value.
pub fn null() -> Value {
  Null
}

/// Creates a nullable parameter value.
///
/// ## Examples
///
/// ```gleam
/// nullable(int, Some(42))
/// // => Integer(42)
///
/// nullable(int, None)
/// // => Null
/// ```
pub fn nullable(inner: fn(a) -> Value, value: Option(a)) -> Value {
  case value {
    option.Some(v) -> inner(v)
    option.None -> Null
  }
}

/// Creates a timestamp parameter value (microseconds since Unix epoch).
pub fn timestamp(micros: Int) -> Value {
  Timestamp(micros)
}

/// Creates a date parameter value (days since Unix epoch).
pub fn date(days: Int) -> Value {
  Date(days)
}

/// Creates a time parameter value (microseconds since midnight).
pub fn time(micros: Int) -> Value {
  Time(micros)
}

/// Creates an interval parameter value.
pub fn interval(months months: Int, days days: Int, nanos nanos: Int) -> Value {
  Interval(months, days, nanos)
}

/// Creates a decimal parameter value from a string representation.
pub fn decimal(value: String) -> Value {
  Decimal(value)
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
    List(_) -> Error(UnsupportedParameterType("List"))
    Array(_) -> Error(UnsupportedParameterType("Array"))
    Map(_) -> Error(UnsupportedParameterType("Map"))
    Struct(_) -> Error(UnsupportedParameterType("Struct"))
    Union(_, _) -> Error(UnsupportedParameterType("Union"))
  }
}

/// Decodes a dynamic value from the NIF into a typed Value.
fn decode_value(dyn: dynamic.Dynamic) -> Result(Value, Error) {
  let classification = dynamic.classify(dyn)
  case classification {
    "Atom" | "Nil" -> Ok(Null)
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
      |> result.map_error(fn(_) {
        DatabaseError("Failed to decode value of type: " <> classification)
      })
    }
  }
}

/// Decodes a list with recursive element decoding.
fn decode_list(dyn: dynamic.Dynamic) -> Result(Value, Error) {
  let decoder = decode.list(decode.dynamic)

  case decode.run(dyn, decoder) {
    Ok(elements) -> {
      use decoded_elements <- result.map(list.try_map(elements, decode_value))
      List(decoded_elements)
    }
    Error(_) -> Error(DatabaseError("Failed to decode list value"))
  }
}

/// Decodes an Erlang map into a Struct with recursive value decoding.
fn decode_struct(dyn: dynamic.Dynamic) -> Result(Value, Error) {
  let decoder = decode.dict(decode.string, decode.dynamic)

  case decode.run(dyn, decoder) {
    Ok(fields) -> {
      let pairs = dict.to_list(fields)
      use decoded_pairs <- result.map(
        list.try_map(pairs, fn(pair) {
          let #(key, val) = pair
          use decoded_val <- result.map(decode_value(val))
          #(key, decoded_val)
        }),
      )
      Struct(dict.from_list(decoded_pairs))
    }
    Error(_) -> Error(DatabaseError("Failed to decode struct value"))
  }
}

/// Decodes tagged tuples sent as Erlang arrays for various types.
fn decode_tagged_tuple(dyn: dynamic.Dynamic) -> Result(Value, Error) {
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
        _ -> Error(DatabaseError("Unknown tagged tuple type: " <> tag))
      }
    }
    Error(_) -> Error(DatabaseError("Failed to decode tagged tuple"))
  }
}

/// Decodes a decimal tagged tuple {decimal, "string"}.
fn decode_decimal_value(dyn: dynamic.Dynamic) -> Result(Value, Error) {
  let decoder = {
    use value <- decode.subfield([1], decode.string)
    decode.success(value)
  }
  case decode.run(dyn, decoder) {
    Ok(value) -> Ok(Decimal(value))
    Error(_) -> Error(DatabaseError("Failed to decode decimal value"))
  }
}

/// Decodes an array tagged tuple {array, [elements]}.
fn decode_array_value(dyn: dynamic.Dynamic) -> Result(Value, Error) {
  let decoder = {
    use elements <- decode.subfield([1], decode.list(decode.dynamic))
    decode.success(elements)
  }
  case decode.run(dyn, decoder) {
    Ok(elements) -> {
      use decoded_elements <- result.map(list.try_map(elements, decode_value))
      Array(decoded_elements)
    }
    Error(_) -> Error(DatabaseError("Failed to decode array value"))
  }
}

/// Decodes a map tagged tuple {map, %{key => value}}.
fn decode_map_value(dyn: dynamic.Dynamic) -> Result(Value, Error) {
  let decoder = {
    use entries <- decode.subfield(
      [1],
      decode.dict(decode.string, decode.dynamic),
    )
    decode.success(entries)
  }
  case decode.run(dyn, decoder) {
    Ok(entries) -> {
      let pairs = dict.to_list(entries)
      use decoded_pairs <- result.map(
        list.try_map(pairs, fn(pair) {
          let #(key, val) = pair
          use decoded_val <- result.map(decode_value(val))
          #(key, decoded_val)
        }),
      )
      Map(dict.from_list(decoded_pairs))
    }
    Error(_) -> Error(DatabaseError("Failed to decode map value"))
  }
}

/// Decodes a union tagged tuple {union, tag_string, value}.
fn decode_union_value(dyn: dynamic.Dynamic) -> Result(Value, Error) {
  let decoder = {
    use tag <- decode.subfield([1], decode.string)
    use value <- decode.subfield([2], decode.dynamic)
    decode.success(#(tag, value))
  }
  case decode.run(dyn, decoder) {
    Ok(#(tag, value)) -> {
      use decoded_value <- result.map(decode_value(value))
      Union(tag: tag, value: decoded_value)
    }
    Error(_) -> Error(DatabaseError("Failed to decode union value"))
  }
}

/// Decodes temporal tagged tuples (timestamp, date, time).
fn decode_temporal_tuple(dyn: dynamic.Dynamic) -> Result(Value, Error) {
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
        "timestamp" -> Ok(Timestamp(value))
        "date" -> Ok(Date(value))
        "time" -> Ok(Time(value))
        _ -> Error(DatabaseError("Unknown temporal type: " <> tag))
      }
    Error(_) -> Error(DatabaseError("Failed to decode temporal value"))
  }
}

/// Decodes interval 4-tuple {interval, months, days, nanos}.
fn decode_interval_tuple(dyn: dynamic.Dynamic) -> Result(Value, Error) {
  let decoder = {
    use months <- decode.subfield([1], decode.int)
    use days <- decode.subfield([2], decode.int)
    use nanos <- decode.subfield([3], decode.int)
    decode.success(#(months, days, nanos))
  }

  case decode.run(dyn, decoder) {
    Ok(#(months, days, nanos)) -> Ok(Interval(months, days, nanos))
    Error(_) -> Error(DatabaseError("Failed to decode interval value"))
  }
}

fn error_from_tag(tag: String, message: String) -> Error {
  case tag {
    "connection_failed" -> ConnectionFailed(message)
    "query_syntax_error" -> QuerySyntaxError(message)
    "unsupported_parameter_type" -> UnsupportedParameterType(message)
    "statement_finalized" -> StatementFinalized
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
