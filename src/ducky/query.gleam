//// Query execution and result handling.

import ducky/connection.{type Connection}
import ducky/error.{type Error}
import ducky/internal/error_decoder
import ducky/internal/ffi
import ducky/types.{type DataFrame, type Value}
import gleam/dict
import gleam/dynamic
import gleam/dynamic/decode
import gleam/list
import gleam/result

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
  use dynamic_params <- result.try(list.try_map(params, value_to_dynamic))

  ffi.execute_query(connection.native(conn), sql, dynamic_params)
  |> result.map(decode_dataframe)
  |> result.map_error(error_decoder.decode_nif_error)
}

/// Decodes a dynamic value from the NIF into a typed Value.
fn decode_value(dyn: dynamic.Dynamic) -> Value {
  let classification = dynamic.classify(dyn)
  case classification {
    // SQL NULL is represented as Erlang's null atom
    "Atom" -> types.Null
    "Dict" -> decode_struct(dyn)
    "List" -> decode_list(dyn)
    "Array" -> decode_tagged_tuple(dyn)
    _ -> {
      let value_decoder =
        decode.one_of(decode.bool |> decode.map(types.Boolean), or: [
          decode.int |> decode.map(types.Integer),
          decode.float |> decode.map(types.Double),
          decode.string |> decode.map(types.Text),
          decode.bit_array |> decode.map(types.Blob),
        ])

      decode.run(dyn, value_decoder)
      |> result.unwrap(or: types.Null)
    }
  }
}

/// Decodes a list with recursive element decoding.
fn decode_list(dyn: dynamic.Dynamic) -> Value {
  let decoder = decode.list(decode.dynamic)

  case decode.run(dyn, decoder) {
    Ok(elements) -> {
      let decoded_elements = list.map(elements, decode_value)
      types.List(decoded_elements)
    }
    Error(_) -> types.Null
  }
}

/// Decodes an Erlang map into a Struct with recursive value decoding.
fn decode_struct(dyn: dynamic.Dynamic) -> Value {
  let decoder =
    decode.dict(decode.string, decode.dynamic)
    |> decode.map(fn(fields) {
      // Recursively decode each value in the struct
      let decoded_fields =
        fields
        |> dict.to_list
        |> list.map(fn(pair) {
          let #(key, val) = pair
          #(key, decode_value(val))
        })
        |> dict.from_list

      types.Struct(decoded_fields)
    })

  decode.run(dyn, decoder)
  |> result.unwrap(or: types.Null)
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
        "Atom" -> atom_to_string(tag_dynamic)
        _ -> ""
      }

      case tag {
        "decimal" -> decode_decimal_value(dyn)
        "array" -> decode_array_value(dyn)
        "map" -> decode_map_value(dyn)
        "timestamp" | "date" | "time" -> decode_temporal_tuple(dyn)
        "interval" -> decode_interval_tuple(dyn)
        _ -> types.Null
      }
    }
    Error(_) -> types.Null
  }
}

/// Decodes a decimal tagged tuple {decimal, "string"}.
fn decode_decimal_value(dyn: dynamic.Dynamic) -> Value {
  let decoder = {
    use value <- decode.subfield([1], decode.string)
    decode.success(value)
  }
  case decode.run(dyn, decoder) {
    Ok(value) -> types.Decimal(value)
    Error(_) -> types.Null
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
      types.Array(decoded_elements)
    }
    Error(_) -> types.Null
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
      types.Map(decoded_entries)
    }
    Error(_) -> types.Null
  }
}

/// Decodes temporal tagged tuples (timestamp, date, time).
fn decode_temporal_tuple(dyn: dynamic.Dynamic) -> Value {
  let decoder = {
    use tag_dynamic <- decode.subfield([0], decode.dynamic)
    use value <- decode.subfield([1], decode.int)

    let tag = case dynamic.classify(tag_dynamic) {
      "Atom" -> atom_to_string(tag_dynamic)
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
        "timestamp" -> types.Timestamp(value)
        "date" -> types.Date(value)
        "time" -> types.Time(value)
        _ -> types.Null
      }
    Error(_) -> types.Null
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
    Ok(#(months, days, nanos)) -> types.Interval(months, days, nanos)
    Error(_) -> types.Null
  }
}

/// Converts an Erlang atom to a String.
@external(erlang, "erlang", "atom_to_binary")
fn atom_to_string(atom: dynamic.Dynamic) -> String

/// Creates an atom from a string.
@external(erlang, "erlang", "binary_to_atom")
fn binary_to_atom(s: String) -> dynamic.Dynamic

/// Converts a list to a tuple.
@external(erlang, "erlang", "list_to_tuple")
fn list_to_tuple(list: List(dynamic.Dynamic)) -> dynamic.Dynamic

/// Creates a tagged tuple {tag, value} for NIF parameter encoding.
fn make_tagged(tag: String, value: dynamic.Dynamic) -> dynamic.Dynamic {
  list_to_tuple([binary_to_atom(tag), value])
}

/// Creates an interval 4-tuple {interval, months, days, nanos}.
fn make_interval_tuple(months: Int, days: Int, nanos: Int) -> dynamic.Dynamic {
  list_to_tuple([
    binary_to_atom("interval"),
    dynamic.int(months),
    dynamic.int(days),
    dynamic.int(nanos),
  ])
}

/// Converts a Value to a Dynamic for passing to the NIF.
fn value_to_dynamic(value: Value) -> Result(dynamic.Dynamic, Error) {
  case value {
    types.Null -> Ok(dynamic.nil())
    types.Boolean(b) -> Ok(dynamic.bool(b))
    types.TinyInt(i) -> Ok(dynamic.int(i))
    types.SmallInt(i) -> Ok(dynamic.int(i))
    types.Integer(i) -> Ok(dynamic.int(i))
    types.BigInt(i) -> Ok(dynamic.int(i))
    types.Float(f) -> Ok(dynamic.float(f))
    types.Double(f) -> Ok(dynamic.float(f))
    types.Text(s) -> Ok(dynamic.string(s))
    types.Blob(bits) -> Ok(dynamic.bit_array(bits))
    // Temporal types as tagged tuples {atom, int}
    types.Timestamp(micros) -> Ok(make_tagged("timestamp", dynamic.int(micros)))
    types.Date(days) -> Ok(make_tagged("date", dynamic.int(days)))
    types.Time(micros) -> Ok(make_tagged("time", dynamic.int(micros)))
    types.Interval(months, days, nanos) ->
      Ok(make_interval_tuple(months, days, nanos))
    // Decimal as tagged tuple {decimal, "string"}
    types.Decimal(s) -> Ok(make_tagged("decimal", dynamic.string(s)))
    // Complex types not yet supported as parameters
    types.List(_) | types.Array(_) | types.Map(_) | types.Struct(_) ->
      Error(error.UnsupportedParameterType(
        "List, Array, Map, and Struct types cannot be used as query parameters",
      ))
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
