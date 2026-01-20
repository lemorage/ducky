//// Type mappings between DuckDB and Gleam.

import gleam/dict.{type Dict}
import gleam/option.{type Option}

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
  Text(String)
  Blob(BitArray)
  Timestamp(Int)
  Date(Int)
  Time(Int)
  Interval(Int)
  List(List(Value))
  Struct(Dict(String, Value))
}

/// A single row from a query result.
pub type Row {
  Row(values: List(Value))
}

/// A complete query result with column metadata.
pub type DataFrame {
  DataFrame(columns: List(String), rows: List(Row))
}

/// Get a value from a row by column index.
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

fn list_at(list: List(a), index: Int) -> Option(a) {
  case list, index {
    [], _ -> option.None
    [first, ..], 0 -> option.Some(first)
    [_, ..rest], n if n > 0 -> list_at(rest, n - 1)
    _, _ -> option.None
  }
}
