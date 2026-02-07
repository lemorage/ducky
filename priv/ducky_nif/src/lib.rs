//! DuckDB Native Implemented Function for Erlang/Elixir/Gleam.
//!
//! Provides native bindings to DuckDB through Rustler.

use chrono::{NaiveDate, NaiveTime, Timelike};
use duckdb::{
    Connection as DuckDBConnection,
    arrow::array::Array,
    types::{TimeUnit, Value, ValueRef},
};
use rustler::{Encoder, Env, NifResult, ResourceArc, Term};
use std::sync::Mutex;

/// Microseconds per second.
const MICROS_PER_SEC: i64 = 1_000_000;
/// Microseconds per day (24 hours).
const MICROS_PER_DAY: i64 = 86_400_000_000;
/// Days from year 0 (CE) to Unix epoch (1970-01-01).
const DAYS_FROM_CE_TO_UNIX: i32 = 719_163;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        // Error atoms
        connection_failed,
        query_syntax_error,
        unsupported_parameter_type,
        database_error,
        nil,
        // Type atoms
        null,
        boolean,
        tiny_int,
        small_int,
        big_int,
        integer,
        float,
        double,
        decimal,
        text,
        blob,
        timestamp,
        date,
        time,
        interval,
        array,
        map,
        union,
    }
}

/// Error type that can be returned to Erlang.
#[derive(Debug)]
pub enum DuckyError {
    ConnectionFailed(String),
    QuerySyntaxError(String),
    UnsupportedParameterType(String),
    DatabaseError(String),
}

impl Encoder for DuckyError {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        let reason = match self {
            DuckyError::ConnectionFailed(msg) => {
                (atoms::connection_failed(), msg.as_str()).encode(env)
            }
            DuckyError::QuerySyntaxError(msg) => {
                (atoms::query_syntax_error(), msg.as_str()).encode(env)
            }
            DuckyError::UnsupportedParameterType(msg) => {
                (atoms::unsupported_parameter_type(), msg.as_str()).encode(env)
            }
            DuckyError::DatabaseError(msg) => (atoms::database_error(), msg.as_str()).encode(env),
        };
        (atoms::error(), reason).encode(env)
    }
}

impl From<duckdb::Error> for DuckyError {
    fn from(err: duckdb::Error) -> Self {
        DuckyError::DatabaseError(err.to_string())
    }
}

/// Resource wrapper for DuckDB connection with thread-safe access.
pub struct ConnectionResource {
    #[allow(dead_code)]
    connection: Mutex<DuckDBConnection>,
}

impl ConnectionResource {
    fn new(connection: DuckDBConnection) -> Self {
        Self {
            connection: Mutex::new(connection),
        }
    }
}

/// Opens a connection to a DuckDB database.
///
/// ## Arguments
/// - `path`: Database file path or `:memory:` for in-memory database
///
/// ## Returns
/// - `Ok(ResourceArc<ConnectionResource>)` on success
/// - `Err(DuckyError)` on failure
#[rustler::nif]
fn connect(path: String) -> Result<ResourceArc<ConnectionResource>, DuckyError> {
    let connection = if path == ":memory:" {
        DuckDBConnection::open_in_memory()
    } else {
        DuckDBConnection::open(&path)
    }
    .map_err(|e| DuckyError::ConnectionFailed(e.to_string()))?;

    Ok(ResourceArc::new(ConnectionResource::new(connection)))
}

/// Closes a database connection.
///
/// ## Arguments
/// - `conn`: Connection resource to close
///
/// ## Returns
/// - `Ok(())` on success
/// - `Err(DuckyError)` if close fails
#[rustler::nif]
fn close(conn: ResourceArc<ConnectionResource>) -> Result<rustler::Atom, DuckyError> {
    // Attempt to lock the connection
    let _guard = conn
        .connection
        .lock()
        .map_err(|e| DuckyError::DatabaseError(format!("Connection mutex poisoned: {}", e)))?;

    // DuckDB connections are closed via Drop when ResourceArc is dropped
    // We just validate the connection is accessible before allowing the drop
    drop(_guard);
    Ok(atoms::nil())
}

/// Executes a SQL query with optional parameter binding.
///
/// Runs on a dirty CPU scheduler to avoid blocking the BEAM VM.
///
/// Handles both result-returning queries (SELECT, SHOW, etc.) and
/// non-result statements (CREATE, INSERT, UPDATE, DELETE, etc.).
///
/// ## Arguments
/// - `env`: NIF environment for term creation
/// - `conn`: Connection resource
/// - `sql`: SQL query string with optional `?` placeholders
/// - `params_list`: Parameter values to bind (empty for non-parameterized queries)
///
/// ## Returns
/// - `Ok({columns, rows})` where columns is a list of column names
///   and rows is a list of rows (each row is a list of values)
/// - For DDL/DML statements, returns empty columns and rows
/// - `Err(DuckyError)` on failure
#[rustler::nif(schedule = "DirtyCpu")]
fn execute_query<'a>(
    env: Env<'a>,
    conn: ResourceArc<ConnectionResource>,
    sql: String,
    params_list: Vec<Term<'a>>,
) -> Result<(Vec<String>, Vec<Vec<Term<'a>>>), DuckyError> {
    use duckdb::types::ToSql;

    let connection = conn
        .connection
        .lock()
        .map_err(|e| DuckyError::DatabaseError(format!("Failed to lock connection: {}", e)))?;

    // Convert Erlang terms to DuckDB params
    let mut params: Vec<Box<dyn ToSql>> = Vec::new();
    for term in params_list {
        let param = term_to_duckdb_param(term)?;
        params.push(param);
    }

    // Create references for binding
    let param_refs: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();

    execute_statement(env, &connection, &sql, param_refs.as_slice())
}

/// Converts Arrow TimeUnit to DuckDB TimeUnit.
fn arrow_to_duckdb_time_unit(
    arrow_unit: duckdb::arrow::datatypes::TimeUnit,
) -> duckdb::types::TimeUnit {
    use duckdb::arrow::datatypes::TimeUnit as ArrowUnit;
    use duckdb::types::TimeUnit as DuckUnit;
    match arrow_unit {
        ArrowUnit::Second => DuckUnit::Second,
        ArrowUnit::Millisecond => DuckUnit::Millisecond,
        ArrowUnit::Microsecond => DuckUnit::Microsecond,
        ArrowUnit::Nanosecond => DuckUnit::Nanosecond,
    }
}

/// Normalizes a temporal value to microseconds based on TimeUnit.
fn normalize_to_micros(time_unit: duckdb::types::TimeUnit, value: i64) -> i64 {
    use duckdb::types::TimeUnit;
    match time_unit {
        TimeUnit::Second => value * 1_000_000,
        TimeUnit::Millisecond => value * 1_000,
        TimeUnit::Microsecond => value,
        TimeUnit::Nanosecond => value / 1_000,
    }
}

/// Converts a DuckDB ValueRef to an Erlang term.
fn value_to_term<'a, 'b>(env: Env<'a>, value: ValueRef<'b>) -> NifResult<Term<'a>> {
    match value {
        ValueRef::Null => Ok(atoms::null().encode(env)),
        ValueRef::Boolean(b) => Ok(b.encode(env)),
        ValueRef::TinyInt(i) => Ok(i.encode(env)),
        ValueRef::SmallInt(i) => Ok(i.encode(env)),
        ValueRef::Int(i) => Ok(i.encode(env)),
        ValueRef::BigInt(i) => Ok(i.encode(env)),
        ValueRef::HugeInt(i) => Ok(i.encode(env)),
        ValueRef::UTinyInt(i) => Ok((i as i32).encode(env)),
        ValueRef::USmallInt(i) => Ok((i as i32).encode(env)),
        ValueRef::UInt(i) => Ok((i as i64).encode(env)),
        ValueRef::UBigInt(i) => match i64::try_from(i) {
            Ok(signed) => Ok(signed.encode(env)),
            Err(_) => Err(rustler::Error::Term(Box::new(format!(
                "Integer overflow: UBigInt value {} exceeds i64::MAX ({})",
                i,
                i64::MAX
            )))),
        },
        ValueRef::Float(f) => Ok(f.encode(env)),
        ValueRef::Double(f) => Ok(f.encode(env)),
        ValueRef::Text(s) => {
            let text = std::str::from_utf8(s)
                .map_err(|_| rustler::Error::Term(Box::new("Invalid UTF-8")))?;
            Ok(text.encode(env))
        }
        ValueRef::Blob(b) => Ok(b.encode(env)),
        ValueRef::Timestamp(time_unit, value) => {
            let micros = normalize_to_micros(time_unit, value);
            Ok((atoms::timestamp(), micros).encode(env))
        }
        ValueRef::Date32(days) => Ok((atoms::date(), days).encode(env)),
        ValueRef::Time64(time_unit, value) => {
            let micros = normalize_to_micros(time_unit, value);
            Ok((atoms::time(), micros).encode(env))
        }
        ValueRef::Interval {
            months,
            days,
            nanos,
        } => Ok((atoms::interval(), months, days, nanos).encode(env)),
        ValueRef::Struct(struct_array, idx) => encode_struct(env, struct_array, idx),
        ValueRef::List(list_type, row_idx) => encode_list(env, list_type, row_idx),
        ValueRef::Decimal(dec) => Ok((atoms::decimal(), dec.to_string()).encode(env)),
        ValueRef::Enum(enum_type, row_idx) => match encode_enum(enum_type, row_idx) {
            Some(s) => Ok(s.encode(env)),
            None => Ok(atoms::null().encode(env)),
        },
        ValueRef::Array(array, row_idx) => encode_array(env, array, row_idx),
        ValueRef::Map(map_array, row_idx) => encode_map(env, map_array, row_idx),
        ValueRef::Union(union_ref, row_idx) => encode_union(env, union_ref, row_idx),
    }
}

/// Converts an Arrow array element to a DuckDB ValueRef.
fn arrow_element_to_value_ref<'b>(
    array: &'b dyn duckdb::arrow::array::Array,
    elem_idx: usize,
) -> Result<ValueRef<'b>, String> {
    use duckdb::arrow::array::AsArray;
    use duckdb::arrow::datatypes::DataType;
    use duckdb::types::ListType;

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_boolean();
            Ok(ValueRef::Boolean(arr.value(elem_idx)))
        }
        DataType::Int8 => {
            let arr = array.as_primitive::<duckdb::arrow::datatypes::Int8Type>();
            Ok(ValueRef::TinyInt(arr.value(elem_idx)))
        }
        DataType::Int16 => {
            let arr = array.as_primitive::<duckdb::arrow::datatypes::Int16Type>();
            Ok(ValueRef::SmallInt(arr.value(elem_idx)))
        }
        DataType::Int32 => {
            let arr = array.as_primitive::<duckdb::arrow::datatypes::Int32Type>();
            Ok(ValueRef::Int(arr.value(elem_idx)))
        }
        DataType::Int64 => {
            let arr = array.as_primitive::<duckdb::arrow::datatypes::Int64Type>();
            Ok(ValueRef::BigInt(arr.value(elem_idx)))
        }
        DataType::UInt8 => {
            let arr = array.as_primitive::<duckdb::arrow::datatypes::UInt8Type>();
            Ok(ValueRef::UTinyInt(arr.value(elem_idx)))
        }
        DataType::UInt16 => {
            let arr = array.as_primitive::<duckdb::arrow::datatypes::UInt16Type>();
            Ok(ValueRef::USmallInt(arr.value(elem_idx)))
        }
        DataType::UInt32 => {
            let arr = array.as_primitive::<duckdb::arrow::datatypes::UInt32Type>();
            Ok(ValueRef::UInt(arr.value(elem_idx)))
        }
        DataType::UInt64 => {
            let arr = array.as_primitive::<duckdb::arrow::datatypes::UInt64Type>();
            Ok(ValueRef::UBigInt(arr.value(elem_idx)))
        }
        DataType::Float32 => {
            let arr = array.as_primitive::<duckdb::arrow::datatypes::Float32Type>();
            Ok(ValueRef::Float(arr.value(elem_idx)))
        }
        DataType::Float64 => {
            let arr = array.as_primitive::<duckdb::arrow::datatypes::Float64Type>();
            Ok(ValueRef::Double(arr.value(elem_idx)))
        }
        DataType::Utf8 => {
            let arr = array.as_string::<i32>();
            Ok(ValueRef::Text(arr.value(elem_idx).as_bytes()))
        }
        DataType::Binary => {
            let arr = array.as_binary::<i32>();
            Ok(ValueRef::Blob(arr.value(elem_idx)))
        }
        DataType::Struct(_) => {
            let child_struct = array.as_struct();
            Ok(ValueRef::Struct(child_struct, elem_idx))
        }
        DataType::List(_) => {
            let child_list = array.as_list();
            Ok(ValueRef::List(ListType::Regular(child_list), elem_idx))
        }
        DataType::Timestamp(time_unit, _) => {
            let arr = array.as_primitive::<duckdb::arrow::datatypes::TimestampMicrosecondType>();
            let duckdb_unit = arrow_to_duckdb_time_unit(*time_unit);
            Ok(ValueRef::Timestamp(duckdb_unit, arr.value(elem_idx)))
        }
        DataType::Date32 => {
            let arr = array.as_primitive::<duckdb::arrow::datatypes::Date32Type>();
            Ok(ValueRef::Date32(arr.value(elem_idx)))
        }
        DataType::Time64(time_unit) => {
            let arr = array.as_primitive::<duckdb::arrow::datatypes::Time64MicrosecondType>();
            let duckdb_unit = arrow_to_duckdb_time_unit(*time_unit);
            Ok(ValueRef::Time64(duckdb_unit, arr.value(elem_idx)))
        }
        DataType::Interval(_) => {
            let arr = array.as_primitive::<duckdb::arrow::datatypes::IntervalMonthDayNanoType>();
            let interval = arr.value(elem_idx);
            Ok(ValueRef::Interval {
                months: interval.months,
                days: interval.days,
                nanos: interval.nanoseconds,
            })
        }
        unsupported_type => Err(format!(
            "Unsupported list element type: {:?}",
            unsupported_type
        )),
    }
}

/// Encodes a DuckDB list as an Erlang list with recursive element encoding.
fn encode_list<'a, 'b>(
    env: Env<'a>,
    list_type: duckdb::types::ListType<'b>,
    row_idx: usize,
) -> NifResult<Term<'a>> {
    use duckdb::types::ListType;

    let (start, end, values_array) = match list_type {
        ListType::Regular(list_array) => {
            if list_array.is_null(row_idx) {
                return Ok(atoms::null().encode(env));
            }
            let offsets = list_array.value_offsets();
            (
                offsets[row_idx] as usize,
                offsets[row_idx + 1] as usize,
                list_array.values(),
            )
        }
        ListType::Large(large_list_array) => {
            if large_list_array.is_null(row_idx) {
                return Ok(atoms::null().encode(env));
            }
            let offsets = large_list_array.value_offsets();
            (
                offsets[row_idx] as usize,
                offsets[row_idx + 1] as usize,
                large_list_array.values(),
            )
        }
    };

    let mut elements = Vec::new();
    for elem_idx in start..end {
        if values_array.is_null(elem_idx) {
            elements.push(atoms::null().encode(env));
        } else {
            let value_ref = arrow_element_to_value_ref(values_array.as_ref(), elem_idx)
                .map_err(|e| rustler::Error::Term(Box::new(e)))?;
            let term = value_to_term(env, value_ref)?;
            elements.push(term);
        }
    }

    Ok(elements.encode(env))
}

/// Encodes a DuckDB struct as an Erlang map with recursive field encoding.
fn encode_struct<'a>(
    env: Env<'a>,
    struct_array: &duckdb::arrow::array::StructArray,
    row_idx: usize,
) -> NifResult<Term<'a>> {
    use rustler::types::map::map_new;

    let mut map = map_new(env);

    // Iterate over struct fields
    for (field_idx, field) in struct_array.columns().iter().enumerate() {
        // Get field name from schema
        let field_name = struct_array
            .fields()
            .get(field_idx)
            .map(|f| f.name().as_str())
            .unwrap_or("unknown");

        if field.is_null(row_idx) {
            map = map.map_put(field_name.encode(env), atoms::null().encode(env))?;
            continue;
        }

        match arrow_element_to_value_ref(field.as_ref(), row_idx) {
            Ok(value_ref) => {
                let term_value = value_to_term(env, value_ref)?;
                map = map.map_put(field_name.encode(env), term_value)?;
            }
            Err(_) => {
                map = map.map_put(field_name.encode(env), atoms::null().encode(env))?;
            }
        }
    }

    Ok(map)
}

/// Encodes a DuckDB enum value as a String.
fn encode_enum(enum_type: duckdb::types::EnumType<'_>, i: usize) -> Option<String> {
    use duckdb::arrow::array::AsArray;
    use duckdb::types::EnumType::*;

    match enum_type {
        UInt8(a) if !a.is_null(i) => {
            Some(a.values().as_string::<i32>().value(a.key(i)? as _).into())
        }
        UInt16(a) if !a.is_null(i) => {
            Some(a.values().as_string::<i32>().value(a.key(i)? as _).into())
        }
        UInt32(a) if !a.is_null(i) => {
            Some(a.values().as_string::<i32>().value(a.key(i)? as _).into())
        }
        _ => None,
    }
}

/// Encodes a DuckDB fixed-size array as an Erlang tagged tuple {array, [elements]}.
fn encode_array<'a>(
    env: Env<'a>,
    array: &duckdb::arrow::array::FixedSizeListArray,
    row_idx: usize,
) -> NifResult<Term<'a>> {
    use duckdb::arrow::array::Array;

    if array.is_null(row_idx) {
        return Ok(atoms::null().encode(env));
    }

    let values = array.value(row_idx);
    let mut elements = Vec::new();

    for elem_idx in 0..values.len() {
        if values.is_null(elem_idx) {
            elements.push(atoms::null().encode(env));
        } else {
            let value_ref = arrow_element_to_value_ref(values.as_ref(), elem_idx)
                .map_err(|e| rustler::Error::Term(Box::new(e)))?;
            let term = value_to_term(env, value_ref)?;
            elements.push(term);
        }
    }

    Ok((atoms::array(), elements).encode(env))
}

/// Encodes a DuckDB map as an Erlang tagged tuple {map, %{key => value}}.
fn encode_map<'a>(
    env: Env<'a>,
    map_array: &duckdb::arrow::array::MapArray,
    row_idx: usize,
) -> NifResult<Term<'a>> {
    use duckdb::arrow::array::Array;
    use rustler::types::map::map_new;

    if map_array.is_null(row_idx) {
        return Ok(atoms::null().encode(env));
    }

    let keys = map_array.keys();
    let values = map_array.values();
    let offsets = map_array.value_offsets();
    let start = offsets[row_idx] as usize;
    let end = offsets[row_idx + 1] as usize;

    let mut result_map = map_new(env);

    for idx in start..end {
        // Get key as string
        let key_str = if keys.is_null(idx) {
            "null".to_string()
        } else {
            let key_ref = arrow_element_to_value_ref(keys.as_ref(), idx)
                .map_err(|e| rustler::Error::Term(Box::new(e)))?;
            value_to_string(key_ref)
        };

        // Get value
        let value_term = if values.is_null(idx) {
            atoms::null().encode(env)
        } else {
            let value_ref = arrow_element_to_value_ref(values.as_ref(), idx)
                .map_err(|e| rustler::Error::Term(Box::new(e)))?;
            value_to_term(env, value_ref)?
        };

        result_map = result_map.map_put(key_str.encode(env), value_term)?;
    }

    Ok((atoms::map(), result_map).encode(env))
}

/// Encodes a DuckDB union as an Erlang tagged tuple {union, tag_string, value}.
fn encode_union<'a>(
    env: Env<'a>,
    union_ref: &duckdb::arrow::array::ArrayRef,
    row_idx: usize,
) -> NifResult<Term<'a>> {
    use duckdb::arrow::array::Array;
    use duckdb::arrow::datatypes::{DataType, UnionMode};

    let union_array = union_ref
        .as_any()
        .downcast_ref::<duckdb::arrow::array::UnionArray>()
        .ok_or_else(|| rustler::Error::Term(Box::new("Failed to downcast to UnionArray")))?;

    if union_array.is_null(row_idx) {
        return Ok(atoms::null().encode(env));
    }

    let type_id = union_array.type_id(row_idx);

    let (fields, mode) = match union_array.data_type() {
        DataType::Union(fields, mode) => (fields, mode),
        _ => {
            return Err(rustler::Error::Term(Box::new(
                "Union array has non-union data type",
            )));
        }
    };

    let (_, field) = fields
        .iter()
        .find(|(tid, _)| *tid == type_id)
        .ok_or_else(|| rustler::Error::Term(Box::new("Invalid union type_id")))?;

    let child_idx = match mode {
        UnionMode::Dense => union_array.value_offset(row_idx),
        UnionMode::Sparse => row_idx,
    };
    let child_array = union_array.child(type_id);
    let value_term = if child_array.is_null(child_idx) {
        atoms::null().encode(env)
    } else {
        match arrow_element_to_value_ref(child_array.as_ref(), child_idx) {
            Ok(value_ref) => value_to_term(env, value_ref)?,
            Err(e) => return Err(rustler::Error::Term(Box::new(e))),
        }
    };

    Ok((atoms::union(), field.name(), value_term).encode(env))
}

/// Converts a ValueRef to a string representation (for map keys).
fn value_to_string(value: ValueRef<'_>) -> String {
    match value {
        ValueRef::Null => "null".to_string(),
        ValueRef::Boolean(b) => b.to_string(),
        ValueRef::TinyInt(i) => i.to_string(),
        ValueRef::SmallInt(i) => i.to_string(),
        ValueRef::Int(i) => i.to_string(),
        ValueRef::BigInt(i) => i.to_string(),
        ValueRef::HugeInt(i) => i.to_string(),
        ValueRef::UTinyInt(i) => i.to_string(),
        ValueRef::USmallInt(i) => i.to_string(),
        ValueRef::UInt(i) => i.to_string(),
        ValueRef::UBigInt(i) => i.to_string(),
        ValueRef::Float(f) => f.to_string(),
        ValueRef::Double(f) => f.to_string(),
        ValueRef::Decimal(d) => d.to_string(),
        ValueRef::Text(s) => std::str::from_utf8(s).unwrap_or("").to_string(),
        _ => format!("{:?}", value),
    }
}

/// Core statement execution logic for all queries.
fn execute_statement<'a>(
    env: Env<'a>,
    connection: &DuckDBConnection,
    sql: &str,
    params: &[&dyn duckdb::types::ToSql],
) -> Result<(Vec<String>, Vec<Vec<Term<'a>>>), DuckyError> {
    let mut stmt = connection.prepare(sql)?;

    // Try executing as a query
    // DuckDB will return an error if it's not a result-returning statement
    match stmt.query(params) {
        Ok(mut rows_result) => {
            // This is a result-returning statement
            let mut raw_rows = Vec::new();
            let mut detected_column_count = 0;

            while let Some(row) = rows_result.next()? {
                if detected_column_count == 0 {
                    detected_column_count = row.as_ref().column_count();
                }

                let mut row_values = Vec::new();
                for i in 0..detected_column_count {
                    let value = row.get_ref(i)?;
                    let term = value_to_term(env, value).map_err(|_| {
                        DuckyError::DatabaseError("Failed to convert value".to_string())
                    })?;
                    row_values.push(term);
                }

                raw_rows.push(row_values);
            }

            // Get column names after consuming rows
            let column_names: Vec<String> = (0..detected_column_count)
                .filter_map(|i| stmt.column_name(i).ok().map(|s| s.to_string()))
                .collect();

            Ok((column_names, raw_rows))
        }
        Err(_) => {
            // Not a query, try executing as DDL/DML statement
            stmt.execute(params)?;
            Ok((Vec::new(), Vec::new()))
        }
    }
}

/// Converts days since Unix epoch to ISO date string (YYYY-MM-DD).
fn days_to_iso_date(days: i32) -> String {
    let days_from_ce = days + DAYS_FROM_CE_TO_UNIX;
    NaiveDate::from_num_days_from_ce_opt(days_from_ce)
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| format!("INVALID_DATE(days={})", days))
}

/// Converts microseconds since midnight to ISO time string (HH:MM:SS[.ffffff]).
fn micros_to_iso_time(micros: i64) -> String {
    // Normalize to valid range [0, MICROS_PER_DAY)
    let normalized = micros.rem_euclid(MICROS_PER_DAY) as u64;
    let secs = (normalized / MICROS_PER_SEC as u64) as u32;
    let nanos = ((normalized % MICROS_PER_SEC as u64) * 1000) as u32;

    NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
        .map(|t| {
            if t.nanosecond() > 0 {
                t.format("%H:%M:%S%.6f").to_string()
            } else {
                t.format("%H:%M:%S").to_string()
            }
        })
        .unwrap_or_else(|| format!("INVALID_TIME(micros={})", micros))
}

/// Converts an Erlang term to a DuckDB parameter.
///
/// Supports: Int, Float, String, Bool, Null, and temporal types via tagged tuples.
fn term_to_duckdb_param(term: Term) -> Result<Box<dyn duckdb::types::ToSql>, DuckyError> {
    use duckdb::types::Null;
    use rustler::types::atom;

    // Check for null/nil atoms first (Gleam's Nil maps to Erlang's nil atom)
    if let Ok(atom_val) = atom::Atom::from_term(term) {
        if atom_val == atoms::null() || atom_val == atoms::nil() {
            return Ok(Box::new(Null));
        }
    }

    // Check for Interval 4-tuple {interval, months, days, nanos}
    if let Ok((tag_term, months_term, days_term, nanos_term)) =
        term.decode::<(Term, Term, Term, Term)>()
    {
        if let Ok(tag_atom) = atom::Atom::from_term(tag_term) {
            if tag_atom == atoms::interval() {
                if let (Ok(months), Ok(days), Ok(nanos)) = (
                    months_term.decode::<i32>(),
                    days_term.decode::<i32>(),
                    nanos_term.decode::<i64>(),
                ) {
                    return Ok(Box::new(Value::Interval {
                        months,
                        days,
                        nanos,
                    }));
                }
            }
        }
    }

    // Check for tagged tuples {atom, value} for temporal and decimal types
    if let Ok((tag_term, value_term)) = term.decode::<(Term, Term)>() {
        if let Ok(tag_atom) = atom::Atom::from_term(tag_term) {
            // Timestamp: {timestamp, micros} -> proper TIMESTAMP binding
            if tag_atom == atoms::timestamp() {
                if let Ok(micros) = value_term.decode::<i64>() {
                    return Ok(Box::new(Value::Timestamp(TimeUnit::Microsecond, micros)));
                }
            }
            // Date: {date, days} -> ISO string (duckdb-rs lacks Date32 statement binding)
            if tag_atom == atoms::date() {
                if let Ok(days) = value_term.decode::<i32>() {
                    return Ok(Box::new(days_to_iso_date(days)));
                }
            }
            // Time: {time, micros} -> ISO string (duckdb-rs lacks Time64 statement binding)
            if tag_atom == atoms::time() {
                if let Ok(micros) = value_term.decode::<i64>() {
                    return Ok(Box::new(micros_to_iso_time(micros)));
                }
            }
            // Decimal: {decimal, "string"} -> string (DuckDB casts to DECIMAL)
            if tag_atom == atoms::decimal() {
                if let Ok(s) = value_term.decode::<String>() {
                    return Ok(Box::new(s));
                }
            }
        }
    }

    if let Ok(b) = term.decode::<bool>() {
        return Ok(Box::new(b));
    }

    if let Ok(i) = term.decode::<i64>() {
        return Ok(Box::new(i));
    }

    if let Ok(f) = term.decode::<f64>() {
        return Ok(Box::new(f));
    }

    if let Ok(s) = term.decode::<String>() {
        return Ok(Box::new(s));
    }

    Err(DuckyError::UnsupportedParameterType(
        "Cannot convert term to DuckDB parameter".to_string(),
    ))
}

/// Health check NIF to verify the library loads correctly.
#[rustler::nif]
fn test() -> String {
    "DuckDB NIF loaded successfully!".to_string()
}

/// Initialize the NIF module and register all functions.
fn on_load(env: Env, _: Term) -> bool {
    #[allow(non_local_definitions)]
    {
        let _ = rustler::resource!(ConnectionResource, env);
    }
    true
}

rustler::init!("ducky_nif", load = on_load);
