//! DuckDB Native Implemented Function for Erlang/Elixir/Gleam.
//!
//! Provides native bindings to DuckDB through Rustler.

use duckdb::{Connection as DuckDBConnection, types::ValueRef};
use rustler::{Encoder, Env, NifResult, ResourceArc, Term};
use std::sync::Mutex;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        // Error atoms
        connection_failed,
        query_syntax_error,
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
        text,
        blob,
    }
}

/// Error type that can be returned to Erlang.
#[derive(Debug)]
pub enum DuckyError {
    ConnectionFailed(String),
    QuerySyntaxError(String),
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

/// Converts a DuckDB ValueRef to an Erlang term.
fn value_to_term<'a>(env: Env<'a>, value: ValueRef) -> NifResult<Term<'a>> {
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
        ValueRef::Struct(struct_array, idx) => encode_struct(env, struct_array, idx),
        other => {
            let type_name = format!("Unsupported ValueRef: {:?}", other);
            Err(rustler::Error::Term(Box::new(type_name)))
        }
    }
}

/// Encodes a DuckDB struct as an Erlang map with recursive field encoding.
fn encode_struct<'a>(
    env: Env<'a>,
    struct_array: &duckdb::arrow::array::StructArray,
    row_idx: usize,
) -> NifResult<Term<'a>> {
    use duckdb::arrow::array::{Array, AsArray};
    use duckdb::arrow::datatypes::DataType;
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

        // Check if this specific field is null
        if field.is_null(row_idx) {
            map = map.map_put(field_name.encode(env), atoms::null().encode(env))?;
            continue;
        }

        // Create the appropriate ValueRef variant based on child field type
        let child_value_ref = match field.data_type() {
            DataType::Boolean => {
                let arr = field.as_boolean();
                ValueRef::Boolean(arr.value(row_idx))
            }
            DataType::Int8 => {
                let arr = field.as_primitive::<duckdb::arrow::datatypes::Int8Type>();
                ValueRef::TinyInt(arr.value(row_idx))
            }
            DataType::Int16 => {
                let arr = field.as_primitive::<duckdb::arrow::datatypes::Int16Type>();
                ValueRef::SmallInt(arr.value(row_idx))
            }
            DataType::Int32 => {
                let arr = field.as_primitive::<duckdb::arrow::datatypes::Int32Type>();
                ValueRef::Int(arr.value(row_idx))
            }
            DataType::Int64 => {
                let arr = field.as_primitive::<duckdb::arrow::datatypes::Int64Type>();
                ValueRef::BigInt(arr.value(row_idx))
            }
            DataType::UInt8 => {
                let arr = field.as_primitive::<duckdb::arrow::datatypes::UInt8Type>();
                ValueRef::UTinyInt(arr.value(row_idx))
            }
            DataType::UInt16 => {
                let arr = field.as_primitive::<duckdb::arrow::datatypes::UInt16Type>();
                ValueRef::USmallInt(arr.value(row_idx))
            }
            DataType::UInt32 => {
                let arr = field.as_primitive::<duckdb::arrow::datatypes::UInt32Type>();
                ValueRef::UInt(arr.value(row_idx))
            }
            DataType::UInt64 => {
                let arr = field.as_primitive::<duckdb::arrow::datatypes::UInt64Type>();
                ValueRef::UBigInt(arr.value(row_idx))
            }
            DataType::Float32 => {
                let arr = field.as_primitive::<duckdb::arrow::datatypes::Float32Type>();
                ValueRef::Float(arr.value(row_idx))
            }
            DataType::Float64 => {
                let arr = field.as_primitive::<duckdb::arrow::datatypes::Float64Type>();
                ValueRef::Double(arr.value(row_idx))
            }
            DataType::Utf8 => {
                let arr = field.as_string::<i32>();
                ValueRef::Text(arr.value(row_idx).as_bytes())
            }
            DataType::Binary => {
                let arr = field.as_binary::<i32>();
                ValueRef::Blob(arr.value(row_idx))
            }
            DataType::Struct(_) => {
                let child_struct = field.as_struct();
                ValueRef::Struct(child_struct, row_idx)
            }
            _ => {
                // Unsupported child type, encode as null
                map = map.map_put(field_name.encode(env), atoms::null().encode(env))?;
                continue;
            }
        };

        let term_value = value_to_term(env, child_value_ref)?;
        map = map.map_put(field_name.encode(env), term_value)?;
    }

    Ok(map)
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

/// Converts an Erlang term to a DuckDB parameter.
///
/// Supports basic types: Int, Float, String, Bool, Null
fn term_to_duckdb_param(term: Term) -> Result<Box<dyn duckdb::types::ToSql>, DuckyError> {
    use duckdb::types::Null;
    use rustler::types::atom;

    // Try to decode as different types
    // Check for null/nil atoms first (Gleam's Nil maps to Erlang's nil atom)
    if let Ok(atom_val) = atom::Atom::from_term(term) {
        if atom_val == atoms::null() || atom_val == atoms::nil() {
            return Ok(Box::new(Null));
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

    Err(DuckyError::DatabaseError(
        "Unsupported parameter type: cannot convert term to DuckDB parameter".to_string(),
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
