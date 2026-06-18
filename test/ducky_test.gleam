import ducky
import gleam/dict
import gleam/dynamic/decode
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import gleeunit
import gleeunit/should

pub fn main() {
  gleeunit.main()
}

pub fn connect_memory_database_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  ducky.path(conn) |> should.equal(":memory:")
}

pub fn connect_empty_path_test() {
  ducky.connect("")
  |> should.be_error
}

pub fn path_memory_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  ducky.path(conn) |> should.equal(":memory:")
}

pub fn close_connection_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  ducky.close(conn)
  |> should.be_ok
}

pub fn run_empty_sql_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  ducky.sql("")
  |> ducky.run(conn)
  |> should.be_error
}

pub fn create_table_then_select_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) = ducky.sql("CREATE TABLE t (id INT)") |> ducky.run(conn)

  let assert Ok(result) = ducky.sql("SELECT * FROM t") |> ducky.run(conn)
  result.rows |> should.equal([])
}

pub fn insert_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) = ducky.sql("CREATE TABLE t (id INT)") |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO t VALUES (1), (2)") |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("SELECT * FROM t ORDER BY id") |> ducky.run(conn)
  list.length(result.rows) |> should.equal(2)
}

pub fn invalid_sql_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  case ducky.sql("SELEKT") |> ducky.run(conn) {
    Error(ducky.QuerySyntaxError(_)) -> True
    Error(ducky.DatabaseError(_)) -> True
    _ -> False
  }
  |> should.be_true
}

pub fn param_constructors_roundtrip_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE t (i INT, f DOUBLE, s VARCHAR, b BOOLEAN)")
    |> ducky.run(conn)

  let assert Ok(_) =
    ducky.sql("INSERT INTO t VALUES (?, ?, ?, ?)")
    |> ducky.parameters([
      ducky.int(42),
      ducky.float(3.14),
      ducky.text("hello"),
      ducky.bool(True),
    ])
    |> ducky.run(conn)

  let assert Ok(result) = ducky.sql("SELECT * FROM t") |> ducky.run(conn)
  let assert [
    ducky.Row([
      ducky.Integer(42),
      ducky.Double(f),
      ducky.Text("hello"),
      ducky.Boolean(True),
    ]),
  ] = result.rows

  should.be_true(f >. 3.0 && f <. 4.0)
}

pub fn param_null_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) = ducky.sql("CREATE TABLE t (v INT)") |> ducky.run(conn)

  let assert Ok(_) =
    ducky.sql("INSERT INTO t VALUES (?)")
    |> ducky.parameters([ducky.null()])
    |> ducky.run(conn)

  let assert Ok(result) = ducky.sql("SELECT * FROM t") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Null])] = result.rows
}

pub fn param_nullable_some_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) = ducky.sql("CREATE TABLE t (v INT)") |> ducky.run(conn)

  let assert Ok(_) =
    ducky.sql("INSERT INTO t VALUES (?)")
    |> ducky.parameters([ducky.nullable(ducky.int, option.Some(42))])
    |> ducky.run(conn)

  let assert Ok(result) = ducky.sql("SELECT * FROM t") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Integer(42)])] = result.rows
}

pub fn param_nullable_none_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) = ducky.sql("CREATE TABLE t (v INT)") |> ducky.run(conn)

  let assert Ok(_) =
    ducky.sql("INSERT INTO t VALUES (?)")
    |> ducky.parameters([ducky.nullable(ducky.int, option.None)])
    |> ducky.run(conn)

  let assert Ok(result) = ducky.sql("SELECT * FROM t") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Null])] = result.rows
}

pub fn param_temporal_constructors_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE t (ts TIMESTAMP, d DATE, t TIME)")
    |> ducky.run(conn)

  let micros = 1_705_315_845_000_000
  let days = 19_738
  let time_micros = 43_200_000_000

  let assert Ok(_) =
    ducky.sql("INSERT INTO t VALUES (?, ?, ?)")
    |> ducky.parameters([
      ducky.timestamp(micros),
      ducky.date(days),
      ducky.time(time_micros),
    ])
    |> ducky.run(conn)

  let assert Ok(result) = ducky.sql("SELECT * FROM t") |> ducky.run(conn)
  let assert [
    ducky.Row([ducky.Timestamp(ret_ts), ducky.Date(ret_d), ducky.Time(ret_t)]),
  ] = result.rows

  ret_ts |> should.equal(micros)
  ret_d |> should.equal(days)
  ret_t |> should.equal(time_micros)
}

pub fn param_decimal_constructor_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE t (v DECIMAL(18,6))") |> ducky.run(conn)

  let assert Ok(_) =
    ducky.sql("INSERT INTO t VALUES (?)")
    |> ducky.parameters([ducky.decimal("123.456789")])
    |> ducky.run(conn)

  let assert Ok(result) = ducky.sql("SELECT * FROM t") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Decimal(d)])] = result.rows
  string.contains(d, "123.456") |> should.be_true
}

pub fn param_interval_constructor_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) = ducky.sql("CREATE TABLE t (v INTERVAL)") |> ducky.run(conn)

  let assert Ok(_) =
    ducky.sql("INSERT INTO t VALUES (?)")
    |> ducky.parameters([
      ducky.interval(months: 1, days: 2, nanos: 10_800_000_000_000),
    ])
    |> ducky.run(conn)

  let assert Ok(result) = ducky.sql("SELECT * FROM t") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Interval(1, 2, nanos)])] = result.rows
  nanos |> should.equal(10_800_000_000_000)
}

pub fn select_simple_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql("SELECT 42 as answer, 'hello' as greeting")
    |> ducky.run(conn)

  result.rows
  |> should.not_equal([])
}

pub fn create_table_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR)")
    |> ducky.run(conn)
  result.rows |> should.equal([])
}

pub fn insert_and_select_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR)") |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
    |> ducky.run(conn)
  let assert Ok(result) =
    ducky.sql("SELECT * FROM users ORDER BY id") |> ducky.run(conn)

  result.rows
  |> list.length
  |> should.equal(2)
}

pub fn as_columns_select_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql(
      "SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS users(id, name)",
    )
    |> ducky.as_columns(conn)

  result.names |> should.equal(["id", "name"])
  let assert [
    [ducky.Integer(1), ducky.Integer(2)],
    [ducky.Text("Alice"), ducky.Text("Bob")],
  ] = result.data
}

pub fn run_and_as_columns_shapes_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let sql = "SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS users(id, name)"

  let assert Ok(row_result) = ducky.sql(sql) |> ducky.run(conn)
  let assert Ok(column_result) = ducky.sql(sql) |> ducky.as_columns(conn)

  let assert [
    ducky.Row([ducky.Integer(1), ducky.Text("Alice")]),
    ducky.Row([ducky.Integer(2), ducky.Text("Bob")]),
  ] = row_result.rows

  column_result.names |> should.equal(["id", "name"])
  let assert [
    [ducky.Integer(1), ducky.Integer(2)],
    [ducky.Text("Alice"), ducky.Text("Bob")],
  ] = column_result.data
}

pub fn as_columns_exposes_whole_column_values_directly_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let sql =
    "SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Eve')) AS users(id, name)"

  let assert Ok(row_result) = ducky.sql(sql) |> ducky.run(conn)
  let row_ids =
    row_result.rows
    |> list.map(fn(row) {
      case ducky.get(row, 0) {
        option.Some(ducky.Integer(id)) -> id
        _ -> -1
      }
    })

  let assert Ok(column_result) = ducky.sql(sql) |> ducky.as_columns(conn)
  let assert [column_ids, _names] = column_result.data

  row_ids |> should.equal([1, 2, 3])
  column_ids
  |> should.equal([ducky.Integer(1), ducky.Integer(2), ducky.Integer(3)])
}

pub fn as_columns_empty_select_keeps_columns_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR)") |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("SELECT * FROM users") |> ducky.as_columns(conn)

  result.names |> should.equal(["id", "name"])
  result.data |> should.equal([[], []])
}

pub fn as_columns_empty_count_select_keeps_column_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql("SELECT 1 AS Count WHERE false") |> ducky.as_columns(conn)

  result.names |> should.equal(["Count"])
  let assert [[]] = result.data
}

pub fn as_columns_non_result_statement_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql("CREATE TABLE t (id INT)") |> ducky.as_columns(conn)

  result.names |> should.equal([])
  result.data |> should.equal([])
}

pub fn as_columns_insert_statement_returns_empty_columns_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) = ducky.sql("CREATE TABLE t (id INT)") |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("INSERT INTO t VALUES (1), (2)") |> ducky.as_columns(conn)

  result.names |> should.equal([])
  result.data |> should.equal([])
}

pub fn as_columns_insert_returning_keeps_columns_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) = ducky.sql("CREATE TABLE t (id INT)") |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("INSERT INTO t VALUES (1), (2) RETURNING id")
    |> ducky.as_columns(conn)

  result.names |> should.equal(["id"])
  let assert [[ducky.Integer(1), ducky.Integer(2)]] = result.data
}

pub fn parameters_select_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR, age INT)")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql(
      "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
    )
    |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("SELECT name FROM users WHERE age > ? ORDER BY name")
    |> ducky.parameters([ducky.int(28)])
    |> ducky.run(conn)

  result.rows
  |> list.length
  |> should.equal(2)
}

pub fn parameters_as_columns_select_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql("SELECT ?::INTEGER AS id, ?::VARCHAR AS name")
    |> ducky.parameters([ducky.int(42), ducky.text("Eve")])
    |> ducky.as_columns(conn)

  result.names |> should.equal(["id", "name"])
  let assert [[ducky.Integer(42)], [ducky.Text("Eve")]] = result.data
}

pub fn parameters_insert_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR)") |> ducky.run(conn)

  let assert Ok(_) =
    ducky.sql("INSERT INTO users VALUES (?, ?)")
    |> ducky.parameters([ducky.int(42), ducky.text("Eve")])
    |> ducky.run(conn)

  let assert Ok(result) = ducky.sql("SELECT * FROM users") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Integer(42), ducky.Text("Eve")])] = result.rows
}

pub fn parameters_null_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR, age INT)")
    |> ducky.run(conn)

  let assert Ok(_) =
    ducky.sql("INSERT INTO users VALUES (?, ?, ?)")
    |> ducky.parameters([ducky.int(1), ducky.text("Alice"), ducky.null()])
    |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("SELECT * FROM users WHERE age IS NULL") |> ducky.run(conn)

  result.rows
  |> list.length
  |> should.equal(1)
}

pub fn with_connection_auto_cleanup_test() {
  let result =
    ducky.with_connection(":memory:", fn(conn) {
      use _ <- result.try(
        ducky.sql("CREATE TABLE test (id INT, name VARCHAR)")
        |> ducky.run(conn),
      )
      use _ <- result.try(
        ducky.sql("INSERT INTO test VALUES (?, ?)")
        |> ducky.parameters([ducky.int(1), ducky.text("Alice")])
        |> ducky.run(conn),
      )
      ducky.sql("SELECT * FROM test") |> ducky.run(conn)
    })

  result
  |> should.be_ok

  let assert Ok(df) = result
  df.rows
  |> list.length
  |> should.equal(1)
}

pub fn with_connection_error_still_closes_test() {
  let result =
    ducky.with_connection(":memory:", fn(conn) {
      use _ <- result.try(
        ducky.sql("CREATE TABLE test (id INT)") |> ducky.run(conn),
      )
      ducky.sql("SELEKT * FROM test") |> ducky.run(conn)
    })

  case result {
    Error(ducky.QuerySyntaxError(_)) -> True
    Error(ducky.DatabaseError(_)) -> True
    _ -> False
  }
  |> should.be_true
}

pub fn transaction_commit_on_success_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE accounts (id INT, balance INT)")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO accounts VALUES (?, ?)")
    |> ducky.parameters([ducky.int(1), ducky.int(100)])
    |> ducky.run(conn)

  let result =
    ducky.transaction(conn, fn(conn) {
      use _ <- result.try(
        ducky.sql("UPDATE accounts SET balance = balance - ? WHERE id = ?")
        |> ducky.parameters([ducky.int(50), ducky.int(1)])
        |> ducky.run(conn),
      )
      ducky.sql("SELECT balance FROM accounts WHERE id = 1")
      |> ducky.run(conn)
    })

  result
  |> should.be_ok

  let assert Ok(check) =
    ducky.sql("SELECT balance FROM accounts WHERE id = 1") |> ducky.run(conn)
  let assert [row] = check.rows
  let assert ducky.Row([ducky.Integer(balance)]) = row
  balance
  |> should.equal(50)
}

pub fn transaction_rollback_on_error_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE accounts (id INT, balance INT)")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO accounts VALUES (?, ?)")
    |> ducky.parameters([ducky.int(1), ducky.int(100)])
    |> ducky.run(conn)

  let result =
    ducky.transaction(conn, fn(conn) {
      use _ <- result.try(
        ducky.sql("UPDATE accounts SET balance = balance - ? WHERE id = ?")
        |> ducky.parameters([ducky.int(50), ducky.int(1)])
        |> ducky.run(conn),
      )
      ducky.sql("SELEKT * FROM accounts") |> ducky.run(conn)
    })

  result
  |> should.be_error

  let assert Ok(check) =
    ducky.sql("SELECT balance FROM accounts WHERE id = 1") |> ducky.run(conn)
  let assert [row] = check.rows
  let assert ducky.Row([ducky.Integer(balance)]) = row
  balance
  |> should.equal(100)
}

pub fn select_struct_simple_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql("SELECT {'name': 'Alice', 'age': 30} as person")
    |> ducky.run(conn)

  let assert [row] = result.rows
  let assert ducky.Row([person_value]) = row
  let assert ducky.Struct(fields) = person_value
  let assert Ok(name_value) = dict.get(fields, "name")
  let assert Ok(age_value) = dict.get(fields, "age")

  name_value
  |> should.equal(ducky.Text("Alice"))

  age_value
  |> should.equal(ducky.Integer(30))
}

pub fn select_struct_with_null_field_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql("SELECT {'name': 'Bob', 'email': NULL} as person")
    |> ducky.run(conn)

  let assert [row] = result.rows
  let assert ducky.Row([person_value]) = row
  let assert ducky.Struct(fields) = person_value

  let assert Ok(email_value) = dict.get(fields, "email")
  email_value
  |> should.equal(ducky.Null)
}

pub fn select_nested_struct_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql(
      "SELECT {'person': {'name': 'Charlie', 'age': 25}, 'city': 'NYC'} as data",
    )
    |> ducky.run(conn)

  let assert [row] = result.rows
  let assert ducky.Row([data_value]) = row
  let assert ducky.Struct(outer_fields) = data_value

  let assert Ok(person_value) = dict.get(outer_fields, "person")
  let assert ducky.Struct(person_fields) = person_value

  let assert Ok(name_value) = dict.get(person_fields, "name")
  name_value
  |> should.equal(ducky.Text("Charlie"))

  let assert Ok(age_value) = dict.get(person_fields, "age")
  age_value
  |> should.equal(ducky.Integer(25))

  let assert Ok(city_value) = dict.get(outer_fields, "city")
  city_value
  |> should.equal(ducky.Text("NYC"))
}

pub fn select_struct_field_accessor_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql("SELECT {'x': 10, 'y': 20} as point")
    |> ducky.run(conn)

  let assert [row] = result.rows
  let assert ducky.Row([point_value]) = row

  ducky.field(point_value, "x")
  |> should.equal(option.Some(ducky.Integer(10)))

  ducky.field(point_value, "y")
  |> should.equal(option.Some(ducky.Integer(20)))

  ducky.field(point_value, "z")
  |> should.equal(option.None)
}

pub fn select_timestamp_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql("SELECT TIMESTAMP '2024-01-15 10:30:45' as ts")
    |> ducky.run(conn)

  let assert [row] = result.rows
  let assert ducky.Row([ts_value]) = row

  case ts_value {
    ducky.Timestamp(_) -> True
    _ -> False
  }
  |> should.be_true
}

pub fn select_date_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql(
      "SELECT DATE '2024-01-15' as future, DATE '1970-01-01' as epoch, DATE '1950-01-01' as past",
    )
    |> ducky.run(conn)

  let assert [row] = result.rows
  let assert ducky.Row([future, epoch, past]) = row

  case future {
    ducky.Date(days) -> should.be_true(days > 19_000)
    _ -> panic as "Expected Date variant"
  }

  case epoch {
    ducky.Date(days) -> days |> should.equal(0)
    _ -> panic as "Expected Date variant"
  }

  case past {
    ducky.Date(days) -> should.be_true(days < 0)
    _ -> panic as "Expected Date variant"
  }
}

pub fn select_time_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql(
      "SELECT TIME '14:30:45' as afternoon, TIME '00:00:00' as midnight",
    )
    |> ducky.run(conn)

  let assert [row] = result.rows
  let assert ducky.Row([afternoon, midnight]) = row

  case afternoon {
    ducky.Time(micros) -> should.be_true(micros > 50_000_000_000)
    _ -> panic as "Expected Time variant"
  }

  case midnight {
    ducky.Time(micros) -> micros |> should.equal(0)
    _ -> panic as "Expected Time variant"
  }
}

pub fn select_interval_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql(
      "SELECT INTERVAL '2 days 3 hours' as pos, INTERVAL '-5 hours' as neg",
    )
    |> ducky.run(conn)

  let assert [row] = result.rows
  let assert ducky.Row([pos, neg]) = row

  case pos {
    ducky.Interval(months, days, nanos) -> {
      should.equal(months, 0)
      should.equal(days, 2)
      should.equal(nanos, 10_800_000_000_000)
    }
    _ -> panic as "Expected Interval variant"
  }

  case neg {
    ducky.Interval(months, days, nanos) -> {
      should.equal(months, 0)
      should.equal(days, 0)
      should.equal(nanos, -18_000_000_000_000)
    }
    _ -> panic as "Expected Interval variant"
  }
}

pub fn select_temporal_in_struct_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql(
      "SELECT {
        'event': 'meeting',
        'timestamp': TIMESTAMP '2024-01-15 10:30:00',
        'date': DATE '2024-01-15'
      } as event_data",
    )
    |> ducky.run(conn)

  let assert [row] = result.rows
  let assert ducky.Row([event_value]) = row
  let assert ducky.Struct(fields) = event_value

  let assert Ok(event_name) = dict.get(fields, "event")
  event_name
  |> should.equal(ducky.Text("meeting"))

  let assert Ok(ts_value) = dict.get(fields, "timestamp")
  case ts_value {
    ducky.Timestamp(_) -> True
    _ -> False
  }
  |> should.be_true

  let assert Ok(date_value) = dict.get(fields, "date")
  case date_value {
    ducky.Date(_) -> True
    _ -> False
  }
  |> should.be_true
}

pub fn select_null_temporal_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE events (id INT, ts TIMESTAMP, d DATE, t TIME)")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO events VALUES (1, NULL, NULL, NULL)")
    |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("SELECT ts, d, t FROM events") |> ducky.run(conn)
  let assert [row] = result.rows
  let assert ducky.Row([ts, date, time]) = row

  ts
  |> should.equal(ducky.Null)
  date
  |> should.equal(ducky.Null)
  time
  |> should.equal(ducky.Null)
}

pub fn select_blob_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) = ducky.sql("CREATE TABLE t (data BLOB)") |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO t VALUES (?)")
    |> ducky.parameters([ducky.blob(<<0, 1, 2, 255, 254>>)])
    |> ducky.run(conn)
  let assert Ok(result) = ducky.sql("SELECT data FROM t") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Blob(bits)])] = result.rows
  should.equal(bits, <<0, 1, 2, 255, 254>>)
}

pub fn select_float_real_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) = ducky.sql("SELECT 3.14::REAL as f") |> ducky.run(conn)
  let assert [ducky.Row([value])] = result.rows
  case value {
    ducky.Float(f) -> should.be_true(f >. 3.0 && f <. 4.0)
    ducky.Double(f) -> should.be_true(f >. 3.0 && f <. 4.0)
    _ -> panic as "Expected float value"
  }
}

pub fn select_simple_list_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql("SELECT [1, 2, 3, 4, 5] as nums") |> ducky.run(conn)

  let assert [row] = result.rows
  let assert ducky.Row([list_value]) = row

  case list_value {
    ducky.List(items) -> {
      list.length(items)
      |> should.equal(5)

      let assert [first, ..] = items
      first
      |> should.equal(ducky.Integer(1))
    }
    _ -> panic as "Expected List variant"
  }
}

pub fn select_string_list_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql("SELECT ['apple', 'banana', 'cherry'] as fruits")
    |> ducky.run(conn)

  let assert [row] = result.rows
  let assert ducky.Row([list_value]) = row

  case list_value {
    ducky.List(items) -> {
      list.length(items)
      |> should.equal(3)

      let assert [first, second, ..] = items
      first
      |> should.equal(ducky.Text("apple"))
      second
      |> should.equal(ducky.Text("banana"))
    }
    _ -> panic as "Expected List variant"
  }
}

pub fn select_empty_list_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) = ducky.sql("SELECT [] as empty") |> ducky.run(conn)

  let assert [row] = result.rows
  let assert ducky.Row([list_value]) = row

  case list_value {
    ducky.List(items) -> {
      list.length(items)
      |> should.equal(0)
    }
    _ -> panic as "Expected List variant"
  }
}

pub fn select_null_in_list_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql("SELECT [1, NULL, 3] as nums") |> ducky.run(conn)

  let assert [row] = result.rows
  let assert ducky.Row([list_value]) = row

  case list_value {
    ducky.List(items) -> {
      let assert [first, second, third] = items
      first
      |> should.equal(ducky.Integer(1))
      second
      |> should.equal(ducky.Null)
      third
      |> should.equal(ducky.Integer(3))
    }
    _ -> panic as "Expected List variant"
  }
}

pub fn select_nested_list_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql("SELECT [[1, 2], [3, 4], [5, 6]] as matrix")
    |> ducky.run(conn)

  let assert [row] = result.rows
  let assert ducky.Row([list_value]) = row

  case list_value {
    ducky.List(outer_items) -> {
      list.length(outer_items)
      |> should.equal(3)

      let assert [first_nested, ..] = outer_items
      case first_nested {
        ducky.List(inner) -> {
          list.length(inner)
          |> should.equal(2)

          let assert [elem1, elem2] = inner
          elem1
          |> should.equal(ducky.Integer(1))
          elem2
          |> should.equal(ducky.Integer(2))
        }
        _ -> panic as "Expected nested List"
      }
    }
    _ -> panic as "Expected List variant"
  }
}

pub fn select_list_in_struct_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.sql(
      "SELECT {
        'name': 'Alice',
        'scores': [95, 87, 92]
      } as student",
    )
    |> ducky.run(conn)

  let assert [row] = result.rows
  let assert ducky.Row([struct_value]) = row
  let assert ducky.Struct(fields) = struct_value

  let assert Ok(name_value) = dict.get(fields, "name")
  name_value
  |> should.equal(ducky.Text("Alice"))

  let assert Ok(scores_value) = dict.get(fields, "scores")
  case scores_value {
    ducky.List(scores) -> {
      list.length(scores)
      |> should.equal(3)
    }
    _ -> panic as "Expected List in struct"
  }
}

pub fn parameters_timestamp_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE events (id INT, ts TIMESTAMP)")
    |> ducky.run(conn)

  let micros = 1_705_315_845_000_000
  let assert Ok(_) =
    ducky.sql("INSERT INTO events VALUES (?, ?)")
    |> ducky.parameters([ducky.int(1), ducky.timestamp(micros)])
    |> ducky.run(conn)

  let assert Ok(result) = ducky.sql("SELECT ts FROM events") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Timestamp(returned_micros)])] = result.rows
  returned_micros
  |> should.equal(micros)
}

pub fn parameters_date_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE events (id INT, d DATE)") |> ducky.run(conn)

  let days = 19_738
  let assert Ok(_) =
    ducky.sql("INSERT INTO events VALUES (?, ?)")
    |> ducky.parameters([ducky.int(1), ducky.date(days)])
    |> ducky.run(conn)

  let assert Ok(result) = ducky.sql("SELECT d FROM events") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Date(returned_days)])] = result.rows
  returned_days
  |> should.equal(days)
}

pub fn parameters_time_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE events (id INT, t TIME)") |> ducky.run(conn)

  let micros = 52_245_000_000
  let assert Ok(_) =
    ducky.sql("INSERT INTO events VALUES (?, ?)")
    |> ducky.parameters([ducky.int(1), ducky.time(micros)])
    |> ducky.run(conn)

  let assert Ok(result) = ducky.sql("SELECT t FROM events") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Time(returned_micros)])] = result.rows
  returned_micros
  |> should.equal(micros)
}

pub fn parameters_interval_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE events (id INT, duration INTERVAL)")
    |> ducky.run(conn)

  let months = 1
  let days = 2
  let nanos = 10_800_000_000_000

  let assert Ok(_) =
    ducky.sql("INSERT INTO events VALUES (?, ?)")
    |> ducky.parameters([
      ducky.int(1),
      ducky.interval(months: months, days: days, nanos: nanos),
    ])
    |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("SELECT duration FROM events") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Interval(ret_months, ret_days, ret_nanos)])] =
    result.rows
  ret_months |> should.equal(months)
  ret_days |> should.equal(days)
  ret_nanos |> should.equal(nanos)
}

pub fn parameters_list_unsupported_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE data (id INT, items INT[])") |> ducky.run(conn)

  let assert Error(ducky.UnsupportedParameterType("List")) =
    ducky.sql("INSERT INTO data VALUES (?, ?)")
    |> ducky.parameters([
      ducky.int(1),
      ducky.List([ducky.Integer(1), ducky.Integer(2)]),
    ])
    |> ducky.run(conn)
}

pub fn parameters_struct_unsupported_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE data (id INT, metadata STRUCT(x INT))")
    |> ducky.run(conn)

  let assert Error(ducky.UnsupportedParameterType("Struct")) =
    ducky.sql("INSERT INTO data VALUES (?, ?)")
    |> ducky.parameters([
      ducky.int(1),
      ducky.Struct(dict.from_list([#("x", ducky.Integer(10))])),
    ])
    |> ducky.run(conn)
}

pub fn error_connection_failed_type_test() {
  let result = ducky.connect("")

  case result {
    Error(ducky.ConnectionFailed(_)) -> True
    _ -> False
  }
  |> should.be_true
}

pub fn error_connection_failed_clean_message_test() {
  let result = ducky.connect("")

  case result {
    Error(ducky.ConnectionFailed(msg)) -> {
      string.contains(msg, "#(")
      |> should.be_false
    }
    _ -> panic as "Expected ConnectionFailed error"
  }
}

pub fn error_query_syntax_type_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let result = ducky.sql("SELEKT * FROM nonexistent") |> ducky.run(conn)

  case result {
    Error(ducky.QuerySyntaxError(_)) -> True
    Error(ducky.DatabaseError(_)) -> True
    _ -> False
  }
  |> should.be_true
}

pub fn error_query_syntax_clean_message_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let result = ducky.sql("SELEKT * FROM nonexistent") |> ducky.run(conn)

  case result {
    Error(ducky.QuerySyntaxError(msg)) -> {
      string.contains(msg, "#(")
      |> should.be_false
    }
    Error(ducky.DatabaseError(msg)) -> {
      string.contains(msg, "#(")
      |> should.be_false
    }
    _ -> panic as "Expected QuerySyntaxError or DatabaseError"
  }
}

pub fn error_unsupported_param_type_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Error(ducky.UnsupportedParameterType("List")) =
    ducky.sql("SELECT ?")
    |> ducky.parameters([
      ducky.List([ducky.Integer(1), ducky.Integer(2)]),
    ])
    |> ducky.run(conn)
}

pub fn parameters_decimal_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE prices (id INT, amount DECIMAL(10,2))")
    |> ducky.run(conn)

  let amount = "1234.56"
  let assert Ok(_) =
    ducky.sql("INSERT INTO prices VALUES (?, ?)")
    |> ducky.parameters([ducky.int(1), ducky.decimal(amount)])
    |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("SELECT amount FROM prices") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Decimal(returned_amount)])] = result.rows
  returned_amount
  |> should.equal(amount)
}

pub fn select_decimal_preserves_precision_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql("SELECT 123456789.123456789::DECIMAL(18,9) as d")
    |> ducky.run(conn)

  let assert [ducky.Row([ducky.Decimal(value)])] = result.rows
  value
  |> should.equal("123456789.123456789")
}

pub fn select_decimal_negative_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql("SELECT -99.99::DECIMAL(5,2) as d") |> ducky.run(conn)

  let assert [ducky.Row([ducky.Decimal(value)])] = result.rows
  string.contains(value, "99.99")
  |> should.be_true
}

pub fn select_decimal_in_table_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE prices (amount DECIMAL(10,2))") |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO prices VALUES (1234.56)") |> ducky.run(conn)

  let assert Ok(result) = ducky.sql("SELECT * FROM prices") |> ducky.run(conn)

  let assert [ducky.Row([ducky.Decimal(value)])] = result.rows
  value
  |> should.equal("1234.56")
}

pub fn select_enum_returns_text_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TYPE status AS ENUM ('pending', 'done')")
    |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("SELECT 'pending'::status as s") |> ducky.run(conn)

  let assert [ducky.Row([ducky.Text(value)])] = result.rows
  value
  |> should.equal("pending")
}

pub fn select_enum_in_table_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("CREATE TABLE tasks (p priority)") |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO tasks VALUES ('high'), ('low')") |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("SELECT * FROM tasks ORDER BY p") |> ducky.run(conn)

  let assert [ducky.Row([ducky.Text(p1)]), ducky.Row([ducky.Text(p2)])] =
    result.rows
  p1
  |> should.equal("low")
  p2
  |> should.equal("high")
}

pub fn select_array_simple_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql("SELECT [1, 2, 3]::INTEGER[3] as arr") |> ducky.run(conn)

  let assert [ducky.Row([ducky.Array(elements)])] = result.rows
  list.length(elements)
  |> should.equal(3)

  let assert [ducky.Integer(a), ducky.Integer(b), ducky.Integer(c)] = elements
  a |> should.equal(1)
  b |> should.equal(2)
  c |> should.equal(3)
}

pub fn select_array_strings_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql("SELECT ['a', 'b']::VARCHAR[2] as arr") |> ducky.run(conn)

  let assert [ducky.Row([ducky.Array(elements)])] = result.rows
  let assert [ducky.Text(a), ducky.Text(b)] = elements
  a |> should.equal("a")
  b |> should.equal("b")
}

pub fn select_map_simple_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql("SELECT MAP {'key1': 'value1', 'key2': 'value2'} as m")
    |> ducky.run(conn)

  let assert [ducky.Row([ducky.Map(entries)])] = result.rows

  let assert Ok(ducky.Text(v1)) = dict.get(entries, "key1")
  let assert Ok(ducky.Text(v2)) = dict.get(entries, "key2")

  v1 |> should.equal("value1")
  v2 |> should.equal("value2")
}

pub fn select_map_int_values_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql("SELECT MAP {'x': 10, 'y': 20} as coords") |> ducky.run(conn)

  let assert [ducky.Row([ducky.Map(entries)])] = result.rows

  let assert Ok(ducky.Integer(x)) = dict.get(entries, "x")
  let assert Ok(ducky.Integer(y)) = dict.get(entries, "y")

  x |> should.equal(10)
  y |> should.equal(20)
}

pub fn select_map_in_table_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE config (settings MAP(VARCHAR, VARCHAR))")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO config VALUES (MAP {'theme': 'dark'})")
    |> ducky.run(conn)

  let assert Ok(result) = ducky.sql("SELECT * FROM config") |> ducky.run(conn)

  let assert [ducky.Row([ducky.Map(entries)])] = result.rows
  let assert Ok(ducky.Text(theme)) = dict.get(entries, "theme")
  theme |> should.equal("dark")
}

pub fn select_union_simple_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql("SELECT union_value(num := 42) as u") |> ducky.run(conn)

  let assert [ducky.Row([ducky.Union(tag, value)])] = result.rows
  tag |> should.equal("num")
  value |> should.equal(ducky.Integer(42))
}

pub fn select_union_string_variant_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql("SELECT union_value(str := 'hello') as u") |> ducky.run(conn)

  let assert [ducky.Row([ducky.Union(tag, value)])] = result.rows
  tag |> should.equal("str")
  value |> should.equal(ducky.Text("hello"))
}

pub fn select_union_in_table_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(_) =
    ducky.sql("CREATE TYPE int_or_str AS UNION(num INTEGER, str VARCHAR)")
    |> ducky.run(conn)

  let assert Ok(_) =
    ducky.sql("CREATE TABLE data (value int_or_str)") |> ducky.run(conn)

  let assert Ok(_) =
    ducky.sql("INSERT INTO data VALUES (42::int_or_str), ('hello'::int_or_str)")
    |> ducky.run(conn)

  let assert Ok(result) = ducky.sql("SELECT * FROM data") |> ducky.run(conn)

  result.rows |> list.length |> should.equal(2)

  let assert [ducky.Row([first]), ducky.Row([second])] = result.rows

  case first {
    ducky.Union(tag, ducky.Integer(n)) -> {
      tag |> should.equal("num")
      n |> should.equal(42)
    }
    _ -> panic as "Expected Union with Integer"
  }

  case second {
    ducky.Union(tag, ducky.Text(s)) -> {
      tag |> should.equal("str")
      s |> should.equal("hello")
    }
    _ -> panic as "Expected Union with Text"
  }
}

pub fn select_union_null_value_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql("SELECT union_value(num := NULL::INTEGER) as u")
    |> ducky.run(conn)

  let assert [ducky.Row([ducky.Union(tag, value)])] = result.rows
  tag |> should.equal("num")
  value |> should.equal(ducky.Null)
}

pub fn select_union_multiple_types_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(_) =
    ducky.sql(
      "CREATE TYPE multi_union AS UNION(i INTEGER, f DOUBLE, s VARCHAR, b BOOLEAN)",
    )
    |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql(
      "SELECT
        42::multi_union as int_val,
        3.14::multi_union as float_val,
        'test'::multi_union as str_val,
        true::multi_union as bool_val",
    )
    |> ducky.run(conn)

  let assert [ducky.Row([int_u, float_u, str_u, bool_u])] = result.rows

  case int_u {
    ducky.Union("i", ducky.Integer(42)) -> True
    _ -> False
  }
  |> should.be_true

  case float_u {
    ducky.Union("f", ducky.Double(f)) -> f |> should.equal(3.14)
    _ -> panic as "Expected Union with Double"
  }

  case str_u {
    ducky.Union("s", ducky.Text("test")) -> True
    _ -> False
  }
  |> should.be_true

  case bool_u {
    ducky.Union("b", ducky.Boolean(True)) -> True
    _ -> False
  }
  |> should.be_true
}

pub fn parameters_union_unsupported_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Error(ducky.UnsupportedParameterType("Union")) =
    ducky.sql("SELECT ?")
    |> ducky.parameters([ducky.Union("tag", ducky.Integer(42))])
    |> ducky.run(conn)
}

pub fn prepare_valid_sql_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR)") |> ducky.run(conn)

  let assert Ok(stmt) = ducky.prepare(conn, "SELECT * FROM users WHERE id = ?")
  let assert Ok(_) = ducky.finalize(stmt)
}

pub fn prepare_invalid_sql_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  case ducky.prepare(conn, "SELEKT * FROM users") {
    Error(ducky.QuerySyntaxError(_)) -> True
    Error(ducky.DatabaseError(_)) -> True
    _ -> False
  }
  |> should.be_true
}

pub fn execute_prepared_select_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR)") |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO users VALUES (1, 'Alice')") |> ducky.run(conn)
  let assert Ok(stmt) =
    ducky.prepare(conn, "SELECT name FROM users WHERE id = ?")

  let assert Ok(result) = ducky.execute(stmt, [ducky.int(1)])

  let assert [ducky.Row([ducky.Text(name)])] = result.rows
  name |> should.equal("Alice")
}

pub fn execute_prepared_insert_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR)") |> ducky.run(conn)
  let assert Ok(stmt) = ducky.prepare(conn, "INSERT INTO users VALUES (?, ?)")

  let assert Ok(_) = ducky.execute(stmt, [ducky.int(1), ducky.text("Alice")])

  let assert Ok(result) = ducky.sql("SELECT * FROM users") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Integer(1), ducky.Text("Alice")])] = result.rows
}

pub fn execute_prepared_reuse_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE counters (id INT, value INT)") |> ducky.run(conn)
  let assert Ok(stmt) =
    ducky.prepare(conn, "INSERT INTO counters VALUES (?, ?)")

  list.range(1, 100)
  |> list.each(fn(i) {
    let assert Ok(_) = ducky.execute(stmt, [ducky.int(i), ducky.int(i * 10)])
    Nil
  })

  let assert Ok(result) =
    ducky.sql("SELECT COUNT(*) FROM counters") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Integer(count)])] = result.rows
  count |> should.equal(100)
}

pub fn execute_prepared_temporal_params_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE events (ts TIMESTAMP, d DATE)") |> ducky.run(conn)
  let assert Ok(stmt) = ducky.prepare(conn, "INSERT INTO events VALUES (?, ?)")
  let micros = 1_705_315_845_000_000
  let days = 19_738

  let assert Ok(_) =
    ducky.execute(stmt, [ducky.timestamp(micros), ducky.date(days)])

  let assert Ok(result) = ducky.sql("SELECT * FROM events") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Timestamp(ret_micros), ducky.Date(ret_days)])] =
    result.rows
  ret_micros |> should.equal(micros)
  ret_days |> should.equal(days)
}

pub fn execute_prepared_decimal_params_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE prices (amount DECIMAL(10,2))") |> ducky.run(conn)
  let assert Ok(stmt) = ducky.prepare(conn, "INSERT INTO prices VALUES (?)")

  let assert Ok(_) = ducky.execute(stmt, [ducky.decimal("1234.56")])

  let assert Ok(result) = ducky.sql("SELECT * FROM prices") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Decimal(amount)])] = result.rows
  amount |> should.equal("1234.56")
}

pub fn finalize_statement_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(stmt) = ducky.prepare(conn, "SELECT 1")

  ducky.finalize(stmt)
  |> should.be_ok
}

pub fn finalize_already_finalized_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(stmt) = ducky.prepare(conn, "SELECT 1")
  let assert Ok(_) = ducky.finalize(stmt)

  ducky.finalize(stmt)
  |> should.be_error
}

pub fn execute_after_finalize_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(stmt) = ducky.prepare(conn, "SELECT 1")
  let assert Ok(_) = ducky.finalize(stmt)

  let result = ducky.execute(stmt, [])
  result |> should.be_error

  case result {
    Error(ducky.StatementFinalized) -> True
    _ -> False
  }
  |> should.be_true
}

pub fn with_statement_success_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE items (id INT, name VARCHAR)") |> ducky.run(conn)

  let result = {
    use stmt <- ducky.with_statement(conn, "INSERT INTO items VALUES (?, ?)")
    ducky.execute(stmt, [ducky.int(1), ducky.text("A")])
  }

  result |> should.be_ok
  let assert Ok(check) = ducky.sql("SELECT * FROM items") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Integer(1), ducky.Text("A")])] = check.rows
}

pub fn with_statement_error_still_finalizes_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let result = {
    use _stmt <- ducky.with_statement(conn, "SELECT 1")
    Error(ducky.DatabaseError("intentional error"))
  }

  let assert Error(ducky.DatabaseError("intentional error")) = result
}

pub fn append_rows_empty_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) = ducky.sql("CREATE TABLE t (id INT)") |> ducky.run(conn)

  let assert Ok(count) = ducky.append(conn, "t", [])
  count |> should.equal(0)
}

pub fn append_rows_simple_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR)") |> ducky.run(conn)

  let assert Ok(count) =
    ducky.append(conn, "users", [
      [ducky.Integer(1), ducky.Text("Alice")],
      [ducky.Integer(2), ducky.Text("Bob")],
      [ducky.Integer(3), ducky.Text("Charlie")],
    ])

  count |> should.equal(3)

  let assert Ok(result) =
    ducky.sql("SELECT * FROM users ORDER BY id") |> ducky.run(conn)
  list.length(result.rows) |> should.equal(3)

  let assert [ducky.Row([ducky.Integer(1), ducky.Text("Alice")]), ..] =
    result.rows
}

pub fn append_rows_invalid_table_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Error(ducky.DatabaseError(_)) =
    ducky.append(conn, "nonexistent_table", [
      [ducky.Integer(1)],
    ])
}

pub fn append_rows_bulk_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE counters (id INT, value INT)") |> ducky.run(conn)

  let rows =
    list.range(1, 1000)
    |> list.map(fn(i) { [ducky.Integer(i), ducky.Integer(i * 10)] })

  let assert Ok(count) = ducky.append(conn, "counters", rows)
  count |> should.equal(1000)

  let assert Ok(result) =
    ducky.sql("SELECT COUNT(*) FROM counters") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Integer(count_result)])] = result.rows
  count_result |> should.equal(1000)

  let assert Ok(sum_result) =
    ducky.sql("SELECT SUM(value) FROM counters") |> ducky.run(conn)
  let assert [ducky.Row([sum_value])] = sum_result.rows

  case sum_value {
    ducky.BigInt(sum) -> sum |> should.equal(5_005_000)
    ducky.Integer(sum) -> sum |> should.equal(5_005_000)
    _ -> panic as "Expected numeric sum"
  }
}

pub fn append_rows_with_null_values_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR, age INT)")
    |> ducky.run(conn)

  let assert Ok(_) =
    ducky.append(conn, "users", [
      [ducky.Integer(1), ducky.Text("Alice"), ducky.Null],
    ])

  let assert Ok(result) =
    ducky.sql("SELECT * FROM users WHERE age IS NULL") |> ducky.run(conn)
  list.length(result.rows) |> should.equal(1)
}

pub fn append_rows_type_mismatch_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE numbers (id INT)") |> ducky.run(conn)

  let assert Error(ducky.DatabaseError(_)) =
    ducky.append(conn, "numbers", [
      [ducky.Text("not a number")],
    ])
}

pub fn append_rows_with_temporal_types_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE events (ts TIMESTAMP, d DATE)") |> ducky.run(conn)

  let micros = 1_705_315_845_000_000
  let days = 19_738

  let assert Ok(_) =
    ducky.append(conn, "events", [
      [ducky.Timestamp(micros), ducky.Date(days)],
    ])

  let assert Ok(result) = ducky.sql("SELECT * FROM events") |> ducky.run(conn)
  let assert [ducky.Row([ducky.Timestamp(ret_micros), ducky.Date(ret_days)])] =
    result.rows

  ret_micros |> should.equal(micros)
  ret_days |> should.equal(days)
}

pub fn append_rows_wrong_column_count_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE t (a INT, b INT)") |> ducky.run(conn)

  let assert Error(ducky.DatabaseError(_)) =
    ducky.append(conn, "t", [
      [ducky.Integer(1)],
    ])
}

pub fn sql_run_simple_select_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql("SELECT 42 AS answer")
    |> ducky.run(conn)

  result.count |> should.equal(1)
  let assert [ducky.Row([ducky.Integer(42)])] = result.rows
}

pub fn sql_run_ddl_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql("CREATE TABLE t (id INT)")
    |> ducky.run(conn)

  result.count |> should.equal(0)
  result.rows |> should.equal([])
}

pub fn sql_parameter_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR)")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
    |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("SELECT name FROM users WHERE id = ?")
    |> ducky.parameter(ducky.int(1))
    |> ducky.run(conn)

  result.count |> should.equal(1)
  let assert [ducky.Row([ducky.Text("Alice")])] = result.rows
}

pub fn sql_parameters_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR, age INT)")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql(
      "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Eve', 35)",
    )
    |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("SELECT name FROM users WHERE id > ? AND age > ?")
    |> ducky.parameters([ducky.int(1), ducky.int(26)])
    |> ducky.run(conn)

  result.count |> should.equal(1)
  let assert [ducky.Row([ducky.Text("Eve")])] = result.rows
}

pub fn sql_mixed_parameter_and_parameters_order_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.sql("SELECT ?::INT AS a, ?::INT AS b, ?::INT AS c")
    |> ducky.parameter(ducky.int(1))
    |> ducky.parameters([ducky.int(2), ducky.int(3)])
    |> ducky.run(conn)

  let assert [ducky.Row([ducky.Integer(1), ducky.Integer(2), ducky.Integer(3)])] =
    result.rows
}

pub fn sql_returning_decoder_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR)")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
    |> ducky.run(conn)

  let user_decoder = {
    use id <- decode.field(0, decode.int)
    use name <- decode.field(1, decode.string)
    decode.success(#(id, name))
  }

  let assert Ok(result) =
    ducky.sql("SELECT id, name FROM users ORDER BY id")
    |> ducky.returning(user_decoder)
    |> ducky.run(conn)

  result.count |> should.equal(2)
  result.rows |> should.equal([#(1, "Alice"), #(2, "Bob")])
}

pub fn sql_returning_with_parameter_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR)")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
    |> ducky.run(conn)

  let name_decoder = {
    use name <- decode.field(0, decode.string)
    decode.success(name)
  }

  let assert Ok(result) =
    ducky.sql("SELECT name FROM users WHERE id = ?")
    |> ducky.parameter(ducky.int(2))
    |> ducky.returning(name_decoder)
    |> ducky.run(conn)

  result.count |> should.equal(1)
  result.rows |> should.equal(["Bob"])
}

pub fn sql_as_columns_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR)")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
    |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("SELECT id, name FROM users ORDER BY id")
    |> ducky.as_columns(conn)

  result.names |> should.equal(["id", "name"])
  let assert [[ducky.Integer(1), ducky.Integer(2)], _names] = result.data
}

pub fn sql_as_columns_with_parameter_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR)")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Eve')")
    |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("SELECT name FROM users WHERE id > ?")
    |> ducky.parameter(ducky.int(1))
    |> ducky.as_columns(conn)

  result.names |> should.equal(["name"])
  let assert [[ducky.Text("Bob"), ducky.Text("Eve")]] = result.data
}

pub fn sql_as_columns_empty_preserves_names_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT, name VARCHAR)")
    |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("SELECT id, name FROM users")
    |> ducky.as_columns(conn)

  result.names |> should.equal(["id", "name"])
  result.data |> should.equal([[], []])
}

pub fn sql_run_empty_result_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE users (id INT)")
    |> ducky.run(conn)

  let assert Ok(result) =
    ducky.sql("SELECT * FROM users")
    |> ducky.run(conn)

  result.count |> should.equal(0)
  result.rows |> should.equal([])
}

pub fn column_helper_test() {
  let cols =
    ducky.Columnar(names: ["id", "name"], data: [
      [ducky.Integer(1), ducky.Integer(2)],
      [ducky.Text("Alice"), ducky.Text("Bob")],
    ])

  let assert option.Some(ids) = ducky.column(cols, "id")
  ids |> should.equal([ducky.Integer(1), ducky.Integer(2)])

  let assert option.Some(names) = ducky.column(cols, "name")
  names |> should.equal([ducky.Text("Alice"), ducky.Text("Bob")])

  ducky.column(cols, "missing") |> should.equal(option.None)
}

pub fn timestamp_decoder_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE events (ts TIMESTAMP)")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO events VALUES ('2024-01-01 00:00:00')")
    |> ducky.run(conn)

  let decoder = {
    use ts <- decode.field(0, ducky.timestamp_decoder())
    decode.success(ts)
  }

  let assert Ok(result) =
    ducky.sql("SELECT ts FROM events")
    |> ducky.returning(decoder)
    |> ducky.run(conn)

  let assert [micros] = result.rows
  micros |> should.equal(1_704_067_200_000_000)
}

pub fn date_decoder_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE events (d DATE)")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO events VALUES ('2024-01-01')")
    |> ducky.run(conn)

  let decoder = {
    use d <- decode.field(0, ducky.date_decoder())
    decode.success(d)
  }

  let assert Ok(result) =
    ducky.sql("SELECT d FROM events")
    |> ducky.returning(decoder)
    |> ducky.run(conn)

  let assert [days] = result.rows
  days |> should.equal(19_723)
}

pub fn time_decoder_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE events (t TIME)")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO events VALUES ('12:30:00')")
    |> ducky.run(conn)

  let decoder = {
    use t <- decode.field(0, ducky.time_decoder())
    decode.success(t)
  }

  let assert Ok(result) =
    ducky.sql("SELECT t FROM events")
    |> ducky.returning(decoder)
    |> ducky.run(conn)

  let assert [micros] = result.rows
  micros |> should.equal(45_000_000_000)
}

pub fn interval_decoder_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let decoder = {
    use iv <- decode.field(0, ducky.interval_decoder())
    decode.success(iv)
  }

  let assert Ok(result) =
    ducky.sql("SELECT INTERVAL '1 year 2 months 3 days 4 hours'")
    |> ducky.returning(decoder)
    |> ducky.run(conn)

  let assert [#(months, days, nanos)] = result.rows
  months |> should.equal(14)
  days |> should.equal(3)
  nanos |> should.equal(14_400_000_000_000)
}

pub fn decimal_decoder_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.sql("CREATE TABLE prices (amount DECIMAL(10,2))")
    |> ducky.run(conn)
  let assert Ok(_) =
    ducky.sql("INSERT INTO prices VALUES (99.95)")
    |> ducky.run(conn)

  let decoder = {
    use amount <- decode.field(0, ducky.decimal_decoder())
    decode.success(amount)
  }

  let assert Ok(result) =
    ducky.sql("SELECT amount FROM prices")
    |> ducky.returning(decoder)
    |> ducky.run(conn)

  let assert ["99.95"] = result.rows
}
