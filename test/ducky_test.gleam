import ducky
import gleam/dict
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
  ducky.connect(":memory:")
  |> should.be_ok
}

pub fn connect_empty_path_test() {
  ducky.connect("")
  |> should.be_error
}

pub fn close_connection_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  ducky.close(conn)
  |> should.be_ok
}

pub fn query_empty_sql_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  ducky.query(conn, "")
  |> should.be_error
}

pub fn query_select_simple_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(conn, "SELECT 42 as answer, 'hello' as greeting")

  result.columns
  |> should.equal(["answer", "greeting"])

  result.rows
  |> should.not_equal([])
}

pub fn query_create_table_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  ducky.query(conn, "CREATE TABLE users (id INT, name VARCHAR)")
  |> should.be_ok
}

pub fn query_insert_and_select_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE users (id INT, name VARCHAR)")
  let assert Ok(_) =
    ducky.query(conn, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
  let assert Ok(result) = ducky.query(conn, "SELECT * FROM users ORDER BY id")

  result.columns
  |> should.equal(["id", "name"])

  result.rows
  |> list.length
  |> should.equal(2)
}

pub fn query_params_select_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE users (id INT, name VARCHAR, age INT)")
  let assert Ok(_) =
    ducky.query(
      conn,
      "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
    )

  let assert Ok(result) =
    ducky.query_params(
      conn,
      "SELECT name FROM users WHERE age > ? ORDER BY name",
      [ducky.Integer(28)],
    )

  result.columns
  |> should.equal(["name"])

  result.rows
  |> list.length
  |> should.equal(2)
}

pub fn query_params_insert_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE users (id INT, name VARCHAR)")

  // Insert with parameters
  let assert Ok(_) =
    ducky.query_params(conn, "INSERT INTO users VALUES (?, ?)", [
      ducky.Integer(42),
      ducky.Text("Eve"),
    ])

  let assert Ok(result) = ducky.query(conn, "SELECT * FROM users")

  result.rows
  |> list.length
  |> should.equal(1)
}

pub fn query_params_null_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE users (id INT, name VARCHAR, age INT)")

  let assert Ok(_) =
    ducky.query_params(conn, "INSERT INTO users VALUES (?, ?, ?)", [
      ducky.Integer(1),
      ducky.Text("Alice"),
      ducky.Null,
    ])

  let assert Ok(result) =
    ducky.query(conn, "SELECT * FROM users WHERE age IS NULL")

  result.rows
  |> list.length
  |> should.equal(1)
}

pub fn with_connection_auto_cleanup_test() {
  let result =
    ducky.with_connection(":memory:", fn(conn) {
      use _created <- result.try(ducky.query(
        conn,
        "CREATE TABLE test (id INT, name VARCHAR)",
      ))
      use _inserted <- result.try(
        ducky.query_params(conn, "INSERT INTO test VALUES (?, ?)", [
          ducky.Integer(1),
          ducky.Text("Alice"),
        ]),
      )
      ducky.query(conn, "SELECT * FROM test")
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
      use _created <- result.try(ducky.query(conn, "CREATE TABLE test (id INT)"))
      // Invalid SQL should return error
      ducky.query(conn, "SELEKT * FROM test")
    })

  result
  |> should.be_error
}

pub fn transaction_commit_on_success_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE accounts (id INT, balance INT)")
  let assert Ok(_) =
    ducky.query_params(conn, "INSERT INTO accounts VALUES (?, ?)", [
      ducky.Integer(1),
      ducky.Integer(100),
    ])

  let result =
    ducky.transaction(conn, fn(conn) {
      use _ <- result.try(
        ducky.query_params(
          conn,
          "UPDATE accounts SET balance = balance - ? WHERE id = ?",
          [ducky.Integer(50), ducky.Integer(1)],
        ),
      )
      ducky.query(conn, "SELECT balance FROM accounts WHERE id = 1")
    })

  result
  |> should.be_ok

  let assert Ok(check) =
    ducky.query(conn, "SELECT balance FROM accounts WHERE id = 1")
  let assert [row] = check.rows
  let assert ducky.Row([ducky.Integer(balance)]) = row
  balance
  |> should.equal(50)
}

pub fn transaction_rollback_on_error_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE accounts (id INT, balance INT)")
  let assert Ok(_) =
    ducky.query_params(conn, "INSERT INTO accounts VALUES (?, ?)", [
      ducky.Integer(1),
      ducky.Integer(100),
    ])

  let result =
    ducky.transaction(conn, fn(conn) {
      use _ <- result.try(
        ducky.query_params(
          conn,
          "UPDATE accounts SET balance = balance - ? WHERE id = ?",
          [ducky.Integer(50), ducky.Integer(1)],
        ),
      )
      // This should cause an error and trigger rollback
      ducky.query(conn, "SELEKT * FROM accounts")
    })

  result
  |> should.be_error

  let assert Ok(check) =
    ducky.query(conn, "SELECT balance FROM accounts WHERE id = 1")
  let assert [row] = check.rows
  let assert ducky.Row([ducky.Integer(balance)]) = row
  balance
  |> should.equal(100)
}

pub fn query_struct_simple_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(conn, "SELECT {'name': 'Alice', 'age': 30} as person")

  result.columns
  |> should.equal(["person"])

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

pub fn query_struct_with_null_field_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(conn, "SELECT {'name': 'Bob', 'email': NULL} as person")

  let assert [row] = result.rows
  let assert ducky.Row([person_value]) = row
  let assert ducky.Struct(fields) = person_value

  let assert Ok(email_value) = dict.get(fields, "email")
  email_value
  |> should.equal(ducky.Null)
}

pub fn query_nested_struct_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(
      conn,
      "SELECT {'person': {'name': 'Charlie', 'age': 25}, 'city': 'NYC'} as data",
    )

  let assert [row] = result.rows
  let assert ducky.Row([data_value]) = row
  let assert ducky.Struct(outer_fields) = data_value

  // Get nested struct
  let assert Ok(person_value) = dict.get(outer_fields, "person")
  let assert ducky.Struct(person_fields) = person_value

  let assert Ok(name_value) = dict.get(person_fields, "name")
  name_value
  |> should.equal(ducky.Text("Charlie"))

  let assert Ok(age_value) = dict.get(person_fields, "age")
  age_value
  |> should.equal(ducky.Integer(25))

  // Get top-level field
  let assert Ok(city_value) = dict.get(outer_fields, "city")
  city_value
  |> should.equal(ducky.Text("NYC"))
}

pub fn query_struct_field_accessor_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(conn, "SELECT {'x': 10, 'y': 20} as point")

  let assert [row] = result.rows
  let assert ducky.Row([point_value]) = row

  ducky.field(point_value, "x")
  |> should.equal(option.Some(ducky.Integer(10)))

  ducky.field(point_value, "y")
  |> should.equal(option.Some(ducky.Integer(20)))

  ducky.field(point_value, "z")
  |> should.equal(option.None)
}

pub fn query_timestamp_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(conn, "SELECT TIMESTAMP '2024-01-15 10:30:45' as ts")

  let assert [row] = result.rows
  let assert ducky.Row([ts_value]) = row

  case ts_value {
    ducky.Timestamp(_) -> True
    _ -> False
  }
  |> should.be_true
}

pub fn query_date_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(
      conn,
      "SELECT DATE '2024-01-15' as future, DATE '1970-01-01' as epoch, DATE '1950-01-01' as past",
    )

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

pub fn query_time_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(
      conn,
      "SELECT TIME '14:30:45' as afternoon, TIME '00:00:00' as midnight",
    )

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

pub fn query_interval_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(
      conn,
      "SELECT INTERVAL '2 days 3 hours' as pos, INTERVAL '-5 hours' as neg",
    )

  let assert [row] = result.rows
  let assert ducky.Row([pos, neg]) = row

  // '2 days 3 hours' = months: 0, days: 2, nanos: 3 hours in nanos
  case pos {
    ducky.Interval(months, days, nanos) -> {
      should.equal(months, 0)
      should.equal(days, 2)
      // 3 hours = 3 * 60 * 60 * 1_000_000_000 nanos
      should.equal(nanos, 10_800_000_000_000)
    }
    _ -> panic as "Expected Interval variant"
  }

  // '-5 hours' = months: 0, days: 0, nanos: -5 hours in nanos
  case neg {
    ducky.Interval(months, days, nanos) -> {
      should.equal(months, 0)
      should.equal(days, 0)
      // -5 hours = -5 * 60 * 60 * 1_000_000_000 nanos
      should.equal(nanos, -18_000_000_000_000)
    }
    _ -> panic as "Expected Interval variant"
  }
}

pub fn query_temporal_in_struct_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(
      conn,
      "SELECT {
        'event': 'meeting',
        'timestamp': TIMESTAMP '2024-01-15 10:30:00',
        'date': DATE '2024-01-15'
      } as event_data",
    )

  let assert [row] = result.rows
  let assert ducky.Row([event_value]) = row
  let assert ducky.Struct(fields) = event_value

  // Check that temporal fields are properly decoded within struct
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

pub fn query_null_temporal_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(
      conn,
      "CREATE TABLE events (id INT, ts TIMESTAMP, d DATE, t TIME)",
    )
  let assert Ok(_) =
    ducky.query(conn, "INSERT INTO events VALUES (1, NULL, NULL, NULL)")

  let assert Ok(result) = ducky.query(conn, "SELECT ts, d, t FROM events")
  let assert [row] = result.rows
  let assert ducky.Row([ts, date, time]) = row

  ts
  |> should.equal(ducky.Null)
  date
  |> should.equal(ducky.Null)
  time
  |> should.equal(ducky.Null)
}

pub fn query_simple_list_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) = ducky.query(conn, "SELECT [1, 2, 3, 4, 5] as nums")

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

pub fn query_string_list_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(conn, "SELECT ['apple', 'banana', 'cherry'] as fruits")

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

pub fn query_empty_list_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) = ducky.query(conn, "SELECT [] as empty")

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

pub fn query_null_in_list_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) = ducky.query(conn, "SELECT [1, NULL, 3] as nums")

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

pub fn query_nested_list_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(conn, "SELECT [[1, 2], [3, 4], [5, 6]] as matrix")

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

pub fn query_list_in_struct_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(
      conn,
      "SELECT {
        'name': 'Alice',
        'scores': [95, 87, 92]
      } as student",
    )

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

pub fn query_params_timestamp_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE events (id INT, ts TIMESTAMP)")

  // Microseconds since epoch: 2024-01-15 10:30:45 UTC
  let micros = 1_705_315_845_000_000
  let assert Ok(_) =
    ducky.query_params(conn, "INSERT INTO events VALUES (?, ?)", [
      ducky.Integer(1),
      ducky.Timestamp(micros),
    ])

  let assert Ok(result) = ducky.query(conn, "SELECT ts FROM events")
  let assert [ducky.Row([ducky.Timestamp(returned_micros)])] = result.rows
  returned_micros
  |> should.equal(micros)
}

pub fn query_params_date_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) = ducky.query(conn, "CREATE TABLE events (id INT, d DATE)")

  // Days since epoch: 2024-01-15
  let days = 19_738
  let assert Ok(_) =
    ducky.query_params(conn, "INSERT INTO events VALUES (?, ?)", [
      ducky.Integer(1),
      ducky.Date(days),
    ])

  let assert Ok(result) = ducky.query(conn, "SELECT d FROM events")
  let assert [ducky.Row([ducky.Date(returned_days)])] = result.rows
  returned_days
  |> should.equal(days)
}

pub fn query_params_time_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) = ducky.query(conn, "CREATE TABLE events (id INT, t TIME)")

  // Microseconds since midnight: 14:30:45
  let micros = 52_245_000_000
  let assert Ok(_) =
    ducky.query_params(conn, "INSERT INTO events VALUES (?, ?)", [
      ducky.Integer(1),
      ducky.Time(micros),
    ])

  let assert Ok(result) = ducky.query(conn, "SELECT t FROM events")
  let assert [ducky.Row([ducky.Time(returned_micros)])] = result.rows
  returned_micros
  |> should.equal(micros)
}

pub fn query_params_interval_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE events (id INT, duration INTERVAL)")

  // Interval with 1 month, 2 days, 3 hours in nanoseconds
  let months = 1
  let days = 2
  let nanos = 10_800_000_000_000

  let assert Ok(_) =
    ducky.query_params(conn, "INSERT INTO events VALUES (?, ?)", [
      ducky.Integer(1),
      ducky.Interval(months, days, nanos),
    ])

  let assert Ok(result) = ducky.query(conn, "SELECT duration FROM events")
  let assert [ducky.Row([ducky.Interval(ret_months, ret_days, ret_nanos)])] =
    result.rows
  ret_months |> should.equal(months)
  ret_days |> should.equal(days)
  ret_nanos |> should.equal(nanos)
}

pub fn query_params_list_unsupported_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE data (id INT, items INT[])")

  ducky.query_params(conn, "INSERT INTO data VALUES (?, ?)", [
    ducky.Integer(1),
    ducky.List([ducky.Integer(1), ducky.Integer(2)]),
  ])
  |> should.be_error
}

pub fn query_params_struct_unsupported_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE data (id INT, metadata STRUCT(x INT))")

  ducky.query_params(conn, "INSERT INTO data VALUES (?, ?)", [
    ducky.Integer(1),
    ducky.Struct(dict.from_list([#("x", ducky.Integer(10))])),
  ])
  |> should.be_error
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
  let result = ducky.query(conn, "SELEKT * FROM nonexistent")

  case result {
    Error(ducky.QuerySyntaxError(_)) -> True
    Error(ducky.DatabaseError(_)) -> True
    _ -> False
  }
  |> should.be_true
}

pub fn error_query_syntax_clean_message_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let result = ducky.query(conn, "SELEKT * FROM nonexistent")

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
  let result =
    ducky.query_params(conn, "SELECT ?", [
      ducky.List([ducky.Integer(1), ducky.Integer(2)]),
    ])

  case result {
    Error(ducky.UnsupportedParameterType(msg)) -> {
      // Message should describe the unsupported types
      string.contains(msg, "List")
      |> should.be_true
    }
    _ -> panic as "Expected UnsupportedParameterType error"
  }
}

pub fn query_params_decimal_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE prices (id INT, amount DECIMAL(10,2))")

  let amount = "1234.56"
  let assert Ok(_) =
    ducky.query_params(conn, "INSERT INTO prices VALUES (?, ?)", [
      ducky.Integer(1),
      ducky.Decimal(amount),
    ])

  let assert Ok(result) = ducky.query(conn, "SELECT amount FROM prices")
  let assert [ducky.Row([ducky.Decimal(returned_amount)])] = result.rows
  returned_amount
  |> should.equal(amount)
}

pub fn query_decimal_preserves_precision_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.query(conn, "SELECT 123456789.123456789::DECIMAL(18,9) as d")

  let assert [ducky.Row([ducky.Decimal(value)])] = result.rows
  value
  |> should.equal("123456789.123456789")
}

pub fn query_decimal_negative_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) = ducky.query(conn, "SELECT -99.99::DECIMAL(5,2) as d")

  let assert [ducky.Row([ducky.Decimal(value)])] = result.rows
  string.contains(value, "99.99")
  |> should.be_true
}

pub fn query_decimal_in_table_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE prices (amount DECIMAL(10,2))")
  let assert Ok(_) = ducky.query(conn, "INSERT INTO prices VALUES (1234.56)")

  let assert Ok(result) = ducky.query(conn, "SELECT * FROM prices")

  let assert [ducky.Row([ducky.Decimal(value)])] = result.rows
  value
  |> should.equal("1234.56")
}

pub fn query_enum_returns_text_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TYPE status AS ENUM ('pending', 'done')")

  let assert Ok(result) = ducky.query(conn, "SELECT 'pending'::status as s")

  let assert [ducky.Row([ducky.Text(value)])] = result.rows
  value
  |> should.equal("pending")
}

pub fn query_enum_in_table_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TYPE priority AS ENUM ('low', 'medium', 'high')")
  let assert Ok(_) = ducky.query(conn, "CREATE TABLE tasks (p priority)")
  let assert Ok(_) =
    ducky.query(conn, "INSERT INTO tasks VALUES ('high'), ('low')")

  let assert Ok(result) = ducky.query(conn, "SELECT * FROM tasks ORDER BY p")

  let assert [ducky.Row([ducky.Text(p1)]), ducky.Row([ducky.Text(p2)])] =
    result.rows
  p1
  |> should.equal("low")
  p2
  |> should.equal("high")
}

pub fn query_array_simple_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  // DuckDB fixed-size array
  let assert Ok(result) =
    ducky.query(conn, "SELECT [1, 2, 3]::INTEGER[3] as arr")

  let assert [ducky.Row([ducky.Array(elements)])] = result.rows
  list.length(elements)
  |> should.equal(3)

  let assert [ducky.Integer(a), ducky.Integer(b), ducky.Integer(c)] = elements
  a |> should.equal(1)
  b |> should.equal(2)
  c |> should.equal(3)
}

pub fn query_array_strings_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.query(conn, "SELECT ['a', 'b']::VARCHAR[2] as arr")

  let assert [ducky.Row([ducky.Array(elements)])] = result.rows
  let assert [ducky.Text(a), ducky.Text(b)] = elements
  a |> should.equal("a")
  b |> should.equal("b")
}

pub fn query_map_simple_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.query(conn, "SELECT MAP {'key1': 'value1', 'key2': 'value2'} as m")

  let assert [ducky.Row([ducky.Map(entries)])] = result.rows

  let assert Ok(ducky.Text(v1)) = dict.get(entries, "key1")
  let assert Ok(ducky.Text(v2)) = dict.get(entries, "key2")

  v1 |> should.equal("value1")
  v2 |> should.equal("value2")
}

pub fn query_map_int_values_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.query(conn, "SELECT MAP {'x': 10, 'y': 20} as coords")

  let assert [ducky.Row([ducky.Map(entries)])] = result.rows

  let assert Ok(ducky.Integer(x)) = dict.get(entries, "x")
  let assert Ok(ducky.Integer(y)) = dict.get(entries, "y")

  x |> should.equal(10)
  y |> should.equal(20)
}

pub fn query_map_in_table_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE config (settings MAP(VARCHAR, VARCHAR))")
  let assert Ok(_) =
    ducky.query(conn, "INSERT INTO config VALUES (MAP {'theme': 'dark'})")

  let assert Ok(result) = ducky.query(conn, "SELECT * FROM config")

  let assert [ducky.Row([ducky.Map(entries)])] = result.rows
  let assert Ok(ducky.Text(theme)) = dict.get(entries, "theme")
  theme |> should.equal("dark")
}

pub fn query_union_simple_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.query(conn, "SELECT union_value(num := 42) as u")

  let assert [ducky.Row([ducky.Union(tag, value)])] = result.rows
  tag |> should.equal("num")
  value |> should.equal(ducky.Integer(42))
}

pub fn query_union_string_variant_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.query(conn, "SELECT union_value(str := 'hello') as u")

  let assert [ducky.Row([ducky.Union(tag, value)])] = result.rows
  tag |> should.equal("str")
  value |> should.equal(ducky.Text("hello"))
}

pub fn query_union_in_table_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(_) =
    ducky.query(
      conn,
      "CREATE TYPE int_or_str AS UNION(num INTEGER, str VARCHAR)",
    )

  let assert Ok(_) = ducky.query(conn, "CREATE TABLE data (value int_or_str)")

  let assert Ok(_) =
    ducky.query(
      conn,
      "INSERT INTO data VALUES (42::int_or_str), ('hello'::int_or_str)",
    )

  let assert Ok(result) = ducky.query(conn, "SELECT * FROM data")

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

pub fn query_union_null_value_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(result) =
    ducky.query(conn, "SELECT union_value(num := NULL::INTEGER) as u")

  let assert [ducky.Row([ducky.Union(tag, value)])] = result.rows
  tag |> should.equal("num")
  value |> should.equal(ducky.Null)
}

pub fn query_union_multiple_types_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  let assert Ok(_) =
    ducky.query(
      conn,
      "CREATE TYPE multi_union AS UNION(i INTEGER, f DOUBLE, s VARCHAR, b BOOLEAN)",
    )

  let assert Ok(result) =
    ducky.query(
      conn,
      "SELECT
        42::multi_union as int_val,
        3.14::multi_union as float_val,
        'test'::multi_union as str_val,
        true::multi_union as bool_val",
    )

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

pub fn query_union_param_unsupported_test() {
  let assert Ok(conn) = ducky.connect(":memory:")

  ducky.query_params(conn, "SELECT ?", [
    ducky.Union("tag", ducky.Integer(42)),
  ])
  |> should.be_error
}
