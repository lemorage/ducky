import ducky
import ducky/error
import ducky/types
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
      [types.Integer(28)],
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
      types.Integer(42),
      types.Text("Eve"),
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
      types.Integer(1),
      types.Text("Alice"),
      types.Null,
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
          types.Integer(1),
          types.Text("Alice"),
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
      types.Integer(1),
      types.Integer(100),
    ])

  let result =
    ducky.transaction(conn, fn(conn) {
      use _ <- result.try(
        ducky.query_params(
          conn,
          "UPDATE accounts SET balance = balance - ? WHERE id = ?",
          [types.Integer(50), types.Integer(1)],
        ),
      )
      ducky.query(conn, "SELECT balance FROM accounts WHERE id = 1")
    })

  result
  |> should.be_ok

  let assert Ok(check) =
    ducky.query(conn, "SELECT balance FROM accounts WHERE id = 1")
  let assert [row] = check.rows
  let assert types.Row([types.Integer(balance)]) = row
  balance
  |> should.equal(50)
}

pub fn transaction_rollback_on_error_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE accounts (id INT, balance INT)")
  let assert Ok(_) =
    ducky.query_params(conn, "INSERT INTO accounts VALUES (?, ?)", [
      types.Integer(1),
      types.Integer(100),
    ])

  let result =
    ducky.transaction(conn, fn(conn) {
      use _ <- result.try(
        ducky.query_params(
          conn,
          "UPDATE accounts SET balance = balance - ? WHERE id = ?",
          [types.Integer(50), types.Integer(1)],
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
  let assert types.Row([types.Integer(balance)]) = row
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
  let assert types.Row([person_value]) = row
  let assert types.Struct(fields) = person_value
  let assert Ok(name_value) = dict.get(fields, "name")
  let assert Ok(age_value) = dict.get(fields, "age")

  name_value
  |> should.equal(types.Text("Alice"))

  age_value
  |> should.equal(types.Integer(30))
}

pub fn query_struct_with_null_field_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(conn, "SELECT {'name': 'Bob', 'email': NULL} as person")

  let assert [row] = result.rows
  let assert types.Row([person_value]) = row
  let assert types.Struct(fields) = person_value

  let assert Ok(email_value) = dict.get(fields, "email")
  email_value
  |> should.equal(types.Null)
}

pub fn query_nested_struct_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(
      conn,
      "SELECT {'person': {'name': 'Charlie', 'age': 25}, 'city': 'NYC'} as data",
    )

  let assert [row] = result.rows
  let assert types.Row([data_value]) = row
  let assert types.Struct(outer_fields) = data_value

  // Get nested struct
  let assert Ok(person_value) = dict.get(outer_fields, "person")
  let assert types.Struct(person_fields) = person_value

  let assert Ok(name_value) = dict.get(person_fields, "name")
  name_value
  |> should.equal(types.Text("Charlie"))

  let assert Ok(age_value) = dict.get(person_fields, "age")
  age_value
  |> should.equal(types.Integer(25))

  // Get top-level field
  let assert Ok(city_value) = dict.get(outer_fields, "city")
  city_value
  |> should.equal(types.Text("NYC"))
}

pub fn query_struct_field_accessor_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(conn, "SELECT {'x': 10, 'y': 20} as point")

  let assert [row] = result.rows
  let assert types.Row([point_value]) = row

  types.field(point_value, "x")
  |> should.equal(option.Some(types.Integer(10)))

  types.field(point_value, "y")
  |> should.equal(option.Some(types.Integer(20)))

  types.field(point_value, "z")
  |> should.equal(option.None)
}

pub fn query_timestamp_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(conn, "SELECT TIMESTAMP '2024-01-15 10:30:45' as ts")

  let assert [row] = result.rows
  let assert types.Row([ts_value]) = row

  case ts_value {
    types.Timestamp(_) -> True
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
  let assert types.Row([future, epoch, past]) = row

  case future {
    types.Date(days) -> should.be_true(days > 19_000)
    _ -> panic as "Expected Date variant"
  }

  case epoch {
    types.Date(days) -> days |> should.equal(0)
    _ -> panic as "Expected Date variant"
  }

  case past {
    types.Date(days) -> should.be_true(days < 0)
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
  let assert types.Row([afternoon, midnight]) = row

  case afternoon {
    types.Time(micros) -> should.be_true(micros > 50_000_000_000)
    _ -> panic as "Expected Time variant"
  }

  case midnight {
    types.Time(micros) -> micros |> should.equal(0)
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
  let assert types.Row([pos, neg]) = row

  case pos {
    types.Interval(nanos) -> should.be_true(nanos > 0)
    _ -> panic as "Expected Interval variant"
  }

  case neg {
    types.Interval(nanos) -> should.be_true(nanos < 0)
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
  let assert types.Row([event_value]) = row
  let assert types.Struct(fields) = event_value

  // Check that temporal fields are properly decoded within struct
  let assert Ok(event_name) = dict.get(fields, "event")
  event_name
  |> should.equal(types.Text("meeting"))

  let assert Ok(ts_value) = dict.get(fields, "timestamp")
  case ts_value {
    types.Timestamp(_) -> True
    _ -> False
  }
  |> should.be_true

  let assert Ok(date_value) = dict.get(fields, "date")
  case date_value {
    types.Date(_) -> True
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
  let assert types.Row([ts, date, time]) = row

  ts
  |> should.equal(types.Null)
  date
  |> should.equal(types.Null)
  time
  |> should.equal(types.Null)
}

pub fn query_simple_list_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) = ducky.query(conn, "SELECT [1, 2, 3, 4, 5] as nums")

  let assert [row] = result.rows
  let assert types.Row([list_value]) = row

  case list_value {
    types.List(items) -> {
      list.length(items)
      |> should.equal(5)

      let assert [first, ..] = items
      first
      |> should.equal(types.Integer(1))
    }
    _ -> panic as "Expected List variant"
  }
}

pub fn query_string_list_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(conn, "SELECT ['apple', 'banana', 'cherry'] as fruits")

  let assert [row] = result.rows
  let assert types.Row([list_value]) = row

  case list_value {
    types.List(items) -> {
      list.length(items)
      |> should.equal(3)

      let assert [first, second, ..] = items
      first
      |> should.equal(types.Text("apple"))
      second
      |> should.equal(types.Text("banana"))
    }
    _ -> panic as "Expected List variant"
  }
}

pub fn query_empty_list_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) = ducky.query(conn, "SELECT [] as empty")

  let assert [row] = result.rows
  let assert types.Row([list_value]) = row

  case list_value {
    types.List(items) -> {
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
  let assert types.Row([list_value]) = row

  case list_value {
    types.List(items) -> {
      let assert [first, second, third] = items
      first
      |> should.equal(types.Integer(1))
      second
      |> should.equal(types.Null)
      third
      |> should.equal(types.Integer(3))
    }
    _ -> panic as "Expected List variant"
  }
}

pub fn query_nested_list_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) =
    ducky.query(conn, "SELECT [[1, 2], [3, 4], [5, 6]] as matrix")

  let assert [row] = result.rows
  let assert types.Row([list_value]) = row

  case list_value {
    types.List(outer_items) -> {
      list.length(outer_items)
      |> should.equal(3)

      let assert [first_nested, ..] = outer_items
      case first_nested {
        types.List(inner) -> {
          list.length(inner)
          |> should.equal(2)

          let assert [elem1, elem2] = inner
          elem1
          |> should.equal(types.Integer(1))
          elem2
          |> should.equal(types.Integer(2))
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
  let assert types.Row([struct_value]) = row
  let assert types.Struct(fields) = struct_value

  let assert Ok(name_value) = dict.get(fields, "name")
  name_value
  |> should.equal(types.Text("Alice"))

  let assert Ok(scores_value) = dict.get(fields, "scores")
  case scores_value {
    types.List(scores) -> {
      list.length(scores)
      |> should.equal(3)
    }
    _ -> panic as "Expected List in struct"
  }
}

pub fn query_params_timestamp_unsupported_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE events (id INT, ts TIMESTAMP)")

  ducky.query_params(conn, "INSERT INTO events VALUES (?, ?)", [
    types.Integer(1),
    types.Timestamp(1_705_315_845),
  ])
  |> should.be_error
}

pub fn query_params_date_unsupported_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) = ducky.query(conn, "CREATE TABLE events (id INT, d DATE)")

  ducky.query_params(conn, "INSERT INTO events VALUES (?, ?)", [
    types.Integer(1),
    types.Date(19_738),
  ])
  |> should.be_error
}

pub fn query_params_time_unsupported_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) = ducky.query(conn, "CREATE TABLE events (id INT, t TIME)")

  ducky.query_params(conn, "INSERT INTO events VALUES (?, ?)", [
    types.Integer(1),
    types.Time(52_245_000_000),
  ])
  |> should.be_error
}

pub fn query_params_interval_unsupported_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE events (id INT, duration INTERVAL)")

  ducky.query_params(conn, "INSERT INTO events VALUES (?, ?)", [
    types.Integer(1),
    types.Interval(86_400_000_000_000),
  ])
  |> should.be_error
}

pub fn query_params_list_unsupported_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE data (id INT, items INT[])")

  ducky.query_params(conn, "INSERT INTO data VALUES (?, ?)", [
    types.Integer(1),
    types.List([types.Integer(1), types.Integer(2)]),
  ])
  |> should.be_error
}

pub fn query_params_struct_unsupported_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(_) =
    ducky.query(conn, "CREATE TABLE data (id INT, metadata STRUCT(x INT))")

  ducky.query_params(conn, "INSERT INTO data VALUES (?, ?)", [
    types.Integer(1),
    types.Struct(dict.from_list([#("x", types.Integer(10))])),
  ])
  |> should.be_error
}

pub fn error_connection_failed_type_test() {
  let result = ducky.connect("")

  case result {
    Error(error.ConnectionFailed(_)) -> True
    _ -> False
  }
  |> should.be_true
}

pub fn error_connection_failed_clean_message_test() {
  let result = ducky.connect("")

  case result {
    Error(error.ConnectionFailed(msg)) -> {
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
    Error(error.QuerySyntaxError(_)) -> True
    Error(error.DatabaseError(_)) -> True
    _ -> False
  }
  |> should.be_true
}

pub fn error_query_syntax_clean_message_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let result = ducky.query(conn, "SELEKT * FROM nonexistent")

  case result {
    Error(error.QuerySyntaxError(msg)) -> {
      string.contains(msg, "#(")
      |> should.be_false
    }
    Error(error.DatabaseError(msg)) -> {
      string.contains(msg, "#(")
      |> should.be_false
    }
    _ -> panic as "Expected QuerySyntaxError or DatabaseError"
  }
}

pub fn error_unsupported_param_type_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let result =
    ducky.query_params(conn, "SELECT ?", [types.Timestamp(1_705_315_845)])

  case result {
    Error(error.UnsupportedParameterType(msg)) -> {
      // Message should describe the unsupported types
      string.contains(msg, "Timestamp")
      |> should.be_true
    }
    _ -> panic as "Expected UnsupportedParameterType error"
  }
}
