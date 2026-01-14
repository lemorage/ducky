import ducky
import ducky/types
import gleam/list
import gleam/result
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
