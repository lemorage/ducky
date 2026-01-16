// Transactions commit on success, rollback on error

import ducky
import ducky/types
import gleam/io
import gleam/result
import gleam/string

pub fn main() {
  let result = {
    // Connect to database
    use conn <- result.try(ducky.connect(":memory:"))

    // Create table
    use _ <- result.try(ducky.query(
      conn,
      "CREATE TABLE accounts (id INT, balance INT)",
    ))

    // Insert data
    use _ <- result.try(ducky.query(
      conn,
      "INSERT INTO accounts VALUES (1, 1000), (2, 500)",
    ))

    // Successful transaction (commits automatically)
    let transfer =
      ducky.transaction(conn, fn(conn) {
        use _ <- result.try(
          ducky.query_params(
            conn,
            "UPDATE accounts SET balance = balance - ? WHERE id = ?",
            [types.Integer(200), types.Integer(1)],
          ),
        )

        ducky.query_params(
          conn,
          "UPDATE accounts SET balance = balance + ? WHERE id = ?",
          [types.Integer(200), types.Integer(2)],
        )
      })

    io.println(string.inspect(transfer))

    // Failed transaction (rolls back automatically)
    let failed =
      ducky.transaction(conn, fn(conn) {
        use _ <- result.try(
          ducky.query_params(
            conn,
            "UPDATE accounts SET balance = balance - ? WHERE id = ?",
            [types.Integer(100), types.Integer(1)],
          ),
        )

        ducky.query(conn, "INVALID SQL")
      })

    io.println(string.inspect(failed))

    // Verify rollback worked
    ducky.query(conn, "SELECT * FROM accounts")
  }

  case result {
    Ok(df) -> io.println(string.inspect(df))
    Error(err) -> io.println(string.inspect(err))
  }
}
