// Parameterized queries prevent SQL injection

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
      "CREATE TABLE users (id INT, name VARCHAR, active BOOL)",
    ))

    // Insert with parameters
    use _ <- result.try(
      ducky.query_params(conn, "INSERT INTO users VALUES (?, ?, ?)", [
        types.Integer(1),
        types.Text("Alice"),
        types.Boolean(True),
      ]),
    )

    // Query with parameters (safe from SQL injection)
    let user_input = "Alice"
    use df <- result.try(
      ducky.query_params(conn, "SELECT * FROM users WHERE name = ?", [
        types.Text(user_input),
      ]),
    )

    // NULL values
    use _ <- result.try(
      ducky.query_params(conn, "INSERT INTO users VALUES (?, ?, ?)", [
        types.Integer(2),
        types.Null,
        types.Boolean(False),
      ]),
    )

    // Close connection
    let _ = ducky.close(conn)
    Ok(df)
  }

  case result {
    Ok(df) -> io.println(string.inspect(df))
    Error(err) -> io.println(string.inspect(err))
  }
}
