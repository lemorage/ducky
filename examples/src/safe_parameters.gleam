// Parameterized queries prevent SQL injection

import ducky
import gleam/io
import gleam/result
import gleam/string

pub fn main() {
  let result = {
    // Connect to database
    use conn <- result.try(ducky.connect(":memory:"))

    // Create table
    use _ <- result.try(ducky.exec(
      conn,
      "CREATE TABLE users (id INT, name VARCHAR, active BOOL)",
    ))

    // Insert with parameters
    use _ <- result.try(
      ducky.query_params(conn, "INSERT INTO users VALUES (?, ?, ?)", [
        ducky.int(1),
        ducky.text("Alice"),
        ducky.bool(True),
      ]),
    )

    // Query with parameters (safe from SQL injection)
    let user_input = "Alice"
    use df <- result.try(
      ducky.query_params(conn, "SELECT * FROM users WHERE name = ?", [
        ducky.text(user_input),
      ]),
    )

    // NULL values
    use _ <- result.try(
      ducky.query_params(conn, "INSERT INTO users VALUES (?, ?, ?)", [
        ducky.int(2),
        ducky.null(),
        ducky.bool(False),
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
