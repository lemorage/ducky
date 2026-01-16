// Basic query operations

import ducky
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
      "CREATE TABLE users (id INT, name VARCHAR, age INT)",
    ))

    // Insert data
    use _ <- result.try(ducky.query(
      conn,
      "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
    ))

    // Query data
    use df <- result.try(ducky.query(conn, "SELECT * FROM users"))

    // Close connection
    let _ = ducky.close(conn)
    Ok(df)
  }

  case result {
    Ok(df) -> io.println(string.inspect(df))
    Error(err) -> io.println(string.inspect(err))
  }
}
