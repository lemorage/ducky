// Automatic connection cleanup

import ducky
import ducky/types
import gleam/io
import gleam/result
import gleam/string

pub fn main() {
  // with_connection closes automatically
  let result =
    ducky.with_connection(":memory:", fn(conn) {
      // Create table
      use _ <- result.try(ducky.query(
        conn,
        "CREATE TABLE products (id INT, name VARCHAR)",
      ))

      // Insert data
      use _ <- result.try(
        ducky.query_params(conn, "INSERT INTO products VALUES (?, ?)", [
          types.Integer(1),
          types.Text("Widget"),
        ]),
      )

      // Query data
      ducky.query(conn, "SELECT * FROM products")
      // Connection closes automatically
    })

  case result {
    Ok(df) -> io.println(string.inspect(df))
    Error(err) -> io.println(string.inspect(err))
  }

  // Connection closes even on error
  let error_case =
    ducky.with_connection(":memory:", fn(conn) {
      use _ <- result.try(ducky.query(conn, "CREATE TABLE test (id INT)"))
      ducky.query(conn, "INVALID SQL")
    })

  case error_case {
    Ok(df) -> io.println(string.inspect(df))
    Error(err) -> io.println(string.inspect(err))
  }
}
