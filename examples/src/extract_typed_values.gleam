// Pattern matching and extracting typed values

import ducky
import ducky/types
import gleam/io
import gleam/list
import gleam/option
import gleam/result
import gleam/string

pub fn main() {
  let result = {
    // Connect to database
    use conn <- result.try(ducky.connect(":memory:"))

    // Create table
    use _ <- result.try(ducky.query(
      conn,
      "CREATE TABLE products (id INT, name VARCHAR, price FLOAT, active BOOL)",
    ))

    // Insert data
    use _ <- result.try(ducky.query(
      conn,
      "INSERT INTO products VALUES (1, 'Widget', 19.99, true), (2, 'Gadget', 29.99, false)",
    ))

    // Query data
    use df <- result.try(ducky.query(conn, "SELECT * FROM products"))

    // Access DataFrame structure
    io.println(string.inspect(df.columns))
    io.println(string.inspect(list.length(df.rows)))

    // Pattern match to extract values
    list.each(df.rows, fn(row) {
      case types.get(row, 0), types.get(row, 1), types.get(row, 2) {
        option.Some(types.Integer(id)),
          option.Some(types.Text(name)),
          option.Some(types.Double(price))
        -> io.println(string.inspect(#(id, name, price)))
        _, _, _ -> Nil
      }
    })

    // Filter rows
    let active =
      df.rows
      |> list.filter(fn(row) {
        case types.get(row, 3) {
          option.Some(types.Boolean(True)) -> True
          _ -> False
        }
      })

    io.println(string.inspect(list.length(active)))

    // Map to custom type
    let products =
      df.rows
      |> list.filter_map(fn(row) {
        case types.get(row, 0), types.get(row, 1) {
          option.Some(types.Integer(id)), option.Some(types.Text(name)) ->
            Ok(Product(id, name))
          _, _ -> Error(Nil)
        }
      })

    io.println(string.inspect(products))

    // Close connection
    let _ = ducky.close(conn)
    Ok(Nil)
  }

  case result {
    Ok(_) -> Nil
    Error(err) -> io.println(string.inspect(err))
  }
}

type Product {
  Product(Int, String)
}
