// Prepared statements for repeated execution

import ducky
import gleam/int
import gleam/io
import gleam/list
import gleam/result
import gleam/string

pub fn main() {
  let result = {
    use conn <- result.try(ducky.connect(":memory:"))

    use _ <- result.try(ducky.exec(
      conn,
      "CREATE TABLE users (id INT, name VARCHAR)",
    ))

    // Prepare once, execute many times
    use stmt <- result.try(ducky.prepare(
      conn,
      "INSERT INTO users VALUES (?, ?)",
    ))

    let names = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
    use _ <- result.try(
      list.index_map(names, fn(name, i) { #(i + 1, name) })
      |> list.try_each(fn(pair) {
        let #(id, name) = pair
        ducky.execute(stmt, [ducky.int(id), ducky.text(name)])
        |> result.map(fn(_) { Nil })
      }),
    )

    // Finalize when done
    use _ <- result.try(ducky.finalize(stmt))

    // Verify results
    use df <- result.try(ducky.query(conn, "SELECT * FROM users ORDER BY id"))
    io.println("Inserted " <> int.to_string(list.length(df.rows)) <> " users")

    // with_statement handles finalization automatically
    use _ <- result.try(
      ducky.with_statement(conn, "SELECT * FROM users WHERE id = ?", fn(stmt) {
        use df <- result.map(ducky.execute(stmt, [ducky.int(3)]))
        io.println("Found: " <> string.inspect(df.rows))
      }),
    )

    let _ = ducky.close(conn)
    Ok(Nil)
  }

  case result {
    Ok(_) -> Nil
    Error(err) -> io.println(string.inspect(err))
  }
}
