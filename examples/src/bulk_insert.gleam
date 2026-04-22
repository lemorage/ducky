// Bulk insert with append_rows for maximum throughput

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
      "CREATE TABLE sensors (id INT, value DOUBLE, label VARCHAR)",
    ))

    // Build 1000 rows
    let rows =
      list.range(1, 1000)
      |> list.map(fn(i) {
        [
          ducky.int(i),
          ducky.float(int.to_float(i) *. 0.1),
          ducky.text("sensor_" <> int.to_string(i)),
        ]
      })

    // Bulk insert via DuckDB's appender API (bypasses SQL parsing)
    use count <- result.try(ducky.append_rows(conn, "sensors", rows))
    io.println("Appended " <> int.to_string(count) <> " rows")

    // Verify
    use df <- result.try(ducky.query(
      conn,
      "SELECT COUNT(*) as n, SUM(value) as total FROM sensors",
    ))
    io.println("Result: " <> string.inspect(df.rows))

    let _ = ducky.close(conn)
    Ok(Nil)
  }

  case result {
    Ok(_) -> Nil
    Error(err) -> io.println(string.inspect(err))
  }
}
