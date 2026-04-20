// Complex types: STRUCT, LIST, ARRAY, MAP, DECIMAL, temporal

import ducky
import gleam/io
import gleam/list
import gleam/option
import gleam/result
import gleam/string

pub fn main() {
  let result = {
    use conn <- result.try(ducky.connect(":memory:"))

    // STRUCT with field accessor
    use structs <- result.try(ducky.query(
      conn,
      "SELECT {'name': 'Alice', 'age': 30} as person",
    ))

    let names =
      structs.rows
      |> list.filter_map(fn(row) {
        case ducky.get(row, 0) {
          option.Some(person) ->
            case ducky.field(person, "name") {
              option.Some(ducky.Text(name)) -> Ok(name)
              _ -> Error(Nil)
            }
          _ -> Error(Nil)
        }
      })

    // LIST type
    use lists <- result.try(ducky.query(
      conn,
      "SELECT [1, 2, 3] as numbers, [[1, 2], [3, 4]] as matrix",
    ))

    // ARRAY type
    use arrays <- result.try(ducky.query(
      conn,
      "SELECT array_value(1, 2, 3) as nums",
    ))

    // MAP type
    use maps <- result.try(ducky.query(
      conn,
      "SELECT map {'a': 1, 'b': 2} as lookup",
    ))

    // DECIMAL: lossless precision
    use decimals <- result.try(ducky.query(
      conn,
      "SELECT 123.456789012345678901::DECIMAL(30,18) as price",
    ))

    // Temporal query results
    use temporals <- result.try(ducky.query(
      conn,
      "SELECT
        TIMESTAMP '2024-01-15 10:30:45' as ts,
        DATE '2024-12-25' as d,
        TIME '14:30:00' as t,
        INTERVAL '2 days' as dur",
    ))

    // Temporal parameter binding
    use _ <- result.try(ducky.exec(
      conn,
      "CREATE TABLE events (id INT, ts TIMESTAMP, d DATE, t TIME, dur INTERVAL)",
    ))

    use _ <- result.try(
      ducky.query_params(conn, "INSERT INTO events VALUES (?, ?, ?, ?, ?)", [
        ducky.int(1),
        ducky.timestamp(1_704_067_200_000_000),
        ducky.date(19_723),
        ducky.time(43_200_000_000),
        ducky.interval(months: 0, days: 7, nanos: 0),
      ]),
    )

    // Decimal parameter binding
    use _ <- result.try(ducky.exec(
      conn,
      "CREATE TABLE prices (amount DECIMAL(18,2))",
    ))

    use _ <- result.try(
      ducky.query_params(conn, "INSERT INTO prices VALUES (?)", [
        ducky.decimal("99999.99"),
      ]),
    )

    use prices <- result.try(ducky.query(conn, "SELECT * FROM prices"))

    let _ = ducky.close(conn)
    Ok(#(names, lists, arrays, maps, decimals, temporals, prices))
  }

  case result {
    Ok(data) -> io.println(string.inspect(data))
    Error(err) -> io.println(string.inspect(err))
  }
}
