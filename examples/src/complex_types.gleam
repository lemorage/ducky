// Complex types: STRUCT, temporal types, and LIST

import ducky
import ducky/types
import gleam/io
import gleam/list
import gleam/option
import gleam/result
import gleam/string

pub fn main() {
  let result = {
    use conn <- result.try(ducky.connect(":memory:"))

    // STRUCT types with field accessor
    use struct_df <- result.try(ducky.query(
      conn,
      "SELECT
        {'name': 'Alice', 'age': 30} as person,
        {'x': 10, 'y': 20} as point",
    ))

    let points =
      struct_df.rows
      |> list.filter_map(fn(row) {
        case types.get(row, 1) {
          option.Some(point) ->
            case types.field(point, "x"), types.field(point, "y") {
              option.Some(types.Integer(x)), option.Some(types.Integer(y)) ->
                Ok(Point(x, y))
              _, _ -> Error(Nil)
            }
          _ -> Error(Nil)
        }
      })

    // Nested STRUCT
    use nested <- result.try(ducky.query(
      conn,
      "SELECT {
        'user': {'name': 'Bob', 'id': 42},
        'active': true
      } as data",
    ))

    // Temporal types
    use temporal <- result.try(ducky.query(
      conn,
      "SELECT
        TIMESTAMP '2024-01-15 10:30:45' as ts,
        DATE '2024-12-25' as date,
        TIME '14:30:00' as time,
        INTERVAL '2 days' as duration",
    ))

    let times =
      temporal.rows
      |> list.filter_map(fn(row) {
        case types.get(row, 0), types.get(row, 1) {
          option.Some(types.Timestamp(ts)), option.Some(types.Date(days)) ->
            Ok(#(ts, days))
          _, _ -> Error(Nil)
        }
      })

    // LIST types
    use lists <- result.try(ducky.query(
      conn,
      "SELECT
        [1, 2, 3] as numbers,
        ['apple', 'banana'] as fruits,
        [[1, 2], [3, 4]] as matrix",
    ))

    let matrices =
      lists.rows
      |> list.filter_map(fn(row) {
        case types.get(row, 2) {
          option.Some(types.List(outer)) ->
            case outer {
              [types.List(first), ..] -> Ok(first)
              _ -> Error(Nil)
            }
          _ -> Error(Nil)
        }
      })

    // Combined: Event with nested data
    use _ <- result.try(ducky.query(
      conn,
      "CREATE TABLE events (
        id INT,
        data STRUCT(
          name VARCHAR,
          timestamp TIMESTAMP,
          attendees VARCHAR[]
        )
      )",
    ))

    use _ <- result.try(ducky.query(
      conn,
      "INSERT INTO events VALUES (
        1,
        {
          'name': 'Conference',
          'timestamp': TIMESTAMP '2024-06-15 09:00:00',
          'attendees': ['Alice', 'Bob', 'Charlie']
        }
      )",
    ))

    use events <- result.try(ducky.query(conn, "SELECT * FROM events"))

    let parsed =
      events.rows
      |> list.filter_map(fn(row) {
        case types.get(row, 1) {
          option.Some(data) ->
            case
              types.field(data, "name"),
              types.field(data, "timestamp"),
              types.field(data, "attendees")
            {
              option.Some(types.Text(name)),
                option.Some(types.Timestamp(ts)),
                option.Some(types.List(attendees))
              -> Ok(Event(name, ts, attendees))
              _, _, _ -> Error(Nil)
            }
          _ -> Error(Nil)
        }
      })

    let _ = ducky.close(conn)
    Ok(#(points, nested, times, matrices, parsed))
  }

  case result {
    Ok(data) -> io.println(string.inspect(data))
    Error(err) -> io.println(string.inspect(err))
  }
}

type Point {
  Point(x: Int, y: Int)
}

type Event {
  Event(name: String, timestamp: Int, attendees: List(types.Value))
}
