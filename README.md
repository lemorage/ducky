# ducky

Native DuckDB driver for Gleam.

[![Package Version](https://img.shields.io/hexpm/v/ducky)](https://hex.pm/packages/ducky)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/ducky/)

## Install

```sh
gleam add ducky
```

## Quick start

```gleam
import ducky
import gleam/int
import gleam/io

pub fn main() {
  use conn <- ducky.with_connection(":memory:")

  // Create our duck pond
  let assert Ok(_) = ducky.query(conn, "
    CREATE TABLE ducks (name TEXT, quack_volume INT, is_rubber BOOLEAN)
  ")
  let assert Ok(_) = ducky.query(conn, "
    INSERT INTO ducks VALUES
      ('Sir Quacksalot', 95, false),
      ('Duck Norris', 100, false),
      ('Mallard Fillmore', 72, false),
      ('Squeaky', 0, true)
  ")

  // Find the loudest quacker
  let assert Ok(result) = ducky.query(conn, "
    SELECT name, quack_volume FROM ducks
    WHERE is_rubber = false
    ORDER BY quack_volume DESC LIMIT 1
  ")

  case result.rows {
    [ducky.Row([ducky.Text(name), ducky.Integer(volume)])] ->
      io.println(name <> " wins at " <> int.to_string(volume) <> " decibels!")
    _ -> io.println("The pond is empty...")
  }
}
// => Duck Norris wins at 100 decibels!
```

See [examples/](https://github.com/lemorage/ducky/tree/master/examples) for complete usage patterns.

## License

Apache-2.0
