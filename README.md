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
import ducky/types

use conn <- ducky.with_connection("data.db")
ducky.query_params(conn, "SELECT * FROM users WHERE id = ?", [
  types.Integer(42)
])
```

See [examples/](https://github.com/lemorage/ducky/tree/master/examples) for complete usage patterns.

## License

Apache-2.0
