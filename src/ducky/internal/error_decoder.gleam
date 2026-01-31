//// Internal error decoding utilities.

import ducky/error.{type Error}
import gleam/dynamic
import gleam/dynamic/decode
import gleam/string

/// Converts an Erlang atom to a String.
@external(erlang, "erlang", "atom_to_binary")
fn atom_to_string(atom: dynamic.Dynamic) -> String

/// Decodes an error from the NIF layer.
///
/// The NIF returns errors as `#(error, #(error_type, message))` where
/// error_type is one of: connection_failed, query_syntax_error,
/// unsupported_parameter_type, database_error.
pub fn decode_nif_error(err: dynamic.Dynamic) -> Error {
  // Structure: #(error, #(error_type_atom, message_string))
  let inner_decoder = {
    use error_type_dyn <- decode.subfield([0], decode.dynamic)
    use message <- decode.subfield([1], decode.string)

    let error_type = case dynamic.classify(error_type_dyn) {
      "Atom" -> atom_to_string(error_type_dyn)
      _ -> "unknown"
    }

    decode.success(#(error_type, message))
  }

  // Extract inner tuple at index 1
  let decoder = decode.at([1], inner_decoder)

  case decode.run(err, decoder) {
    Ok(#("connection_failed", msg)) -> error.ConnectionFailed(msg)
    Ok(#("query_syntax_error", msg)) -> error.QuerySyntaxError(msg)
    Ok(#("unsupported_parameter_type", msg)) ->
      error.UnsupportedParameterType(msg)
    Ok(#("database_error", msg)) -> error.DatabaseError(msg)
    Ok(#(unknown_type, msg)) ->
      error.DatabaseError("[" <> unknown_type <> "] " <> msg)
    Error(_) -> fallback_decode(err)
  }
}

/// Fallback decoder for unexpected error formats.
fn fallback_decode(err: dynamic.Dynamic) -> Error {
  // Try direct tuple decode for simpler error formats
  let simple_decoder = {
    use error_type_dyn <- decode.subfield([0], decode.dynamic)
    use message <- decode.subfield([1], decode.string)

    let error_type = case dynamic.classify(error_type_dyn) {
      "Atom" -> atom_to_string(error_type_dyn)
      _ -> "unknown"
    }

    decode.success(#(error_type, message))
  }

  case decode.run(err, simple_decoder) {
    Ok(#("connection_failed", msg)) -> error.ConnectionFailed(msg)
    Ok(#("query_syntax_error", msg)) -> error.QuerySyntaxError(msg)
    Ok(#("unsupported_parameter_type", msg)) ->
      error.UnsupportedParameterType(msg)
    Ok(#("database_error", msg)) -> error.DatabaseError(msg)
    Ok(#(unknown_type, msg)) ->
      error.DatabaseError("[" <> unknown_type <> "] " <> msg)
    Error(_) -> error.DatabaseError("Unknown error: " <> string.inspect(err))
  }
}
