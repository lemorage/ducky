//// Error types for DuckDB operations.

/// Errors that can occur during DuckDB operations.
pub type Error {
  /// Connection to database failed.
  ConnectionFailed(reason: String)
  /// SQL query has syntax errors.
  QuerySyntaxError(message: String)
  /// Operation timed out.
  Timeout(duration_ms: Int)
  /// Type conversion failed.
  TypeMismatch(expected: String, got: String)
  /// Unsupported parameter type in query.
  UnsupportedParameterType(type_name: String)
  /// Generic error from DuckDB.
  DatabaseError(message: String)
}
