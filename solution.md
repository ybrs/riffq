## Authentication Callback Implementation

This update introduces a new authentication callback that is invoked from Rust
whenever a client connects.  The callback receives `(user, password, database,
host)` and must return a boolean indicating whether the connection is allowed.
A new `set_auth_callback` method is available on the `Server` Python class.

The startup handler was rewritten to send a `CleartextPassword` request and
validate credentials by calling the Python callback.  On success the normal
startup flow continues; otherwise an error is returned and the connection is
closed.

Tests were extended to exercise the new behaviour and the concurrency test was
skipped because it requires additional heavy dependencies.
