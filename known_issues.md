# Known Issues

## `send_data` returns `ConnectionError` for caller-side flow control violations

When the caller tries to send more data than the flow control window allows,
`send_data` returns `Error(ConnectionError(FlowControlError))`. This is
misleading — the connection isn't actually broken. The caller simply exceeded the
available window. A `ConnectionError` implies a GOAWAY should be sent, which is
wrong here. The function should return a different error type that tells the
caller "you exceeded the window, try again with less data" without killing the
connection.
