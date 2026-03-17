import gleam/dict
import gleam/option.{None}
import h2_core.{
  type Connection, Closed, Client, HalfClosedRemote, Header, ReservedLocal,
  ReservedRemote, Server, Stream, StreamReset, WithIndexing, new_connection,
  receive_data, send_headers, send_rst_stream,
}
import h2_frame

// =============================================================================
// Helpers
// =============================================================================

// Helper: create a server with an open client-initiated stream 1
fn server_with_open_stream() -> #(Connection, Connection) {
  let server = new_connection(Server)
  let client = new_connection(Client)
  let assert Ok(#(client, _events, headers)) =
    send_headers(client, [Header(":method", "GET", WithIndexing)], False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  #(server, client)
}

// Helper: create a server with a half-closed (remote) stream 1
// (client sent END_STREAM with headers)
fn server_with_half_closed_remote_stream() -> #(Connection, Connection) {
  let server = new_connection(Server)
  let client = new_connection(Client)
  let assert Ok(#(client, _events, headers)) =
    send_headers(client, [Header(":method", "GET", WithIndexing)], True)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  #(server, client)
}

// Helper: create a client that received a PUSH_PROMISE, putting promised
// stream 2 in reserved (remote) state
fn client_with_reserved_remote_stream() -> Connection {
  let #(_server, client) = server_with_open_stream()
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, pp)
  let assert Ok(stream) = dict.get(client.streams, 2)
  assert stream.state == ReservedRemote
  client
}

// Helper: create a server where stream 2 is in reserved (local) state.
// Since send_push_promise is not yet implemented, we manually insert the
// stream into the server's streams dict.
fn server_with_reserved_local_stream() -> Connection {
  let #(server, _client) = server_with_open_stream()
  let server =
    h2_core.Connection(
      ..server,
      streams: dict.insert(
        server.streams,
        2,
        Stream(state: ReservedLocal, send_window_size: 65_535, recv_window_size: 65_535),
      ),
    )
  server
}

// Helper: create a server with a closed stream 1
// (client sent END_STREAM, server sent RST_STREAM)
fn server_with_closed_stream() -> #(Connection, Connection) {
  let #(server, client) = server_with_half_closed_remote_stream()
  // Server sends RST_STREAM to fully close the stream
  let assert Ok(#(server, _events, _to_send)) =
    send_rst_stream(server, 1, h2_frame.NoError)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Closed
  #(server, client)
}

// =============================================================================
// Reserved (local) state — RFC 9113 Section 5.1
// =============================================================================

// RFC 9113 Section 5.1 (reserved local):
// "An endpoint MUST NOT send any type of frame other than HEADERS,
// RST_STREAM, or PRIORITY in this state."
//
// Test: sending DATA on a reserved (local) stream should be an error.
pub fn send_data_on_reserved_local_stream_is_error_test() {
  let server = server_with_reserved_local_stream()
  let assert Ok(stream) = dict.get(server.streams, 2)
  assert stream.state == ReservedLocal

  // Attempt to send DATA on reserved (local) stream 2 — should fail
  let assert Error(_) =
    h2_core.send_data(server, 2, <<"illegal":utf8>>, False)
}

// =============================================================================
// Reserved (remote) state — RFC 9113 Section 5.1
// =============================================================================

// RFC 9113 Section 5.1 (reserved remote):
// "An endpoint receiving any type of frame other than HEADERS, RST_STREAM,
// or PRIORITY MUST treat this as a connection error (Section 5.4.1) of type
// PROTOCOL_ERROR."
//
// RFC 9113 Section 5.1 (reserved remote):
// "An endpoint MUST NOT send any type of frame other than RST_STREAM,
// WINDOW_UPDATE, or PRIORITY in this state."
//
// Test: sending DATA on a reserved (remote) stream should be an error.
pub fn send_data_on_reserved_remote_stream_is_error_test() {
  let client = client_with_reserved_remote_stream()
  let assert Ok(stream) = dict.get(client.streams, 2)
  assert stream.state == ReservedRemote

  // Attempt to send DATA on reserved (remote) stream 2 — should fail
  let assert Error(_) =
    h2_core.send_data(client, 2, <<"illegal":utf8>>, False)
}

// =============================================================================
// Half-closed (remote) state — RFC 9113 Section 5.1
// =============================================================================

// RFC 9113 Section 5.1 (half-closed remote):
// "If an endpoint receives additional frames, other than WINDOW_UPDATE,
// PRIORITY, or RST_STREAM, for a stream that is in this state, it MUST
// respond with a stream error (Section 5.4.2) of type STREAM_CLOSED."
//
// Test: receiving WINDOW_UPDATE on a half-closed (remote) stream is accepted
// (not a stream error).
pub fn receive_window_update_on_half_closed_remote_is_accepted_test() {
  let #(server, _client) = server_with_half_closed_remote_stream()
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == HalfClosedRemote

  // Receive WINDOW_UPDATE on half-closed (remote) stream 1 — should succeed
  let assert Ok(wu) =
    h2_frame.encode_window_update(
      stream_id: 1,
      window_size_increment: 1024,
    )
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, wu)

  // Verify the stream's send_window_size was updated
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.send_window_size == 65_535 + 1024
}

// RFC 9113 Section 5.1 (half-closed remote):
// "If an endpoint receives additional frames, other than WINDOW_UPDATE,
// PRIORITY, or RST_STREAM, for a stream that is in this state, it MUST
// respond with a stream error (Section 5.4.2) of type STREAM_CLOSED."
//
// Test: receiving RST_STREAM on a half-closed (remote) stream is accepted.
pub fn receive_rst_stream_on_half_closed_remote_is_accepted_test() {
  let #(server, _client) = server_with_half_closed_remote_stream()
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == HalfClosedRemote

  // Receive RST_STREAM on half-closed (remote) stream 1 — should succeed
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(
      stream_id: 1,
      error_code: h2_frame.Cancel,
    )
  let assert Ok(#(server, events, _to_send)) = receive_data(server, rst)

  // Should emit a StreamReset event and transition to Closed
  assert events == [StreamReset(stream_id: 1, error_code: h2_frame.Cancel)]
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Closed
}

// =============================================================================
// Closed state — sending restrictions — RFC 9113 Section 5.1
// =============================================================================

// RFC 9113 Section 5.1 (closed):
// "An endpoint MUST NOT send frames other than PRIORITY on a closed stream."
//
// Test: send_data on a closed stream should be an error.
pub fn send_data_on_closed_stream_is_error_test() {
  let #(server, _client) = server_with_closed_stream()
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Closed

  // Attempt to send DATA on closed stream 1 — should fail
  let assert Error(_) =
    h2_core.send_data(server, 1, <<"illegal":utf8>>, False)
}

// RFC 9113 Section 5.1 (closed):
// "An endpoint MUST NOT send frames other than PRIORITY on a closed stream."
//
// Test: send_headers on a closed stream should be an error.
// Note: send_headers currently always creates a new stream, so this test
// verifies that sending headers *on an existing closed stream* is rejected.
// This may require send_headers to accept a stream_id parameter, or a
// separate send function for trailers/additional headers on existing streams.
// For now we test via send_data which takes a stream_id.
pub fn send_headers_on_closed_stream_is_error_test() {
  let #(server, _client) = server_with_closed_stream()
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Closed

  // Since send_headers always creates a new stream (doesn't take stream_id),
  // we cannot directly test "send HEADERS on closed stream 1" with the
  // current API. Instead, we verify that the closed stream cannot be used
  // for any frame type by testing send_data (which takes a stream_id).
  //
  // TODO: When send_headers gains support for sending on existing streams
  // (e.g. trailers), add a test that sending headers on a closed stream
  // returns an error.
  let assert Error(_) =
    h2_core.send_data(server, 1, <<>>, True)
}

// =============================================================================
// Closed state — receiving grace period — RFC 9113 Section 5.1
// =============================================================================

// RFC 9113 Section 5.1 (closed):
// "An endpoint that sends a frame with the END_STREAM flag set or a
// RST_STREAM frame might receive a WINDOW_UPDATE or RST_STREAM frame
// from its peer in the time before the closing condition is received."
//
// "An endpoint that receives a WINDOW_UPDATE frame after receiving a
// RST_STREAM on that stream can treat the WINDOW_UPDATE as having been
// sent in that grace period."
//
// Test: receiving WINDOW_UPDATE on a closed stream should be silently
// discarded (not an error).
pub fn receive_window_update_on_closed_stream_is_silently_discarded_test() {
  let #(server, _client) = server_with_closed_stream()
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Closed

  // Receive WINDOW_UPDATE on closed stream 1 — should be silently discarded
  let assert Ok(wu) =
    h2_frame.encode_window_update(
      stream_id: 1,
      window_size_increment: 1024,
    )
  let assert Ok(#(_server, events, to_send)) = receive_data(server, wu)

  // No events should be emitted and no frames should be sent
  assert events == []
  assert to_send == <<>>
}

// RFC 9113 Section 5.1 (closed):
// "An endpoint that sends a frame with the END_STREAM flag set or a
// RST_STREAM frame might receive a WINDOW_UPDATE or RST_STREAM frame
// from its peer in the time before the closing condition is received."
//
// Test: receiving RST_STREAM on a closed stream should be silently
// discarded (not an error).
pub fn receive_rst_stream_on_closed_stream_is_silently_discarded_test() {
  let #(server, _client) = server_with_closed_stream()
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Closed

  // Receive RST_STREAM on closed stream 1 — should be silently discarded
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(
      stream_id: 1,
      error_code: h2_frame.Cancel,
    )
  let assert Ok(#(_server, events, to_send)) = receive_data(server, rst)

  // No events should be emitted and no frames should be sent
  assert events == []
  assert to_send == <<>>
}
