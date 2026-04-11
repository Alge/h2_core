import gleam/option.{None}
import h2_core.{
  type Connection, Cancel, Header, InvalidStreamState, NoError, ProtocolError,
  StreamReset, WithIndexing, receive_data, send_headers, send_rst_stream,
}
import h2_core/internal/stream.{
  Closed, HalfClosedLocal, HalfClosedRemote, ReservedLocal, ReservedRemote,
}
import h2_frame
import helper

// =============================================================================
// Helpers
// =============================================================================

fn server_with_open_stream() -> #(Connection, Connection) {
  helper.server_with_open_stream()
}

fn server_with_half_closed_remote_stream() -> #(Connection, Connection) {
  helper.server_with_half_closed_remote_stream()
}

// Helper: create a client that received a PUSH_PROMISE, putting promised
// stream 2 in reserved (remote) state
fn client_with_reserved_remote_stream() -> #(Connection, Int) {
  let #(_server, client, promised_id) =
    helper.client_with_reserved_remote_stream()
  #(client, promised_id)
}

fn server_with_reserved_local_stream() -> #(Connection, Int) {
  let #(server, _client, promised_id) =
    helper.server_with_reserved_local_stream()
  #(server, promised_id)
}

// Helper: create a server with a closed stream 1
// (client sent END_STREAM, server sent RST_STREAM)
fn server_with_closed_stream() -> #(Connection, Connection) {
  let #(server, client) = server_with_half_closed_remote_stream()
  // Server sends RST_STREAM to fully close the stream
  let assert Ok(#(server, _to_send)) = send_rst_stream(server, 1, NoError)
  let assert Ok(Closed) = h2_core.get_stream_state(server, 1)
  #(server, client)
}

// =============================================================================
// Reserved (local) state - RFC 9113 Section 5.1
// =============================================================================

// RFC 9113 Section 5.1 (reserved local):
// "An endpoint MUST NOT send any type of frame other than HEADERS,
// RST_STREAM, or PRIORITY in this state."
//
// Test: sending DATA on a reserved (local) stream should be an error.
pub fn send_data_on_reserved_local_stream_is_error_test() {
  let #(server, promised_id) = server_with_reserved_local_stream()
  let assert Ok(ReservedLocal) = h2_core.get_stream_state(server, promised_id)

  // Attempt to send DATA on reserved (local) stream 2 - should fail
  let assert Error(InvalidStreamState) =
    h2_core.send_data(server, promised_id, <<"illegal":utf8>>, False, None)
}

// =============================================================================
// Reserved (remote) state - RFC 9113 Section 5.1
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
  let #(client, promised_id) = client_with_reserved_remote_stream()
  let assert Ok(ReservedRemote) = h2_core.get_stream_state(client, promised_id)

  // Attempt to send DATA on reserved (remote) stream 2 - should fail
  let assert Error(InvalidStreamState) =
    h2_core.send_data(client, promised_id, <<"illegal":utf8>>, False, None)
}

// RFC 9113 Section 5.1 (reserved remote):
// "Receiving any type of frame other than HEADERS, RST_STREAM, or
// PRIORITY on a stream in this state MUST be treated as a connection
// error (Section 5.4.1) of type PROTOCOL_ERROR."
//
// WINDOW_UPDATE is not in the allowed list for reserved (remote).
pub fn receive_window_update_on_reserved_remote_is_protocol_error_test() {
  let #(client, promised_id) = client_with_reserved_remote_stream()
  let assert Ok(ReservedRemote) = h2_core.get_stream_state(client, promised_id)

  let assert Ok(wu) =
    h2_frame.encode_window_update(
      stream_id: promised_id,
      window_size_increment: 1024,
    )
  let assert Error(h2_core.ConnectionError(ProtocolError)) =
    receive_data(client, wu)
}

// =============================================================================
// Reserved (local) state - receiving restrictions - RFC 9113 Section 5.1
// =============================================================================

// RFC 9113 Section 5.1 (reserved local):
// "Receiving any type of frame other than RST_STREAM, PRIORITY, or
// WINDOW_UPDATE on a stream in this state MUST be treated as a
// connection error (Section 5.4.1) of type PROTOCOL_ERROR."
//
// HEADERS is not in the allowed list for reserved (local).
pub fn receive_headers_on_reserved_local_is_protocol_error_test() {
  let #(server, promised_id) = server_with_reserved_local_stream()
  let assert Ok(ReservedLocal) = h2_core.get_stream_state(server, promised_id)

  // Manually craft a HEADERS frame on the promised stream with valid HPACK
  // Length=3, Type=0x01, Flags=0x04 (END_HEADERS)
  // HPACK: 0x82 = :method GET, 0x87 = :scheme https, 0x84 = :path /
  let bad_headers = <<
    3:size(24), 0x01:size(8), 0x04:size(8), 0:size(1), promised_id:size(31),
    0x82, 0x87, 0x84,
  >>
  let assert Error(h2_core.ConnectionError(ProtocolError)) =
    receive_data(server, bad_headers)
}

// =============================================================================
// Reserved (remote) state - transitions - RFC 9113 Section 5.1
// =============================================================================

// RFC 9113 Section 5.1 (reserved remote):
// "Receiving a HEADERS frame causes the stream to transition to
// 'half-closed (local)'."
//
// This is the normal push response flow: after receiving PUSH_PROMISE,
// the server sends HEADERS on the promised stream to deliver the response.
pub fn receive_headers_on_reserved_remote_transitions_to_half_closed_local_test() {
  let #(client, promised_id) = client_with_reserved_remote_stream()
  let assert Ok(ReservedRemote) = h2_core.get_stream_state(client, promised_id)

  // Server sends HEADERS on promised stream 2
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: promised_id,
      end_stream: False,
      end_headers: True,
      priority: None,
      field_block_fragment: <<0x88>>,
      padding: None,
    )
  let assert Ok(#(client, _events, _to_send)) =
    receive_data(client, headers_frame)
  let assert Ok(HalfClosedLocal) = h2_core.get_stream_state(client, promised_id)
}

// RFC 9113 Section 5.1: "the END_STREAM flag is processed as a separate event
// to the frame that bears it; a HEADERS frame with the END_STREAM flag set can
// cause two state transitions."
// reserved (remote) --recv H--> half-closed (local) --recv ES--> closed
pub fn receive_headers_end_stream_on_reserved_remote_transitions_to_closed_test() {
  let #(client, promised_id) = client_with_reserved_remote_stream()
  let assert Ok(ReservedRemote) = h2_core.get_stream_state(client, promised_id)

  // Server sends HEADERS+END_STREAM on promised stream
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: promised_id,
      end_stream: True,
      end_headers: True,
      priority: None,
      field_block_fragment: <<0x88>>,
      padding: None,
    )
  let assert Ok(#(client, _events, _to_send)) =
    receive_data(client, headers_frame)
  let assert Ok(Closed) = h2_core.get_stream_state(client, promised_id)
}

// RFC 9113 Section 5.1 (reserved remote):
// "Either endpoint can send a RST_STREAM frame to cause the stream
// to become 'closed'. This releases the stream reservation."
pub fn receive_rst_stream_on_reserved_remote_transitions_to_closed_test() {
  let #(client, promised_id) = client_with_reserved_remote_stream()
  let assert Ok(ReservedRemote) = h2_core.get_stream_state(client, promised_id)

  // Server sends RST_STREAM on promised stream 2
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(
      stream_id: promised_id,
      error_code: h2_frame.Cancel,
    )
  let assert Ok(#(client, events, _to_send)) = receive_data(client, rst)
  assert events == [StreamReset(stream_id: promised_id, error_code: Cancel)]
  let assert Ok(Closed) = h2_core.get_stream_state(client, promised_id)
}

// =============================================================================
// Reserved (local) state - transitions - RFC 9113 Section 5.1
// =============================================================================

// RFC 9113 Section 5.1 (reserved local):
// "Either endpoint can send a RST_STREAM frame to cause the stream
// to become 'closed'. This releases the stream reservation."
//
// The client rejects a push by sending RST_STREAM on the reserved stream.
pub fn receive_rst_stream_on_reserved_local_transitions_to_closed_test() {
  let #(server, promised_id) = server_with_reserved_local_stream()
  let assert Ok(ReservedLocal) = h2_core.get_stream_state(server, promised_id)

  // Client sends RST_STREAM on promised stream to reject the push
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(
      stream_id: promised_id,
      error_code: h2_frame.Cancel,
    )
  let assert Ok(#(server, events, _to_send)) = receive_data(server, rst)
  assert events == [StreamReset(stream_id: promised_id, error_code: Cancel)]
  let assert Ok(Closed) = h2_core.get_stream_state(server, promised_id)
}

// =============================================================================
// Half-closed (local) state - RFC 9113 Section 5.1
// =============================================================================

// RFC 9113 Section 5.1 (half-closed local):
// "A stream transitions from this state to 'closed' when a frame is
// received with the END_STREAM flag set or when either peer sends a
// RST_STREAM frame."
//
// Test: receiving RST_STREAM on a half-closed (local) stream transitions
// to closed.
pub fn receive_rst_stream_on_half_closed_local_transitions_to_closed_test() {
  let #(server, _client) = server_with_open_stream()
  // Server sends headers with END_STREAM → half-closed (local)
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      True,
    )
  let assert Ok(HalfClosedLocal) = h2_core.get_stream_state(server, 1)

  // Client sends RST_STREAM
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.Cancel)
  let assert Ok(#(server, events, _to_send)) = receive_data(server, rst)
  assert events == [StreamReset(stream_id: 1, error_code: Cancel)]
  let assert Ok(Closed) = h2_core.get_stream_state(server, 1)
}

// =============================================================================
// Half-closed (remote) state - RFC 9113 Section 5.1
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
  let assert Ok(HalfClosedRemote) = h2_core.get_stream_state(server, 1)

  // Receive WINDOW_UPDATE on half-closed (remote) stream 1 - should succeed
  let assert Ok(wu) =
    h2_frame.encode_window_update(stream_id: 1, window_size_increment: 1024)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, wu)

  // Verify the stream's send_window_size was updated
  let assert Ok(66_559) = h2_core.get_stream_send_window_size(server, 1)
}

// RFC 9113 Section 5.1 (half-closed remote):
// "If an endpoint receives additional frames, other than WINDOW_UPDATE,
// PRIORITY, or RST_STREAM, for a stream that is in this state, it MUST
// respond with a stream error (Section 5.4.2) of type STREAM_CLOSED."
//
// Test: receiving RST_STREAM on a half-closed (remote) stream is accepted.
pub fn receive_rst_stream_on_half_closed_remote_is_accepted_test() {
  let #(server, _client) = server_with_half_closed_remote_stream()
  let assert Ok(HalfClosedRemote) = h2_core.get_stream_state(server, 1)

  // Receive RST_STREAM on half-closed (remote) stream 1 - should succeed
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.Cancel)
  let assert Ok(#(server, events, _to_send)) = receive_data(server, rst)

  // Should emit a StreamReset event and transition to Closed
  assert events == [StreamReset(stream_id: 1, error_code: Cancel)]
  let assert Ok(Closed) = h2_core.get_stream_state(server, 1)
}

// =============================================================================
// Closed state - sending restrictions - RFC 9113 Section 5.1
// =============================================================================

// RFC 9113 Section 5.1 (closed):
// "An endpoint MUST NOT send frames other than PRIORITY on a closed stream."
//
// Test: send_data on a closed stream should be an error.
pub fn send_data_on_closed_stream_is_error_test() {
  let #(server, _client) = server_with_closed_stream()
  let assert Ok(Closed) = h2_core.get_stream_state(server, 1)

  // Attempt to send DATA on closed stream 1 - should fail
  let assert Error(InvalidStreamState) =
    h2_core.send_data(server, 1, <<"illegal":utf8>>, False, None)
}

// RFC 9113 Section 5.1 (closed):
// "An endpoint MUST NOT send frames other than PRIORITY on a closed stream."
//
// Test: send_rst_stream on a closed stream should be an error.
pub fn send_rst_stream_on_closed_stream_is_error_test() {
  let #(server, _client) = server_with_closed_stream()
  let assert Ok(Closed) = h2_core.get_stream_state(server, 1)

  let assert Error(InvalidStreamState) = send_rst_stream(server, 1, Cancel)
}

// RFC 9113 Section 5.1 (closed):
// "An endpoint MUST NOT send frames other than PRIORITY on a closed stream."
//
// Test: acknowledge_data on a closed stream should be an error.
pub fn acknowledge_data_on_closed_stream_is_error_test() {
  let #(server, _client) = server_with_closed_stream()
  let assert Ok(Closed) = h2_core.get_stream_state(server, 1)

  let assert Error(InvalidStreamState) =
    h2_core.acknowledge_data(server, 1, 1024)
}

// =============================================================================
// Closed state - receiving grace period - RFC 9113 Section 5.1
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
  let assert Ok(Closed) = h2_core.get_stream_state(server, 1)

  // Receive WINDOW_UPDATE on closed stream 1 - should be silently discarded
  let assert Ok(wu) =
    h2_frame.encode_window_update(stream_id: 1, window_size_increment: 1024)
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
  let assert Ok(Closed) = h2_core.get_stream_state(server, 1)

  // Receive RST_STREAM on closed stream 1 - should be silently discarded
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.Cancel)
  let assert Ok(#(_server, events, to_send)) = receive_data(server, rst)

  // No events should be emitted and no frames should be sent
  assert events == []
  assert to_send == <<>>
}
