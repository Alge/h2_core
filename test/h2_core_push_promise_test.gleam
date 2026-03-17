import gleam/dict
import gleam/option.{None}
import h2_core.{
  type Connection, Client, ConnectionError, Header, PushPromiseReceived,
  ReservedLocal, ReservedRemote, Server, StreamError, WithIndexing,
  new_connection, receive_data, send_headers, send_settings,
}
import h2_frame

// Helper: create a server connection with an open client-initiated stream 1
fn server_with_open_stream() -> #(Connection, Connection) {
  let server = new_connection(Server)
  let client = new_connection(Client)
  let assert Ok(#(client, _events, headers)) =
    send_headers(client, [Header(":method", "GET", WithIndexing)], False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  #(server, client)
}

// =============================================================================
// Section 8.4 - "A client cannot push. Thus, servers MUST treat the receipt
// of a PUSH_PROMISE frame as a connection error (Section 5.4.1) of type
// PROTOCOL_ERROR."
// =============================================================================

pub fn receive_push_promise_from_client_is_protocol_error_test() {
  // A server receives a PUSH_PROMISE from a client, which is illegal.
  // Clients cannot push.
  let #(server, _client) = server_with_open_stream()
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, pp)
}

// =============================================================================
// Section 6.6 - "If the Stream Identifier field specifies the value 0x00,
// a recipient MUST respond with a connection error (Section 5.4.1) of type
// PROTOCOL_ERROR."
// =============================================================================

pub fn receive_push_promise_on_stream_zero_is_protocol_error_test() {
  let client = new_connection(Client)
  // Manually craft a PUSH_PROMISE on stream 0
  // Type=0x05, Flags=0x04 (END_HEADERS), Stream ID=0, Promised ID=2
  let bad_pp = <<
    4:size(24),
    0x05:size(8),
    0x04:size(8),
    0:size(1),
    0:size(31),
    0:size(1),
    2:size(31),
  >>
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(client, bad_pp)
}

// =============================================================================
// Section 6.6 - "PUSH_PROMISE MUST NOT be sent if the SETTINGS_ENABLE_PUSH
// setting of the peer endpoint is set to 0. An endpoint that has set this
// setting and has received acknowledgment MUST treat the receipt of a
// PUSH_PROMISE frame as a connection error (Section 5.4.1) of type
// PROTOCOL_ERROR."
// =============================================================================

pub fn receive_push_promise_when_push_disabled_is_protocol_error_test() {
  let client = new_connection(Client)
  let #(server, _client) = server_with_open_stream()

  // Client disables push
  let assert Ok(#(client, _events, settings_frame)) =
    h2_core.send_settings(client, [h2_frame.EnablePush(0)])

  // Server acknowledges (client receives the ack)
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Ok(#(client, _events, _to_send)) =
    receive_data(client, settings_ack)

  // Server sends the settings to its side
  let assert Ok(#(_server, _events, _to_send)) =
    receive_data(server, settings_frame)

  // Now server sends PUSH_PROMISE — client should reject it
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(client, pp)
}

// =============================================================================
// Section 6.6 - "PUSH_PROMISE frames MUST only be sent on a peer-initiated
// stream that is in either the 'open' or 'half-closed (remote)' state."
//
// Section 6.6 - "A receiver MUST treat the receipt of a PUSH_PROMISE on a
// stream that is neither 'open' nor 'half-closed (local)' as a connection
// error (Section 5.4.1) of type PROTOCOL_ERROR."
// =============================================================================

pub fn receive_push_promise_on_idle_stream_is_protocol_error_test() {
  let client = new_connection(Client)
  // Stream 1 was never opened — it's idle
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(client, pp)
}

pub fn receive_push_promise_on_open_stream_is_valid_test() {
  let #(_server, client) = server_with_open_stream()
  // Server pushes on stream 1 which is open
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Ok(#(client, events, _to_send)) = receive_data(client, pp)
  // Should emit PushPromiseReceived event
  assert events == [PushPromiseReceived(
    stream_id: 1,
    promised_stream_id: 2,
    headers: [],
  )]
  // Promised stream 2 should be in reserved (remote) state
  let assert Ok(stream) = dict.get(client.streams, 2)
  assert stream.state == ReservedRemote
}

pub fn receive_push_promise_on_half_closed_local_is_valid_test() {
  let server = new_connection(Server)
  let client = new_connection(Client)
  // Client sends headers with END_STREAM — stream 1 is half-closed (local)
  // on the client side
  let assert Ok(#(client, _events, headers)) =
    send_headers(client, [Header(":method", "GET", WithIndexing)], True)
  let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)

  // Server pushes on stream 1 — valid because it's half-closed (local)
  // from the client's perspective
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Ok(#(_client, events, _to_send)) = receive_data(client, pp)
  assert events == [PushPromiseReceived(
    stream_id: 1,
    promised_stream_id: 2,
    headers: [],
  )]
}

// =============================================================================
// Section 6.6 - "A receiver MUST treat the receipt of a PUSH_PROMISE that
// promises an illegal stream identifier (Section 5.1.1) as a connection error
// (Section 5.4.1) of type PROTOCOL_ERROR. Note that an illegal stream
// identifier is an identifier for a stream that is not currently in the
// 'idle' state."
// =============================================================================

pub fn receive_push_promise_with_odd_promised_id_is_protocol_error_test() {
  let #(_server, client) = server_with_open_stream()
  // Server promises stream 3 (odd) — only even IDs are server-initiated
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 3,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(client, pp)
}

pub fn receive_push_promise_with_zero_promised_id_is_protocol_error_test() {
  let #(_server, client) = server_with_open_stream()
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 0,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(client, pp)
}

pub fn receive_push_promise_with_already_used_id_is_protocol_error_test() {
  let #(_server, client) = server_with_open_stream()
  // First push reserves stream 2
  let assert Ok(pp1) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, pp1)

  // Second push tries to reuse stream 2 — not idle anymore
  let assert Ok(pp2) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(client, pp2)
}

// Section 5.1.1 - "The identifier of a newly established stream MUST be
// numerically greater than all streams that the initiating endpoint has
// opened or reserved."
pub fn receive_push_promise_with_decreasing_id_is_protocol_error_test() {
  let #(_server, client) = server_with_open_stream()
  // First push reserves stream 4
  let assert Ok(pp1) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 4,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, pp1)

  // Second push tries stream 2 — lower than 4, violates ordering
  let assert Ok(pp2) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(client, pp2)
}

// =============================================================================
// Section 6.6 - "The promised stream identifier MUST be a valid choice for
// the next stream sent by the sender (see 'new stream identifier' in
// Section 5.1.1)."
//
// Promised stream transitions to "reserved (remote)" on the receiver side.
// =============================================================================

pub fn receive_push_promise_reserves_promised_stream_test() {
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
}

// =============================================================================
// Section 6.6 - "A PUSH_PROMISE frame without the END_HEADERS flag set MUST
// be followed by a CONTINUATION frame for the same stream. A receiver MUST
// treat the receipt of any other type of frame or a frame on a different
// stream as a connection error (Section 5.4.1) of type PROTOCOL_ERROR."
// =============================================================================

pub fn receive_push_promise_without_end_headers_expects_continuation_test() {
  let #(_server, client) = server_with_open_stream()
  // PUSH_PROMISE without END_HEADERS
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: False,
      promised_stream_id: 2,
      field_block_fragment: <<>>,
      padding: None,
    )
  // Followed by CONTINUATION with END_HEADERS on stream 1
  let assert Ok(cont) =
    h2_frame.encode_continuation(
      stream_id: 1,
      end_headers: True,
      field_block_fragment: <<>>,
    )
  let assert Ok(#(_client, events, _to_send)) =
    receive_data(client, <<pp:bits, cont:bits>>)
  assert events == [PushPromiseReceived(
    stream_id: 1,
    promised_stream_id: 2,
    headers: [],
  )]
}

pub fn receive_push_promise_without_end_headers_then_wrong_frame_is_protocol_error_test() {
  let #(_server, client) = server_with_open_stream()
  // PUSH_PROMISE without END_HEADERS
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: False,
      promised_stream_id: 2,
      field_block_fragment: <<>>,
      padding: None,
    )
  // Followed by PING instead of CONTINUATION
  let assert Ok(ping) = h2_frame.encode_ping(ack: False, data: <<0:64>>)
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(client, <<pp:bits, ping:bits>>)
}

pub fn receive_push_promise_without_end_headers_then_wrong_stream_is_protocol_error_test() {
  let #(_server, client) = server_with_open_stream()
  // Also open stream 3 so CONTINUATION on stream 3 would be "different stream"
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: False,
      promised_stream_id: 2,
      field_block_fragment: <<>>,
      padding: None,
    )
  // CONTINUATION on wrong stream
  let assert Ok(cont) =
    h2_frame.encode_continuation(
      stream_id: 3,
      end_headers: True,
      field_block_fragment: <<>>,
    )
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(client, <<pp:bits, cont:bits>>)
}

// =============================================================================
// Section 6.6 - PUSH_PROMISE modifies HPACK state. Even if the PUSH_PROMISE
// is on a stream that was reset, the HPACK state must still be updated.
// (Referenced from Section 5.1 closed state rules and Section 4.3)
// =============================================================================

pub fn receive_push_promise_updates_hpack_state_test() {
  let #(_server, client) = server_with_open_stream()
  // Send a PUSH_PROMISE with headers that modify HPACK dynamic table
  // First, we need actual HPACK-encoded headers
  let assert Ok(pp1) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      // HPACK literal with indexing: :method GET (indexed as 2)
      field_block_fragment: <<0x82>>,
      padding: None,
    )
  let assert Ok(#(_client, events, _to_send)) = receive_data(client, pp1)
  // Should have decoded the header
  assert events == [PushPromiseReceived(
    stream_id: 1,
    promised_stream_id: 2,
    headers: [Header(":method", "GET", WithIndexing)],
  )]
}

// =============================================================================
// Section 8.4 - "A server cannot set the SETTINGS_ENABLE_PUSH setting to a
// value other than 0 (see Section 6.5.2)."
// This is tested in settings tests but included here for completeness.
// =============================================================================

// Note: This requirement is about SETTINGS validation, already covered
// in h2_core_settings_test.gleam (receive_settings_server_sends_enable_push_1_test)

// =============================================================================
// Sending PUSH_PROMISE
// =============================================================================

// RFC 9113 Section 6.6 - "PUSH_PROMISE frames MUST only be sent on a
// peer-initiated stream that is in either the 'open' or 'half-closed
// (remote)' state."
pub fn send_push_promise_on_open_peer_stream_test() {
  let #(server, _client) = server_with_open_stream()
  // Server pushes on stream 1 (client-initiated, open)
  let assert Ok(#(server, _events, _to_send)) =
    h2_core.send_push_promise(
      server,
      1,
      2,
      [Header(":method", "GET", WithIndexing)],
    )
  // Promised stream 2 should be in reserved (local) on server side
  let assert Ok(stream) = dict.get(server.streams, 2)
  assert stream.state == ReservedLocal
}

pub fn send_push_promise_on_half_closed_remote_test() {
  let server = new_connection(Server)
  let client = new_connection(Client)
  // Client sends headers with END_STREAM
  let assert Ok(#(_client, _events, headers)) =
    send_headers(client, [Header(":method", "GET", WithIndexing)], True)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  // Stream 1 is half-closed (remote) on server — push should work
  let assert Ok(#(server, _events, _to_send)) =
    h2_core.send_push_promise(
      server,
      1,
      2,
      [Header(":method", "GET", WithIndexing)],
    )
  let assert Ok(stream) = dict.get(server.streams, 2)
  assert stream.state == ReservedLocal
}

// RFC 9113 Section 6.6 - "A sender MUST NOT send a PUSH_PROMISE on a stream
// unless that stream is either 'open' or 'half-closed (remote)'"
pub fn send_push_promise_on_idle_stream_is_error_test() {
  let server = new_connection(Server)
  let assert Error(_) =
    h2_core.send_push_promise(
      server,
      1,
      2,
      [Header(":method", "GET", WithIndexing)],
    )
}

// RFC 9113 Section 6.6 - "the sender MUST ensure that the promised stream
// is a valid choice for a new stream identifier (Section 5.1.1) (that is,
// the promised stream MUST be in the 'idle' state)."
pub fn send_push_promise_promised_id_must_be_even_test() {
  let #(server, _client) = server_with_open_stream()
  // Promised stream 3 is odd — invalid for server
  let assert Error(_) =
    h2_core.send_push_promise(
      server,
      1,
      3,
      [Header(":method", "GET", WithIndexing)],
    )
}

pub fn send_push_promise_promised_id_must_be_increasing_test() {
  let #(server, _client) = server_with_open_stream()
  // First push reserves stream 4
  let assert Ok(#(server, _events, _to_send)) =
    h2_core.send_push_promise(
      server,
      1,
      4,
      [Header(":method", "GET", WithIndexing)],
    )
  // Second push tries stream 2 — lower than 4
  let assert Error(_) =
    h2_core.send_push_promise(
      server,
      1,
      2,
      [Header(":method", "GET", WithIndexing)],
    )
}

// RFC 9113 Section 6.6 - "PUSH_PROMISE MUST NOT be sent if the
// SETTINGS_ENABLE_PUSH setting of the peer endpoint is set to 0."
pub fn send_push_promise_when_peer_disabled_push_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  // Simulate peer (client) having sent ENABLE_PUSH=0
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.EnablePush(0),
    ])
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, settings_frame)
  // Server tries to push — should fail
  let assert Error(_) =
    h2_core.send_push_promise(
      server,
      1,
      2,
      [Header(":method", "GET", WithIndexing)],
    )
}

// RFC 9113 Section 8.4 - "A client cannot push."
// Clients should not be able to send PUSH_PROMISE.
pub fn client_send_push_promise_is_error_test() {
  let client = new_connection(Client)
  let server = new_connection(Server)
  // Open stream 1 from client
  let assert Ok(#(client, _events, headers)) =
    send_headers(client, [Header(":method", "GET", WithIndexing)], False)
  let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)
  // Client tries to push — must be rejected
  let assert Error(_) =
    h2_core.send_push_promise(
      client,
      1,
      2,
      [Header(":method", "GET", WithIndexing)],
    )
}

// RFC 9113 Section 6.6 - Stream ID 0 is invalid for PUSH_PROMISE
pub fn send_push_promise_on_stream_zero_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Error(_) =
    h2_core.send_push_promise(
      server,
      0,
      2,
      [Header(":method", "GET", WithIndexing)],
    )
}
