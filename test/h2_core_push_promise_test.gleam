import gleam/dict
import gleam/option.{None}
import h2_core.{
  type Connection, Client, Closed, Connected, ConnectionError, HalfClosedRemote,
  Header, PushPromiseReceived, ReservedLocal, ReservedRemote, Server,
  StreamReset, WithIndexing, open_stream, receive_data, send_headers,
  send_rst_stream,
}
import h2_frame
import helper

// Helper: create a server connection with an open client-initiated stream 1
fn server_with_open_stream() -> #(Connection, Connection) {
  let server = helper.new_connection(Server, Connected)
  let client = helper.new_connection(Client, Connected)
  let assert Ok(#(client, _events, headers)) =
    open_stream(client, helper.request_headers(), False)
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
  let client = helper.new_connection(Client, Connected)
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
  let #(server, client) = server_with_open_stream()

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
  let client = helper.new_connection(Client, Connected)
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
  assert events
    == [PushPromiseReceived(stream_id: 1, promised_stream_id: 2, headers: [])]
  // Promised stream 2 should be in reserved (remote) state
  let assert Ok(stream) = dict.get(client.streams, 2)
  assert stream.state == ReservedRemote
}

pub fn receive_push_promise_on_half_closed_local_is_valid_test() {
  let server = helper.new_connection(Server, Connected)
  let client = helper.new_connection(Client, Connected)
  // Client sends headers with END_STREAM — stream 1 is half-closed (local)
  // on the client side
  let assert Ok(#(client, _events, headers)) =
    open_stream(client, helper.request_headers(), True)
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
  assert events
    == [PushPromiseReceived(stream_id: 1, promised_stream_id: 2, headers: [])]
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
  // Simulate that stream 2 was already promised by setting last_remote_stream_id
  let client = h2_core.Connection(..client, last_remote_stream_id: 2)
  // Push promises stream 2 — already used (2 <= 2)
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

// Section 5.1.1 - "The identifier of a newly established stream MUST be
// numerically greater than all streams that the initiating endpoint has
// opened or reserved."
pub fn receive_push_promise_with_decreasing_id_is_protocol_error_test() {
  let #(_server, client) = server_with_open_stream()
  // Simulate that stream 4 was already promised
  let client = h2_core.Connection(..client, last_remote_stream_id: 4)
  // Push promises stream 2 — lower than 4, violates ordering
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
  assert events
    == [PushPromiseReceived(stream_id: 1, promised_stream_id: 2, headers: [])]
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
  assert events
    == [
      PushPromiseReceived(stream_id: 1, promised_stream_id: 2, headers: [
        Header(":method", "GET", WithIndexing),
      ]),
    ]
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
  let assert Ok(#(server, _events, _to_send, promised_id)) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", "GET", WithIndexing),
    ])
  // Promised stream should be even and in reserved (local) on server side
  assert promised_id % 2 == 0
  let assert Ok(stream) = dict.get(server.streams, promised_id)
  assert stream.state == ReservedLocal
}

pub fn send_push_promise_on_half_closed_remote_test() {
  let server = helper.new_connection(Server, Connected)
  let client = helper.new_connection(Client, Connected)
  // Client sends headers with END_STREAM
  let assert Ok(#(_client, _events, headers)) =
    open_stream(client, helper.request_headers(), True)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  // Stream 1 is half-closed (remote) on server — push should work
  let assert Ok(#(server, _events, _to_send, promised_id)) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", "GET", WithIndexing),
    ])
  let assert Ok(stream) = dict.get(server.streams, promised_id)
  assert stream.state == ReservedLocal
}

// RFC 9113 Section 6.6 - "A sender MUST NOT send a PUSH_PROMISE on a stream
// unless that stream is either 'open' or 'half-closed (remote)'"
pub fn send_push_promise_on_idle_stream_is_error_test() {
  let server = helper.new_connection(Server, Connected)
  let assert Error(_) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", "GET", WithIndexing),
    ])
}

// RFC 9113 Section 5.1.1 - Auto-allocated promised stream IDs must be
// even and strictly increasing.
pub fn send_push_promise_auto_allocates_increasing_even_ids_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _events, _to_send, id1)) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", "GET", WithIndexing),
    ])
  let assert Ok(#(_server, _events, _to_send, id2)) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", "GET", WithIndexing),
    ])
  assert id1 % 2 == 0
  assert id2 % 2 == 0
  assert id2 > id1
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
    h2_core.send_push_promise(server, 1, [
      Header(":method", "GET", WithIndexing),
    ])
}

// RFC 9113 Section 8.4 - "A client cannot push."
// Clients should not be able to send PUSH_PROMISE.
pub fn client_send_push_promise_is_error_test() {
  let client = helper.new_connection(Client, Connected)
  let server = helper.new_connection(Server, Connected)
  // Open stream 1 from client
  let assert Ok(#(client, _events, headers)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)
  // Client tries to push — must be rejected
  let assert Error(_) =
    h2_core.send_push_promise(client, 1, [
      Header(":method", "GET", WithIndexing),
    ])
}

// RFC 9113 Section 6.6 - Stream ID 0 is invalid for PUSH_PROMISE
pub fn send_push_promise_on_stream_zero_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Error(_) =
    h2_core.send_push_promise(server, 0, [
      Header(":method", "GET", WithIndexing),
    ])
}

// RFC 9113 Section 6.6 - "PUSH_PROMISE frames MUST only be sent on a
// peer-initiated stream that is in either the 'open' or 'half-closed
// (remote)' state."
//
// A half-closed (local) stream has had END_STREAM sent by the server,
// so it is no longer open or half-closed (remote) — sending a
// PUSH_PROMISE on it must be an error.
pub fn send_push_promise_on_half_closed_local_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  // Server sends response headers with END_STREAM — stream becomes
  // half-closed (local) from the server's perspective
  let assert Ok(#(server, _events, _to_send)) =
    send_headers(server, 1, [Header(":status", "200", WithIndexing)], True)
  let assert Error(_) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", "GET", WithIndexing),
    ])
}

// RFC 9113 Section 6.6 - "A receiver MUST treat the receipt of a
// PUSH_PROMISE on a stream that is neither 'open' nor 'half-closed
// (local)' as a connection error of type PROTOCOL_ERROR. However, an
// endpoint that has sent RST_STREAM on the associated stream MUST handle
// PUSH_PROMISE frames that might have been created before the RST_STREAM
// frame is received and processed."
//
// NOTE: The RFC is ambiguous about whether "handle" means "accept
// normally" or just "don't crash". It also doesn't distinguish between
// streams closed by our RST_STREAM vs the peer's RST_STREAM. We take
// the lenient approach: accept PUSH_PROMISE on any closed stream, decode
// HPACK, reserve the promised stream, and emit the event. The caller can
// RST_STREAM the promised stream if unwanted.
pub fn receive_push_promise_on_closed_stream_is_handled_gracefully_test() {
  let #(_server, client) = server_with_open_stream()
  // Server RST_STREAMs stream 1 — now closed on client
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.NoError)
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, rst)
  let assert Ok(stream) = dict.get(client.streams, 1)
  assert stream.state == h2_core.Closed

  // PUSH_PROMISE arrives on closed stream — must be handled, not rejected
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Ok(#(client, events, _to_send)) = receive_data(client, pp)
  assert events
    == [PushPromiseReceived(stream_id: 1, promised_stream_id: 2, headers: [])]
  // Promised stream should still be reserved
  let assert Ok(stream) = dict.get(client.streams, 2)
  assert stream.state == ReservedRemote
}

// RFC 9113 Section 6.6 - "The total number of padding octets is
// determined by the value of the Pad Length field. If the length of
// the padding is the length of the frame payload or greater, the
// recipient MUST treat this as a connection error (Section 5.4.1) of
// type PROTOCOL_ERROR."
pub fn receive_push_promise_invalid_padding_length_is_protocol_error_test() {
  let #(_server, client) = server_with_open_stream()
  // Manually craft a PUSH_PROMISE with PADDED flag where pad_length
  // equals the remaining payload — invalid.
  // Length=5 (1 pad_length + 4 promised_stream_id), Type=0x05,
  // Flags=0x0C (PADDED | END_HEADERS), Stream ID=1
  // Pad Length=4 — remaining after pad_length field is 4 bytes (the
  // promised stream ID), so padding (4) >= remaining payload (4).
  let bad_pp = <<
    5:size(24),
    0x05:size(8),
    0x0C:size(8),
    0:size(1),
    1:size(31),
    4:size(8),
    0:size(1),
    2:size(31),
  >>
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(client, bad_pp)
}

// RFC 9113 Section 6.6 - "A receiver is not obligated to verify
// padding but MAY treat non-zero padding as a connection error
// (Section 5.4.1) of type PROTOCOL_ERROR."
//
// By default, non-zero padding bytes MUST be silently accepted.
pub fn receive_push_promise_nonzero_padding_bytes_accepted_by_default_test() {
  let #(_server, client) = server_with_open_stream()
  // Manually craft a PUSH_PROMISE with PADDED flag, pad_length=3,
  // promised_stream_id=2, then 3 non-zero padding bytes.
  // Length=8 (1 pad_length + 4 promised_id + 3 padding), Type=0x05,
  // Flags=0x0C (PADDED | END_HEADERS), Stream ID=1
  let non_zero_padded = <<
    8:size(24),
    0x05:size(8),
    0x0C:size(8),
    0:size(1),
    1:size(31),
    3:size(8),
    0:size(1),
    2:size(31),
    0xFF,
    0xFF,
    0xFF,
  >>
  let assert Ok(#(_client, events, _to_send)) =
    receive_data(client, non_zero_padded)
  assert events
    == [PushPromiseReceived(stream_id: 1, promised_stream_id: 2, headers: [])]
}

// =============================================================================
// Section 6.6 - "However, an endpoint that has sent RST_STREAM on the
// associated stream MUST handle PUSH_PROMISE frames that might have been
// created before the RST_STREAM frame is received and processed."
//
// A PUSH_PROMISE arriving on a stream where the client previously sent
// RST_STREAM must not cause a connection error — the server may have sent
// the PUSH_PROMISE before processing the RST_STREAM.
// =============================================================================

pub fn receive_push_promise_after_client_sent_rst_stream_is_not_connection_error_test() {
  let #(_server, client) = server_with_open_stream()
  // Client sends RST_STREAM on stream 1
  let assert Ok(#(client, _events, _to_send)) =
    send_rst_stream(client, 1, h2_frame.Cancel)
  let assert Ok(stream) = dict.get(client.streams, 1)
  assert stream.state == Closed

  // Server's PUSH_PROMISE arrives (was in flight before RST_STREAM processed)
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<>>,
      padding: None,
    )
  // Must not be a connection error — the endpoint must handle this gracefully
  let assert Ok(#(_client, _events, _to_send)) = receive_data(client, pp)
}

// =============================================================================
// Section 6.6 - "A receiver MUST treat the receipt of a PUSH_PROMISE on a
// stream that is neither 'open' nor 'half-closed (local)' as a connection
// error (Section 5.4.1) of type PROTOCOL_ERROR."
//
// Half-closed (remote) on the receiver means the receiver sent END_STREAM
// but hasn't received it — the stream is not "open" or "half-closed (local)",
// so receiving PUSH_PROMISE here is a PROTOCOL_ERROR.
// =============================================================================

pub fn receive_push_promise_on_half_closed_remote_is_protocol_error_test() {
  let #(_server, client) = server_with_open_stream()
  // Simulate stream 1 being half-closed (remote) on client
  // (server sent END_STREAM to client)
  let client = helper.set_stream_state(client, 1, HalfClosedRemote)
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
// Section 6.6 - "A sender MUST NOT send a PUSH_PROMISE on a stream unless
// that stream is either 'open' or 'half-closed (remote)'"
//
// Sending on a closed stream must error.
// =============================================================================

pub fn send_push_promise_on_closed_stream_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let server = helper.set_stream_state(server, 1, Closed)
  let assert Error(_) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", "GET", WithIndexing),
    ])
}

// =============================================================================
// Verify that send_push_promise produces correctly encoded output by
// comparing against h2_frame.encode_push_promise directly.
// =============================================================================

pub fn send_push_promise_returns_encoded_frame_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(_server, _events, to_send, promised_id)) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", "GET", WithIndexing),
    ])
  // :method GET is HPACK static index 2, encoded as 0x82
  let assert Ok(expected) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: promised_id,
      field_block_fragment: <<0x82>>,
      padding: None,
    )
  assert to_send == expected
}

// Verify that send_push_promise with empty headers produces a valid frame.
pub fn send_push_promise_empty_headers_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(_server, _events, to_send, promised_id)) =
    h2_core.send_push_promise(server, 1, [])
  let assert Ok(expected) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: promised_id,
      field_block_fragment: <<>>,
      padding: None,
    )
  assert to_send == expected
}

// Verify that send_push_promise with multiple headers encodes them all.
pub fn send_push_promise_multiple_headers_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _events, to_send, promised_id)) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", "GET", WithIndexing),
      Header(":path", "/", WithIndexing),
    ])
  // :method GET = 0x82, :path / = 0x84 (HPACK static table indices)
  let assert Ok(expected) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: promised_id,
      field_block_fragment: <<0x82, 0x84>>,
      padding: None,
    )
  assert to_send == expected
  // HPACK encoder state should be updated on the connection
  // Verify by sending a second push — the encoder should still work
  let assert Ok(#(_server, _events, _to_send, id2)) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", "GET", WithIndexing),
    ])
  assert id2 > promised_id
}

// =============================================================================
// Section 8.4 - Multiple PUSH_PROMISE frames reserving different streams.
// Verify all are properly reserved.
// =============================================================================

pub fn receive_multiple_push_promises_reserves_all_streams_test() {
  let #(_server, client) = server_with_open_stream()
  let assert Ok(pp1) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Ok(pp2) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 4,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Ok(pp3) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 6,
      field_block_fragment: <<>>,
      padding: None,
    )
  let assert Ok(#(client, events, _to_send)) =
    receive_data(client, <<pp1:bits, pp2:bits, pp3:bits>>)
  assert events
    == [
      PushPromiseReceived(stream_id: 1, promised_stream_id: 2, headers: []),
      PushPromiseReceived(stream_id: 1, promised_stream_id: 4, headers: []),
      PushPromiseReceived(stream_id: 1, promised_stream_id: 6, headers: []),
    ]
  let assert Ok(s2) = dict.get(client.streams, 2)
  let assert Ok(s4) = dict.get(client.streams, 4)
  let assert Ok(s6) = dict.get(client.streams, 6)
  assert s2.state == ReservedRemote
  assert s4.state == ReservedRemote
  assert s6.state == ReservedRemote
}

// RFC 9113 Section 8.4.1 - "The header fields in PUSH_PROMISE and any
// subsequent CONTINUATION frames MUST be a valid and complete set of
// request header fields (Section 8.3.1)."
//
// A PUSH_PROMISE missing mandatory request pseudo-headers is malformed.
pub fn receive_push_promise_missing_method_is_malformed_test() {
  let #(_server, client) = server_with_open_stream()
  // HPACK: :scheme https, :path / — missing :method
  let bad_hpack = <<0x87, 0x84>>
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: bad_hpack,
      padding: None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal.
  let assert Ok(#(_client, events, to_send)) = receive_data(client, pp)
  assert events == [StreamReset(stream_id: 1, error_code: h2_frame.ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.4.1 - PUSH_PROMISE with pseudo-header after
// regular header is malformed.
pub fn receive_push_promise_pseudo_after_regular_is_malformed_test() {
  let #(_server, client) = server_with_open_stream()
  // HPACK: regular header first, then pseudo-header
  let bad_hpack = <<
    0x40, 0x05, "x-foo":utf8, 0x03, "bar":utf8, 0x82,
  >>
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: bad_hpack,
      padding: None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal.
  let assert Ok(#(_client, events, to_send)) = receive_data(client, pp)
  assert events == [StreamReset(stream_id: 1, error_code: h2_frame.ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}
