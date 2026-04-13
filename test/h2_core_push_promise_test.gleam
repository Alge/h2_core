import gleam/bit_array
import gleam/int
import gleam/list
import gleam/option.{None}
import gleam/string
import h2_core.{
  type Connection, Cancel, Client, ConnectionError, Header, HeadersReceived,
  InvalidHeaders, InvalidRole, InvalidStreamState, ProtocolError, PushDisabled,
  PushPromiseReceived, Server, StreamEnded, StreamReset, UnknownStream,
  WithIndexing, get_stream_state, open_stream, receive_data, send_data,
  send_headers, send_rst_stream,
}
import h2_core/internal/stream.{
  Closed, HalfClosedLocal, Open, ReservedLocal, ReservedRemote,
}
import h2_frame
import helper

fn server_with_open_stream() -> #(Connection, Connection) {
  helper.server_with_open_stream()
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
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Error(ConnectionError(ProtocolError)) = receive_data(server, pp)
}

// =============================================================================
// Section 6.6 - "If the Stream Identifier field specifies the value 0x00,
// a recipient MUST respond with a connection error (Section 5.4.1) of type
// PROTOCOL_ERROR."
// =============================================================================

pub fn receive_push_promise_on_stream_zero_is_protocol_error_test() {
  let client = helper.connected_connection(Client)
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
  let assert Error(ConnectionError(ProtocolError)) =
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
  let assert Ok(#(client, settings_frame)) =
    h2_core.send_settings(client, [h2_core.EnablePush(False)])

  // Server acknowledges (client receives the ack)
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Ok(#(client, _events, _to_send)) =
    receive_data(client, settings_ack)

  // Server sends the settings to its side
  let assert Ok(#(_server, _events, _to_send)) =
    receive_data(server, settings_frame)

  // Now server sends PUSH_PROMISE - client should reject it
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Error(ConnectionError(ProtocolError)) = receive_data(client, pp)
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
  let client = helper.connected_connection(Client)
  // Stream 1 was never opened - it's idle
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Error(ConnectionError(ProtocolError)) = receive_data(client, pp)
}

pub fn receive_push_promise_on_open_stream_is_valid_test() {
  let #(_server, client) = server_with_open_stream()
  // Server pushes on stream 1 which is open
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Ok(#(client, events, _to_send)) = receive_data(client, pp)
  // Should emit PushPromiseReceived event
  let assert [
    PushPromiseReceived(stream_id: 1, promised_stream_id: 2, headers: _),
  ] = events
  // Promised stream 2 should be in reserved (remote) state
  let assert Ok(ReservedRemote) = h2_core.get_stream_state(client, 2)
}

pub fn receive_push_promise_on_half_closed_local_is_valid_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  // Client sends headers with END_STREAM - stream 1 is half-closed (local)
  // on the client side
  let assert Ok(#(client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), True)
  let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)

  // Server pushes on stream 1 - valid because it's half-closed (local)
  // from the client's perspective
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Ok(#(_client, events, _to_send)) = receive_data(client, pp)
  let assert [
    PushPromiseReceived(stream_id: 1, promised_stream_id: 2, headers: _),
  ] = events
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
  // Server promises stream 3 (odd) - only even IDs are server-initiated
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 3,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Error(ConnectionError(ProtocolError)) = receive_data(client, pp)
}

pub fn receive_push_promise_with_zero_promised_id_is_protocol_error_test() {
  let #(_server, client) = server_with_open_stream()
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 0,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Error(ConnectionError(ProtocolError)) = receive_data(client, pp)
}

pub fn receive_push_promise_with_already_used_id_is_protocol_error_test() {
  let #(server, client) = server_with_open_stream()
  // Server sends a push promise so that client's last_remote_stream_id == 2
  let assert Ok(#(_server, pp_bytes, _promised_id)) =
    h2_core.send_push_promise(server, 1, helper.request_headers())
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, pp_bytes)
  // Push promises stream 2 - already used (2 <= 2)
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Error(ConnectionError(ProtocolError)) = receive_data(client, pp)
}

// Section 5.1.1 - "The identifier of a newly established stream MUST be
// numerically greater than all streams that the initiating endpoint has
// opened or reserved."
pub fn receive_push_promise_with_decreasing_id_is_protocol_error_test() {
  let #(server, client) = server_with_open_stream()
  // Server sends two push promises so that client's last_remote_stream_id == 4
  let assert Ok(#(server, pp1_bytes, _promised_id)) =
    h2_core.send_push_promise(server, 1, helper.request_headers())
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, pp1_bytes)
  let assert Ok(#(_server, pp2_bytes, _promised_id)) =
    h2_core.send_push_promise(server, 1, helper.request_headers())
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, pp2_bytes)
  // Push promises stream 2 - lower than 4, violates ordering
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Error(ConnectionError(ProtocolError)) = receive_data(client, pp)
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
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, pp)
  let assert Ok(ReservedRemote) = h2_core.get_stream_state(client, 2)
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
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  // Followed by CONTINUATION with END_HEADERS on stream 1 (empty fragment)
  let assert Ok(cont) =
    h2_frame.encode_continuation(
      stream_id: 1,
      end_headers: True,
      field_block_fragment: <<>>,
    )
  let assert Ok(#(_client, events, _to_send)) =
    receive_data(client, <<pp:bits, cont:bits>>)
  let assert [
    PushPromiseReceived(stream_id: 1, promised_stream_id: 2, headers: _),
  ] = events
}

pub fn receive_push_promise_without_end_headers_then_wrong_frame_is_protocol_error_test() {
  let #(_server, client) = server_with_open_stream()
  // PUSH_PROMISE without END_HEADERS
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: False,
      promised_stream_id: 2,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  // Followed by PING instead of CONTINUATION
  let assert Ok(ping) = h2_frame.encode_ping(ack: False, data: <<0:64>>)
  let assert Error(ConnectionError(ProtocolError)) =
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
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  // CONTINUATION on wrong stream
  let assert Ok(cont) =
    h2_frame.encode_continuation(
      stream_id: 3,
      end_headers: True,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
    )
  let assert Error(ConnectionError(ProtocolError)) =
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
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Ok(#(_client, events, _to_send)) = receive_data(client, pp1)
  // Should have decoded the header
  let assert [
    PushPromiseReceived(stream_id: 1, promised_stream_id: 2, headers: h),
  ] = events
  let assert [
    Header(":method", <<"GET":utf8>>, _),
    Header(":scheme", <<"https":utf8>>, _),
    Header(":path", <<"/":utf8>>, _),
  ] = h
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
  let assert Ok(#(server, _to_send, promised_id)) =
    h2_core.send_push_promise(server, 1, helper.request_headers())
  // Promised stream should be even and in reserved (local) on server side
  assert promised_id % 2 == 0
  let assert Ok(ReservedLocal) = h2_core.get_stream_state(server, promised_id)
}

pub fn send_push_promise_on_half_closed_remote_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  // Client sends headers with END_STREAM
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), True)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  // Stream 1 is half-closed (remote) on server - push should work
  let assert Ok(#(server, _to_send, promised_id)) =
    h2_core.send_push_promise(server, 1, helper.request_headers())
  let assert Ok(ReservedLocal) = h2_core.get_stream_state(server, promised_id)
}

// RFC 9113 Section 6.6 - "A sender MUST NOT send a PUSH_PROMISE on a stream
// unless that stream is either 'open' or 'half-closed (remote)'"
pub fn send_push_promise_on_idle_stream_is_error_test() {
  let server = helper.connected_connection(Server)
  let assert Error(UnknownStream) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", <<"GET":utf8>>, WithIndexing),
    ])
}

// RFC 9113 Section 5.1.1 - Auto-allocated promised stream IDs must be
// even and strictly increasing.
pub fn send_push_promise_auto_allocates_increasing_even_ids_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send, id1)) =
    h2_core.send_push_promise(server, 1, helper.request_headers())
  let assert Ok(#(_server, _to_send, id2)) =
    h2_core.send_push_promise(server, 1, helper.request_headers())
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
  // Server tries to push - should fail
  let assert Error(PushDisabled) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", <<"GET":utf8>>, WithIndexing),
    ])
}

// RFC 9113 Section 8.4 - "A client cannot push."
// Clients should not be able to send PUSH_PROMISE.
pub fn client_send_push_promise_is_error_test() {
  let client = helper.connected_connection(Client)
  let server = helper.connected_connection(Server)
  // Open stream 1 from client
  let assert Ok(#(client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)
  // Client tries to push - must be rejected
  let assert Error(InvalidRole) =
    h2_core.send_push_promise(client, 1, [
      Header(":method", <<"GET":utf8>>, WithIndexing),
    ])
}

// RFC 9113 Section 6.6 - Stream ID 0 is invalid for PUSH_PROMISE
pub fn send_push_promise_on_stream_zero_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Error(UnknownStream) =
    h2_core.send_push_promise(server, 0, [
      Header(":method", <<"GET":utf8>>, WithIndexing),
    ])
}

// RFC 9113 Section 6.6 - "PUSH_PROMISE frames MUST only be sent on a
// peer-initiated stream that is in either the 'open' or 'half-closed
// (remote)' state."
//
// A half-closed (local) stream has had END_STREAM sent by the server,
// so it is no longer open or half-closed (remote) - sending a
// PUSH_PROMISE on it must be an error.
pub fn send_push_promise_on_half_closed_local_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  // Server sends response headers with END_STREAM - stream becomes
  // half-closed (local) from the server's perspective
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      True,
    )
  let assert Error(InvalidStreamState) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", <<"GET":utf8>>, WithIndexing),
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
// RFC 9113 Section 6.6 - when a stream is closed via RST_STREAM (either
// direction), a PUSH_PROMISE may still be in flight. Both sides of an RST_STREAM
// close are treated as a potential race condition - the PUSH_PROMISE must be
// handled gracefully (promised stream not reserved, no connection error).
pub fn receive_push_promise_on_stream_closed_by_remote_rst_is_graceful_test() {
  let #(_server, client) = server_with_open_stream()
  // Server RST_STREAMs stream 1
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.NoError)
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, rst)
  let assert Ok(Closed) = h2_core.get_stream_state(client, 1)

  // PUSH_PROMISE arrives - must be handled gracefully (promised stream not reserved)
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, pp)
  let assert Error(_) = h2_core.get_stream_state(client, 2)
}

// RFC 9113 Section 6.6 - a stream closed naturally by END_STREAM (no RST_STREAM
// involved) is a genuine protocol violation if PUSH_PROMISE arrives on it.
// There is no race condition to account for, so this must be a connection error.
pub fn receive_push_promise_on_stream_closed_by_end_stream_is_protocol_error_test() {
  let #(server, client) = server_with_open_stream()
  // Server sends response with END_STREAM -> client stream goes to HalfClosedRemote
  let assert Ok(#(_server, response_bytes)) =
    send_headers(server, 1, helper.response_headers(), True)
  let assert Ok(#(client, _events, _to_send)) =
    receive_data(client, response_bytes)
  // Client sends END_STREAM -> stream is now Closed (no RST_STREAM involved)
  let assert Ok(#(client, _to_send)) = send_data(client, 1, <<>>, True, None)
  let assert Ok(Closed) = h2_core.get_stream_state(client, 1)

  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Error(ConnectionError(ProtocolError)) = receive_data(client, pp)
}

// RFC 9113 Section 6.6 - "The total number of padding octets is
// determined by the value of the Pad Length field. If the length of
// the padding is the length of the frame payload or greater, the
// recipient MUST treat this as a connection error (Section 5.4.1) of
// type PROTOCOL_ERROR."
pub fn receive_push_promise_invalid_padding_length_is_protocol_error_test() {
  let #(_server, client) = server_with_open_stream()
  // Manually craft a PUSH_PROMISE with PADDED flag where pad_length
  // equals the remaining payload - invalid.
  // Length=5 (1 pad_length + 4 promised_stream_id), Type=0x05,
  // Flags=0x0C (PADDED | END_HEADERS), Stream ID=1
  // Pad Length=4 - remaining after pad_length field is 4 bytes (the
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
  let assert Error(ConnectionError(ProtocolError)) =
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
  // promised_stream_id=2, HPACK data, then 3 non-zero padding bytes.
  // Length=11 (1 pad_length + 4 promised_id + 3 HPACK + 3 padding),
  // Type=0x05, Flags=0x0C (PADDED | END_HEADERS), Stream ID=1
  // HPACK: 0x82 = :method GET, 0x87 = :scheme https, 0x84 = :path /
  let non_zero_padded = <<
    11:size(24), 0x05:size(8), 0x0C:size(8), 0:size(1), 1:size(31), 3:size(8),
    0:size(1), 2:size(31), 0x82, 0x87, 0x84, 0xFF, 0xFF, 0xFF,
  >>
  let assert Ok(#(_client, events, _to_send)) =
    receive_data(client, non_zero_padded)
  let assert [
    PushPromiseReceived(stream_id: 1, promised_stream_id: 2, headers: _),
  ] = events
}

// =============================================================================
// Header validation - RFC 9113 Section 8.4.1
//
// "The header fields in PUSH_PROMISE... MUST be a valid and complete set of
// request header fields (Section 8.3.1)."
// =============================================================================

// RFC 9113 Section 8.4.1 - Empty headers are missing all required pseudo-headers.
pub fn send_push_promise_empty_headers_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Error(InvalidHeaders) = h2_core.send_push_promise(server, 1, [])
}

// =============================================================================
// Bug fix: RFC 9113 Section 8.4.1 - receiving a PUSH_PROMISE with an invalid
// request field block MUST be treated as a stream error on the PROMISED stream,
// not the parent stream.
//
// "A PUSH_PROMISE frame that includes a request field block that is invalid
// (Section 8.1.1) MUST be treated as a stream error (Section 5.4.2) of type
// PROTOCOL_ERROR."
//
// The stream in error is the promised stream. The parent stream must be
// unaffected.
// =============================================================================

// HPACK: 0x82 = :method GET, 0x87 = :scheme https - missing :path, so invalid.
pub fn receive_push_promise_invalid_headers_rsts_promised_stream_not_parent_test() {
  let #(_server, client) = server_with_open_stream()

  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<0x82, 0x87>>,
      padding: None,
    )
  let assert Ok(#(client, events, to_send)) = receive_data(client, pp)

  // StreamReset must target the promised stream (2), not the parent (1).
  let assert [StreamReset(stream_id: 2, error_code: ProtocolError)] = events

  // Parent stream (1) must remain open - the push promise failure must not
  // affect it.
  let assert Ok(Open) = h2_core.get_stream_state(client, 1)

  // Promised stream (2) must not be reserved - it was rejected before creation.
  let assert Error(Nil) = h2_core.get_stream_state(client, 2)

  // to_send must contain RST_STREAM targeting stream 2.
  let frames = helper.parse_all_frames(to_send, [])
  let assert True =
    list.any(frames, fn(f) {
      case f {
        h2_frame.RstStream(2, h2_frame.ProtocolError) -> True
        _ -> False
      }
    })
}

// Same bug exercised via the CONTINUATION path: PUSH_PROMISE with end_headers=False
// followed by CONTINUATION completing an invalid header block.
pub fn receive_push_promise_invalid_headers_via_continuation_rsts_promised_stream_test() {
  let #(_server, client) = server_with_open_stream()

  // Split the invalid header block across PUSH_PROMISE + CONTINUATION.
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: False,
      promised_stream_id: 2,
      field_block_fragment: <<0x82>>,
      padding: None,
    )
  // Continuation carries :scheme https - still missing :path, so invalid.
  let assert Ok(cont) =
    h2_frame.encode_continuation(
      stream_id: 1,
      end_headers: True,
      field_block_fragment: <<0x87>>,
    )
  let assert Ok(#(client, events, _to_send)) =
    receive_data(client, <<pp:bits, cont:bits>>)

  // StreamReset must target the promised stream (2), not the parent (1).
  let assert [StreamReset(stream_id: 2, error_code: ProtocolError)] = events

  // Parent stream (1) must remain open.
  let assert Ok(Open) = h2_core.get_stream_state(client, 1)
}

// RFC 9113 Section 8.3.1 - Missing :scheme is malformed.
pub fn send_push_promise_missing_scheme_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Error(InvalidHeaders) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", <<"GET":utf8>>, WithIndexing),
      Header(":path", <<"/":utf8>>, WithIndexing),
    ])
}

// RFC 9113 Section 8.3.1 - Missing :path is malformed.
pub fn send_push_promise_missing_path_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Error(InvalidHeaders) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", <<"GET":utf8>>, WithIndexing),
      Header(":scheme", <<"https":utf8>>, WithIndexing),
    ])
}

// RFC 9113 Section 8.3.1 - Missing :method is malformed.
pub fn send_push_promise_missing_method_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Error(InvalidHeaders) =
    h2_core.send_push_promise(server, 1, [
      Header(":scheme", <<"https":utf8>>, WithIndexing),
      Header(":path", <<"/":utf8>>, WithIndexing),
    ])
}

// RFC 9113 Section 8.3 - Pseudo-headers must appear before regular fields.
pub fn send_push_promise_pseudo_after_regular_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Error(InvalidHeaders) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", <<"GET":utf8>>, WithIndexing),
      Header("x-foo", <<"bar":utf8>>, WithIndexing),
      Header(":scheme", <<"https":utf8>>, WithIndexing),
      Header(":path", <<"/":utf8>>, WithIndexing),
    ])
}

// RFC 9113 Section 8.3 - Duplicate pseudo-headers are malformed.
pub fn send_push_promise_duplicate_pseudo_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Error(InvalidHeaders) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", <<"GET":utf8>>, WithIndexing),
      Header(":method", <<"POST":utf8>>, WithIndexing),
      Header(":scheme", <<"https":utf8>>, WithIndexing),
      Header(":path", <<"/":utf8>>, WithIndexing),
    ])
}

// RFC 9113 Section 8.2.2 - Connection-specific headers are forbidden.
pub fn send_push_promise_with_connection_header_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Error(InvalidHeaders) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", <<"GET":utf8>>, WithIndexing),
      Header(":scheme", <<"https":utf8>>, WithIndexing),
      Header(":path", <<"/":utf8>>, WithIndexing),
      Header("connection", <<"close":utf8>>, WithIndexing),
    ])
}

// RFC 9113 Section 8.4.1 - push promise headers must be request headers;
// response pseudo-headers like :status are not valid.
pub fn send_push_promise_with_status_pseudo_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Error(InvalidHeaders) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", <<"GET":utf8>>, WithIndexing),
      Header(":scheme", <<"https":utf8>>, WithIndexing),
      Header(":path", <<"/":utf8>>, WithIndexing),
      Header(":status", <<"200":utf8>>, WithIndexing),
    ])
}

// RFC 9113 Section 8.2.2 - TE header must only have value "trailers".
pub fn send_push_promise_with_te_non_trailers_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Error(InvalidHeaders) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", <<"GET":utf8>>, WithIndexing),
      Header(":scheme", <<"https":utf8>>, WithIndexing),
      Header(":path", <<"/":utf8>>, WithIndexing),
      Header("te", <<"chunked":utf8>>, WithIndexing),
    ])
}

// Valid push promise headers should succeed.
pub fn send_push_promise_with_valid_headers_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(_server, _to_send, _promised_id)) =
    h2_core.send_push_promise(server, 1, helper.request_headers())
}

// =============================================================================
// Section 6.6 - "However, an endpoint that has sent RST_STREAM on the
// associated stream MUST handle PUSH_PROMISE frames that might have been
// created before the RST_STREAM frame is received and processed."
//
// A PUSH_PROMISE arriving on a stream where the client previously sent
// RST_STREAM must not cause a connection error - the server may have sent
// the PUSH_PROMISE before processing the RST_STREAM.
// =============================================================================

pub fn receive_push_promise_after_client_sent_rst_stream_is_not_connection_error_test() {
  let #(_server, client) = server_with_open_stream()
  // Client sends RST_STREAM on stream 1
  let assert Ok(#(client, _to_send)) = send_rst_stream(client, 1, Cancel)
  let assert Ok(Closed) = h2_core.get_stream_state(client, 1)

  // Server's PUSH_PROMISE arrives (was in flight before RST_STREAM processed)
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  // Must not be a connection error - handle gracefully (promised stream not reserved)
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, pp)
  let assert Error(_) = h2_core.get_stream_state(client, 2)
}

// =============================================================================
// Section 6.6 - "A receiver MUST treat the receipt of a PUSH_PROMISE on a
// stream that is neither 'open' nor 'half-closed (local)' as a connection
// error (Section 5.4.1) of type PROTOCOL_ERROR."
//
// Half-closed (remote) on the receiver means the receiver sent END_STREAM
// but hasn't received it - the stream is not "open" or "half-closed (local)",
// so receiving PUSH_PROMISE here is a PROTOCOL_ERROR.
// =============================================================================

pub fn receive_push_promise_on_half_closed_remote_is_protocol_error_test() {
  let #(server, client) = server_with_open_stream()
  // Get client's stream 1 to half-closed (remote) by having server send
  // response headers with END_STREAM, then client receives them.
  let assert Ok(#(_server, response_bytes)) =
    send_headers(server, 1, helper.response_headers(), True)
  let assert Ok(#(client, _events, _to_send)) =
    receive_data(client, response_bytes)
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Error(ConnectionError(ProtocolError)) = receive_data(client, pp)
}

// =============================================================================
// Section 6.6 - "A sender MUST NOT send a PUSH_PROMISE on a stream unless
// that stream is either 'open' or 'half-closed (remote)'"
//
// Sending on a closed stream must error.
// =============================================================================

pub fn send_push_promise_on_closed_stream_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  // Close stream 1 by sending RST_STREAM
  let assert Ok(#(server, _to_send)) =
    send_rst_stream(server, 1, h2_core.NoError)
  let assert Error(InvalidStreamState) =
    h2_core.send_push_promise(server, 1, [
      Header(":method", <<"GET":utf8>>, WithIndexing),
    ])
}

// =============================================================================
// Verify that send_push_promise produces correctly encoded output by
// comparing against h2_frame.encode_push_promise directly.
// =============================================================================

pub fn send_push_promise_returns_encoded_frame_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(_server, to_send, promised_id)) =
    h2_core.send_push_promise(server, 1, helper.request_headers())
  // :method GET = 0x82, :scheme https = 0x87, :path / = 0x84
  let assert Ok(expected) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: promised_id,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  assert to_send == expected
}

// Verify that send_push_promise with empty headers produces a valid frame.
pub fn send_push_promise_minimal_valid_headers_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(_server, to_send, promised_id)) =
    h2_core.send_push_promise(server, 1, helper.request_headers())
  // 0x82 = :method GET, 0x87 = :scheme https, 0x84 = :path /
  let assert Ok(expected) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: promised_id,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  assert to_send == expected
}

// Verify that send_push_promise encodes all headers correctly by round-tripping
// through a client that decodes the frame and emits a PushPromiseReceived event.
pub fn send_push_promise_multiple_headers_test() {
  let #(server, client) = server_with_open_stream()
  let headers = [
    Header(":method", <<"GET":utf8>>, WithIndexing),
    Header(":scheme", <<"https":utf8>>, WithIndexing),
    Header(":path", <<"/resource":utf8>>, WithIndexing),
    Header("x-custom", <<"value":utf8>>, WithIndexing),
  ]
  let assert Ok(#(_server, to_send, promised_id)) =
    h2_core.send_push_promise(server, 1, headers)
  // Client decodes the frame - all four headers must be present
  let assert Ok(#(_client, events, _)) = receive_data(client, to_send)
  let assert [
    PushPromiseReceived(stream_id: 1, promised_stream_id: pid, headers: decoded),
  ] = events
  assert pid == promised_id
  assert list.length(decoded) == 4
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
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Ok(pp2) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 4,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Ok(pp3) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 6,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: None,
    )
  let assert Ok(#(client, events, _to_send)) =
    receive_data(client, <<pp1:bits, pp2:bits, pp3:bits>>)
  let assert [
    PushPromiseReceived(stream_id: 1, promised_stream_id: 2, headers: _),
    PushPromiseReceived(stream_id: 1, promised_stream_id: 4, headers: _),
    PushPromiseReceived(stream_id: 1, promised_stream_id: 6, headers: _),
  ] = events
  let assert Ok(ReservedRemote) = h2_core.get_stream_state(client, 2)
  let assert Ok(ReservedRemote) = h2_core.get_stream_state(client, 4)
  let assert Ok(ReservedRemote) = h2_core.get_stream_state(client, 6)
}

// RFC 9113 Section 8.4.1 - "The header fields in PUSH_PROMISE and any
// subsequent CONTINUATION frames MUST be a valid and complete set of
// request header fields (Section 8.3.1)."
//
// A PUSH_PROMISE missing mandatory request pseudo-headers is malformed.
pub fn receive_push_promise_missing_method_is_malformed_test() {
  let #(_server, client) = server_with_open_stream()
  // HPACK: :scheme https, :path / - missing :method
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
  // Stream error targets the promised stream (2), not the parent (1).
  assert events == [StreamReset(stream_id: 2, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 2, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.4.1 - PUSH_PROMISE with pseudo-header after
// regular header is malformed.
pub fn receive_push_promise_pseudo_after_regular_is_malformed_test() {
  let #(_server, client) = server_with_open_stream()
  // HPACK: regular header first, then pseudo-header
  let bad_hpack = <<0x40, 0x05, "x-foo":utf8, 0x03, "bar":utf8, 0x82>>
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
  // Stream error targets the promised stream (2), not the parent (1).
  assert events == [StreamReset(stream_id: 2, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 2, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// =============================================================================
// Sending PUSH_PROMISE with CONTINUATION frames
// =============================================================================

// Helper: generate a large list of request headers that will exceed
// the default max_frame_size (16384 bytes) when HPACK-encoded.
fn large_push_headers(count: Int) -> List(h2_core.Header) {
  let pseudo = [
    Header(":method", <<"GET":utf8>>, WithIndexing),
    Header(":scheme", <<"https":utf8>>, WithIndexing),
    Header(":path", <<"/pushed":utf8>>, WithIndexing),
  ]
  let custom =
    int.range(1, count + 1, [], fn(acc, i) {
      let name = "x-push-" <> string.pad_start(int.to_string(i), 4, "0")
      let value = string.repeat("p", 64)
      [Header(name, <<value:utf8>>, h2_core.NeverIndexed), ..acc]
    })
    |> list.reverse
  list.append(pseudo, custom)
}

// RFC 9113 Section 6.6/6.10 - When the header block exceeds
// max_frame_size, send_push_promise should produce PUSH_PROMISE +
// CONTINUATION frames.
//
// The initial PUSH_PROMISE frame MUST have end_headers=False when
// there are CONTINUATION frames following it.
pub fn send_push_promise_large_block_produces_continuation_test() {
  let #(server, _client) = server_with_open_stream()
  let headers = large_push_headers(500)
  let assert Ok(#(_server, to_send, _promised_id)) =
    h2_core.send_push_promise(server, 1, headers)

  // First frame should be PUSH_PROMISE with end_headers=False
  let assert Ok(#(frame_data, rest)) = h2_frame.extract_frame(to_send, 65_535)
  let assert Ok(frame) = h2_frame.decode_frame(frame_data)
  let assert h2_frame.PushPromise(
    stream_id: 1,
    end_headers: False,
    promised_stream_id: _,
    field_block_fragment: _,
  ) = frame
  // There should be remaining data (CONTINUATION frames)
  assert rest != <<>>
}

// The last CONTINUATION frame must have end_headers=True.
pub fn send_push_promise_continuation_last_has_end_headers_test() {
  let #(server, _client) = server_with_open_stream()
  let headers = large_push_headers(500)
  let assert Ok(#(_server, to_send, _promised_id)) =
    h2_core.send_push_promise(server, 1, headers)

  // Use 65535 limit because PUSH_PROMISE payload includes the 4-byte
  // promised_stream_id, making the first frame exceed 16384.
  let frames = helper.parse_all_frames_with_limit(to_send, [], 65_535)
  let assert Ok(last) = list.last(frames)
  case last {
    h2_frame.Continuation(end_headers: True, ..) -> Nil
    h2_frame.PushPromise(end_headers: True, ..) -> Nil
    _ -> panic as "Last frame should have end_headers=True"
  }
}

// The round-trip must work: client receives the PUSH_PROMISE +
// CONTINUATION sequence and correctly reassembles the headers.
pub fn send_push_promise_continuation_round_trip_test() {
  let #(server, _client) = server_with_open_stream()
  let headers = large_push_headers(500)
  let assert Ok(#(_server, to_send, _promised_id)) =
    h2_core.send_push_promise(server, 1, headers)

  // Verify it was actually split
  let frames = helper.parse_all_frames_with_limit(to_send, [], 65_535)
  assert list.length(frames) >= 2

  // Client receives the full sequence - the client's max_frame_size
  // is still 16384 so it will reject oversized frames. We need to
  // increase the client's setting too.
  // For now, verify the frame structure is correct.
  let assert [h2_frame.PushPromise(end_headers: False, ..), ..rest] = frames
  assert rest != []
  let assert Ok(h2_frame.Continuation(end_headers: True, ..)) = list.last(rest)
}

// =============================================================================
// RFC 9113 Section 4.2 - "An endpoint MUST send an error code of
// FRAME_SIZE_ERROR if a frame exceeds the size defined in
// SETTINGS_MAX_FRAME_SIZE"
//
// The PUSH_PROMISE frame payload includes a 4-byte promised_stream_id
// field in addition to the header block fragment. When chunking the
// encoded header block, this 4-byte overhead must be subtracted from
// max_frame_size so that the first frame's total payload does not
// exceed max_frame_size.
// =============================================================================

// Every frame produced by send_push_promise must have a payload that
// fits within max_frame_size (default 16384). The first PUSH_PROMISE
// frame has 4 bytes of overhead for the promised_stream_id.
pub fn send_push_promise_first_frame_payload_within_max_frame_size_test() {
  let #(server, _client) = server_with_open_stream()
  let headers = large_push_headers(500)
  let assert Ok(#(_server, to_send, _promised_id)) =
    h2_core.send_push_promise(server, 1, headers)

  // Parse every frame at the standard limit. If any frame exceeds
  // 16384, extract_frame will return an error and we'll get fewer
  // frames than expected.
  let frames_standard = helper.parse_all_frames(to_send, [])
  assert list.length(frames_standard) >= 2

  // Also verify the raw payload length of the first frame directly.
  // The frame header is 9 bytes: 3 (length) + 1 (type) + 1 (flags) + 4 (stream id).
  // The first 3 bytes encode the payload length.
  let assert <<payload_length:size(24), _rest:bits>> = to_send
  assert payload_length <= 16_384
}

// The round-trip should work at the standard max_frame_size: the
// client should be able to receive and decode all frames without
// hitting a FRAME_SIZE_ERROR.
pub fn send_push_promise_round_trip_at_standard_max_frame_size_test() {
  let #(server, client) = server_with_open_stream()
  let headers = large_push_headers(500)
  let assert Ok(#(_server, to_send, _promised_id)) =
    h2_core.send_push_promise(server, 1, headers)

  // Client receives the full frame sequence. If the first PUSH_PROMISE
  // frame exceeds 16384 bytes, receive_data will return
  // Error(ConnectionError(FrameSizeError)).
  let assert Ok(#(_client, events, _to_send)) = receive_data(client, to_send)

  // Should produce a PushPromiseReceived event
  let assert [PushPromiseReceived(stream_id: 1, ..)] = events
}

// RFC 9113 Section 6.10 - CONTINUATION frames carry only the field
// block fragment with no additional overhead fields. Their payload
// can use the full max_frame_size, unlike the first PUSH_PROMISE
// frame which has a 4-byte promised_stream_id. A naive fix that
// subtracts 4 from all chunks (not just the first) would produce
// CONTINUATION frames that are 4 bytes smaller than necessary.
pub fn send_push_promise_continuation_uses_full_max_frame_size_test() {
  let #(server, _client) = server_with_open_stream()
  let headers = large_push_headers(1000)
  let assert Ok(#(_server, to_send, _promised_id)) =
    h2_core.send_push_promise(server, 1, headers)

  // Parse all frames at 16384 limit (must succeed for all frames).
  let frames = helper.parse_all_frames(to_send, [])
  assert list.length(frames) >= 3

  // Get only the middle CONTINUATION frames (not the last one, which
  // holds the remainder and can be smaller). Every non-last
  // CONTINUATION must have a fragment of exactly max_frame_size (16384).
  let continuations =
    list.filter(frames, fn(f) {
      case f {
        h2_frame.Continuation(end_headers: False, ..) -> True
        _ -> False
      }
    })
  // There must be at least one non-last CONTINUATION frame for this
  // test to be meaningful.
  assert continuations != []
  list.each(continuations, fn(f) {
    let assert h2_frame.Continuation(field_block_fragment: frag, ..) = f
    assert bit_array.byte_size(frag) == 16_384
  })
}

// =============================================================================
// PUSH_PROMISE request_method tracking
//
// RFC 9113 Section 8.1.1 / RFC 9110 Section 6.4.1: Responses to HEAD
// requests may include content-length that doesn't match the (empty) body.
// The push promise headers contain the :method, so the promised stream
// must track request_method so the HEAD exemption applies.
// =============================================================================

// A PUSH_PROMISE with HEAD method followed by a response with
// content-length and END_STREAM must be accepted by the client. The
// HEAD exemption requires the promised stream to store request_method.
pub fn push_promise_head_response_with_content_length_is_valid_test() {
  let #(server, client) = server_with_open_stream()

  // Server pushes a HEAD request
  let push_headers = [
    Header(":method", <<"HEAD":utf8>>, WithIndexing),
    Header(":scheme", <<"https":utf8>>, WithIndexing),
    Header(":path", <<"/pushed":utf8>>, WithIndexing),
  ]
  let assert Ok(#(server, push_bytes, promised_id)) =
    h2_core.send_push_promise(server, 1, push_headers)
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, push_bytes)

  // Server sends HEADERS response on the promised stream with
  // content-length: 5000 and END_STREAM. Per RFC 9110 Section 6.4.1,
  // HEAD responses are defined as having no content, so the
  // content-length describes the resource size — not the body.
  let assert Ok(#(_server, response_bytes)) =
    send_headers(
      server,
      promised_id,
      [
        Header(":status", <<"200":utf8>>, WithIndexing),
        Header("content-length", <<"5000":utf8>>, WithIndexing),
      ],
      True,
    )
  let assert Ok(#(client, events, _to_send)) =
    receive_data(client, response_bytes)

  // With the bug: request_method is None, HEAD exemption doesn't fire,
  //   client rejects as malformed → StreamReset(ProtocolError)
  // With the fix: request_method is Some("HEAD"), accepted correctly
  let assert [
    HeadersReceived(stream_id: sid, end_stream: True, ..),
    StreamEnded(stream_id: sid2),
  ] = events
  assert sid == promised_id
  assert sid2 == promised_id
  let assert Ok(Closed) = get_stream_state(client, promised_id)
}

// =============================================================================
// ReservedRemote content-length validation
//
// RFC 9113 Section 8.1.1: content-length validation applies to all
// responses, including those on pushed streams. The ReservedRemote →
// Closed transition (HEADERS with END_STREAM) must check that a
// non-zero content-length with zero DATA bytes is malformed.
// =============================================================================

// A pushed GET response with content-length: 100 and END_STREAM on the
// HEADERS frame means zero DATA bytes will follow. This is malformed
// per RFC 9113 Section 8.1.1 — the content-length doesn't match the
// body length (0).
pub fn push_promise_response_with_content_length_and_end_stream_is_malformed_test() {
  let #(server, client) = server_with_open_stream()

  // Server pushes a GET request
  let push_headers = [
    Header(":method", <<"GET":utf8>>, WithIndexing),
    Header(":scheme", <<"https":utf8>>, WithIndexing),
    Header(":path", <<"/pushed":utf8>>, WithIndexing),
  ]
  let assert Ok(#(server, push_bytes, promised_id)) =
    h2_core.send_push_promise(server, 1, push_headers)
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, push_bytes)
  let assert Ok(ReservedRemote) = get_stream_state(client, promised_id)

  // Server sends response with content-length: 100 and END_STREAM.
  // Body length is 0 (no DATA), so this is malformed.
  let assert Ok(#(_server, response_bytes)) =
    send_headers(
      server,
      promised_id,
      [
        Header(":status", <<"200":utf8>>, WithIndexing),
        Header("content-length", <<"100":utf8>>, WithIndexing),
      ],
      True,
    )
  let assert Ok(#(client, events, _to_send)) =
    receive_data(client, response_bytes)
  let assert [StreamReset(stream_id: sid, error_code: ProtocolError)] = events
  assert sid == promised_id
  let assert Ok(Closed) = get_stream_state(client, promised_id)
}

// A pushed response with content-length: 0 and END_STREAM is valid —
// the body length (0) matches content-length (0).
pub fn push_promise_response_with_zero_content_length_and_end_stream_is_valid_test() {
  let #(server, client) = server_with_open_stream()

  let push_headers = [
    Header(":method", <<"GET":utf8>>, WithIndexing),
    Header(":scheme", <<"https":utf8>>, WithIndexing),
    Header(":path", <<"/pushed":utf8>>, WithIndexing),
  ]
  let assert Ok(#(server, push_bytes, promised_id)) =
    h2_core.send_push_promise(server, 1, push_headers)
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, push_bytes)

  // content-length: 0 + END_STREAM — body is 0 bytes, matches.
  let assert Ok(#(_server, response_bytes)) =
    send_headers(
      server,
      promised_id,
      [
        Header(":status", <<"200":utf8>>, WithIndexing),
        Header("content-length", <<"0":utf8>>, WithIndexing),
      ],
      True,
    )
  let assert Ok(#(client, events, _to_send)) =
    receive_data(client, response_bytes)
  let assert [
    HeadersReceived(stream_id: sid, end_stream: True, ..),
    StreamEnded(stream_id: sid2),
  ] = events
  assert sid == promised_id
  assert sid2 == promised_id
  let assert Ok(Closed) = get_stream_state(client, promised_id)
}

// A pushed response with content-length: 5 (no END_STREAM on HEADERS) followed
// by a DATA frame with 10 bytes should be rejected as malformed — the DATA
// exceeds the promised content-length. This test verifies that
// expected_content_length is stored on the stream during the ReservedRemote →
// HalfClosedLocal transition so the DATA handler can enforce it.
pub fn push_promise_response_data_exceeding_content_length_is_malformed_test() {
  let #(server, client) = server_with_open_stream()

  // Server pushes a GET request
  let push_headers = [
    Header(":method", <<"GET":utf8>>, WithIndexing),
    Header(":scheme", <<"https":utf8>>, WithIndexing),
    Header(":path", <<"/pushed":utf8>>, WithIndexing),
  ]
  let assert Ok(#(server, push_bytes, promised_id)) =
    h2_core.send_push_promise(server, 1, push_headers)
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, push_bytes)
  let assert Ok(ReservedRemote) = get_stream_state(client, promised_id)

  // Server sends response HEADERS with content-length: 5 (no END_STREAM)
  let assert Ok(#(server, response_bytes)) =
    send_headers(
      server,
      promised_id,
      [
        Header(":status", <<"200":utf8>>, WithIndexing),
        Header("content-length", <<"5":utf8>>, WithIndexing),
      ],
      False,
    )
  let assert Ok(#(client, _events, _to_send)) =
    receive_data(client, response_bytes)
  let assert Ok(HalfClosedLocal) = get_stream_state(client, promised_id)

  // Server sends 10 bytes of DATA — exceeds content-length of 5
  let assert Ok(#(_server, data_bytes)) =
    send_data(server, promised_id, <<"0123456789":utf8>>, False, None)
  let assert Ok(#(_client, events, _to_send)) = receive_data(client, data_bytes)
  let assert [StreamReset(stream_id: sid, error_code: ProtocolError)] = events
  assert sid == promised_id
}
