import gleam/list
import h2_core.{
  Client, Connected, Connection, ConnectionError, GoawayReceived, Header, Server,
  WithIndexing, open_stream, receive_data, send_goaway, send_headers,
}
import h2_frame
import helper

// RFC 9113 Section 6.8 - Sending GOAWAY

pub fn send_goaway_returns_encoded_frame_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Ok(#(_conn, to_send)) = send_goaway(conn, h2_frame.NoError, <<>>)
  let expected =
    h2_frame.encode_goaway(
      last_stream_id: 0,
      error_code: h2_frame.NoError,
      debug_data: <<>>,
    )
  assert to_send == expected
}

pub fn send_goaway_with_error_code_test() {
  let conn = helper.new_connection(Server, Connected)
  let assert Ok(#(_conn, to_send)) =
    send_goaway(conn, h2_frame.ProtocolError, <<>>)
  let expected =
    h2_frame.encode_goaway(
      last_stream_id: 0,
      error_code: h2_frame.ProtocolError,
      debug_data: <<>>,
    )
  assert to_send == expected
}

pub fn send_goaway_uses_last_remote_stream_id_test() {
  let conn = helper.new_connection(Server, Connected)
  let conn = Connection(..conn, last_remote_stream_id: 7)
  let assert Ok(#(_conn, to_send)) = send_goaway(conn, h2_frame.NoError, <<>>)
  let expected =
    h2_frame.encode_goaway(
      last_stream_id: 7,
      error_code: h2_frame.NoError,
      debug_data: <<>>,
    )
  assert to_send == expected
}

pub fn send_goaway_with_debug_data_test() {
  let conn = helper.new_connection(Client, Connected)
  let debug = <<"something went wrong":utf8>>
  let assert Ok(#(_conn, to_send)) =
    send_goaway(conn, h2_frame.InternalError, debug)
  let expected =
    h2_frame.encode_goaway(
      last_stream_id: 0,
      error_code: h2_frame.InternalError,
      debug_data: debug,
    )
  assert to_send == expected
}

// RFC 9113 Section 6.8 - Receiving GOAWAY

pub fn receive_goaway_emits_event_test() {
  let conn = helper.new_connection(Client, Connected)
  let goaway =
    h2_frame.encode_goaway(
      last_stream_id: 0,
      error_code: h2_frame.NoError,
      debug_data: <<>>,
    )
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, goaway)
  assert events == [GoawayReceived(0, h2_frame.NoError, <<>>)]
  assert to_send == <<>>
}

pub fn receive_goaway_with_error_code_test() {
  let conn = helper.new_connection(Client, Connected)
  let goaway =
    h2_frame.encode_goaway(
      last_stream_id: 5,
      error_code: h2_frame.InternalError,
      debug_data: <<>>,
    )
  let assert Ok(#(_conn, events, _to_send)) = receive_data(conn, goaway)
  assert events == [GoawayReceived(5, h2_frame.InternalError, <<>>)]
}

pub fn receive_goaway_with_debug_data_test() {
  let conn = helper.new_connection(Client, Connected)
  let debug = <<"too many requests":utf8>>
  let goaway =
    h2_frame.encode_goaway(
      last_stream_id: 3,
      error_code: h2_frame.EnhanceYourCalm,
      debug_data: debug,
    )
  let assert Ok(#(_conn, events, _to_send)) = receive_data(conn, goaway)
  assert events == [GoawayReceived(3, h2_frame.EnhanceYourCalm, debug)]
}

pub fn receive_goaway_no_response_sent_test() {
  let conn = helper.new_connection(Client, Connected)
  let goaway =
    h2_frame.encode_goaway(
      last_stream_id: 0,
      error_code: h2_frame.NoError,
      debug_data: <<>>,
    )
  let assert Ok(#(_conn, _events, to_send)) = receive_data(conn, goaway)
  // GOAWAY does not trigger any response frame
  assert to_send == <<>>
}

// RFC 9113 Section 6.8 - GOAWAY on non-zero stream is PROTOCOL_ERROR
pub fn receive_goaway_nonzero_stream_is_protocol_error_test() {
  let conn = helper.new_connection(Client, Connected)
  // Manually craft a GOAWAY frame on stream 1
  // Length=8 (4 last_stream_id + 4 error_code, no debug), Type=0x07, Flags=0, Stream ID=1
  let bad_goaway = <<
    8:size(24),
    0x07:size(8),
    0:size(8),
    0:size(1),
    1:size(31),
    0:size(1),
    0:size(31),
    0:size(32),
  >>
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(conn, bad_goaway)
}

// RFC 9113 Section 6.8 - last_stream_id can be 0 (no streams processed)
pub fn receive_goaway_last_stream_id_zero_test() {
  let conn = helper.new_connection(Server, Connected)
  let goaway =
    h2_frame.encode_goaway(
      last_stream_id: 0,
      error_code: h2_frame.NoError,
      debug_data: <<>>,
    )
  let assert Ok(#(_conn, events, _to_send)) = receive_data(conn, goaway)
  assert events == [GoawayReceived(0, h2_frame.NoError, <<>>)]
}

// Receiving GOAWAY should not clear the receive buffer of remaining data
pub fn receive_goaway_does_not_affect_buffer_test() {
  let conn = helper.new_connection(Client, Connected)
  let goaway =
    h2_frame.encode_goaway(
      last_stream_id: 0,
      error_code: h2_frame.NoError,
      debug_data: <<>>,
    )
  // Append some trailing bytes that represent a partial frame header
  // (length=0, then 2 more header bytes — not enough for a full 9-byte header)
  let trailing = <<0, 0, 0, 0x06, 0>>
  let data = <<goaway:bits, trailing:bits>>
  let assert Ok(#(conn, events, _to_send)) = receive_data(conn, data)
  assert events == [GoawayReceived(0, h2_frame.NoError, <<>>)]
  assert conn.recv_buffer == trailing
}

// RFC 9113 Section 6.8 - "However, any frames that alter connection
// state cannot be completely ignored. For instance, HEADERS,
// PUSH_PROMISE, and CONTINUATION frames MUST be minimally processed
// to ensure that the state maintained for field section compression
// is consistent (see Section 4.3)."
//
// After receiving GOAWAY, the server must still HPACK-decode HEADERS
// frames (even on streams above last_stream_id) to keep the dynamic
// table in sync. If it doesn't, subsequent header decoding will fail
// with a CompressionError.
pub fn receive_headers_after_goaway_maintains_hpack_state_test() {
  let server = helper.new_connection(Server, Connected)
  let client = helper.new_connection(Client, Connected)

  // Client sends three HEADERS frames — HPACK state accumulates across all three
  let assert Ok(#(client, encoded1)) =
    open_stream(
      client,
      list.append(helper.request_headers(), [
        Header("x-custom", "value1", WithIndexing),
      ]),
      False,
    )
  let assert Ok(#(client, encoded2)) =
    open_stream(
      client,
      list.append(helper.request_headers(), [
        Header("x-custom", "value2", WithIndexing),
      ]),
      False,
    )
  let assert Ok(#(_client, encoded3)) =
    open_stream(
      client,
      list.append(helper.request_headers(), [
        Header("x-custom", "value3", WithIndexing),
      ]),
      False,
    )

  // Server receives stream 1
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)

  // Server sends GOAWAY with last_stream_id=1
  let assert Ok(#(server, _to_send)) =
    send_goaway(server, h2_frame.NoError, <<>>)

  // Server receives stream 3 (above last_stream_id in the GOAWAY we sent,
  // silently discarded but HPACK-decoded to maintain compression state)
  let assert Ok(#(server, events2, _to_send)) = receive_data(server, encoded2)
  assert events2 == []

  // Server receives stream 5 — also discarded, but if HPACK state from
  // stream 3 was not decoded, this will fail with CompressionError
  let assert Ok(#(_server, events3, _to_send)) = receive_data(server, encoded3)
  assert events3 == []
}

// RFC 9113 Section 6.8 - "Receivers of a GOAWAY frame MUST NOT open
// additional streams on the connection."
//
// After receiving GOAWAY, attempting to open a new stream should error.
pub fn receive_goaway_prevents_opening_new_streams_test() {
  let client = helper.new_connection(Client, Connected)
  // Open stream 1
  let assert Ok(#(client, _to_send)) =
    open_stream(client, helper.request_headers(), False)

  // Receive GOAWAY from server
  let goaway =
    h2_frame.encode_goaway(
      last_stream_id: 1,
      error_code: h2_frame.NoError,
      debug_data: <<>>,
    )
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, goaway)

  // Attempt to open a new stream — must be rejected
  let assert Error(_) = open_stream(client, helper.request_headers(), False)
}

// RFC 9113 Section 6.8 - "Activity on streams numbered lower than or
// equal to the last stream identifier might still complete successfully."
//
// After receiving GOAWAY, existing streams should still work.
pub fn receive_goaway_existing_streams_still_work_test() {
  let #(_server, client) = helper.server_with_open_stream()

  // Receive GOAWAY from server (as client)
  let goaway =
    h2_frame.encode_goaway(
      last_stream_id: 1,
      error_code: h2_frame.NoError,
      debug_data: <<>>,
    )
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, goaway)

  // Client can still send trailers on stream 1
  let assert Ok(#(_client, _to_send)) =
    send_headers(client, 1, [Header("x-trailer", "done", WithIndexing)], True)
}

// RFC 9113 Section 6.8 - After receiving GOAWAY, the server should not
// be able to send PUSH_PROMISE (which opens new streams).
pub fn receive_goaway_prevents_push_promise_test() {
  let #(server, _client) = helper.server_with_open_stream()

  // Server sends GOAWAY
  let assert Ok(#(server, _to_send)) =
    send_goaway(server, h2_frame.NoError, <<>>)

  // Server tries to push — must be rejected (no new streams)
  let assert Error(_) =
    h2_core.send_push_promise(server, 1, helper.request_headers())
}

// RFC 9113 Section 6.8 - "Endpoints MUST NOT increase the value they
// send in the last stream identifier."
//
// Sending a second GOAWAY with a higher last_stream_id should error.
pub fn send_goaway_must_not_increase_last_stream_id_test() {
  let server = helper.new_connection(Server, Connected)
  let client = helper.new_connection(Client, Connected)
  let assert Ok(#(client, headers1)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(_client, headers2)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers1)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers2)

  // First GOAWAY — last_stream_id will be 3 (last_remote_stream_id)
  let assert Ok(#(server, _to_send)) =
    send_goaway(server, h2_frame.NoError, <<>>)

  // Manually lower last_remote_stream_id to simulate wanting to send
  // a second GOAWAY with a higher value — the library should prevent this.
  // Since send_goaway uses last_remote_stream_id, we'd need to receive
  // a new stream to increase it. But after sending GOAWAY, the server
  // shouldn't accept new streams. Instead, let's verify the second
  // GOAWAY has the same or lower last_stream_id.
  let assert Ok(#(_server, goaway2)) =
    send_goaway(server, h2_frame.NoError, <<>>)

  // Decode the GOAWAY to verify last_stream_id didn't increase
  let assert Ok(#(frame_data, _rest)) = h2_frame.extract_frame(goaway2, 16_384)
  let assert Ok(h2_frame.Goaway(last_stream_id: last_id, ..)) =
    h2_frame.decode_frame(frame_data)
  assert last_id == 3
}

// RFC 9113 Section 6.8 - "Once the GOAWAY is sent, the sender will
// ignore frames sent on streams initiated by the receiver if the
// stream has an identifier higher than the included last stream
// identifier."
//
// After sending GOAWAY with last_stream_id=1, receiving HEADERS on
// stream 3 (above last_stream_id) should be silently discarded.
// HPACK state must still be maintained.
pub fn send_goaway_ignores_streams_above_last_stream_id_test() {
  let server = helper.new_connection(Server, Connected)
  let client = helper.new_connection(Client, Connected)
  // Client opens stream 1
  let assert Ok(#(client, headers1)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers1)

  // Server sends GOAWAY with last_stream_id=1
  let assert Ok(#(server, _to_send)) =
    send_goaway(server, h2_frame.NoError, <<>>)

  // Client opens stream 3 (doesn't know about GOAWAY yet)
  let assert Ok(#(client, headers3)) =
    open_stream(
      client,
      list.append(helper.request_headers(), [
        Header("x-custom", "value", WithIndexing),
      ]),
      False,
    )

  // Server receives stream 3 — should be silently discarded (no event)
  // but HPACK state must be maintained
  let assert Ok(#(server, events, _to_send)) = receive_data(server, headers3)
  assert events == []

  // Client opens stream 5 — also above last_stream_id, also discarded
  // If HPACK state from stream 3 was not decoded, this will fail
  // with CompressionError
  let assert Ok(#(_client, headers5)) =
    open_stream(
      client,
      list.append(helper.request_headers(), [
        Header("x-other", "test", WithIndexing),
      ]),
      False,
    )
  let assert Ok(#(_server, events2, _to_send)) = receive_data(server, headers5)
  assert events2 == []
}
