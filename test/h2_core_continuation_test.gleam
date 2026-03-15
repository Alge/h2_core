import gleam/bit_array
import gleam/dict
import gleam/list
import gleam/option
import h2_core.{
  Client, Connection, ConnectionError, HalfClosedLocal, Header, HeadersReceived,
  Open, Server, Settings, WithIndexing, new_connection, receive_data,
  send_headers,
}
import h2_frame

// Helper: create a connection with a small remote max_frame_size
// to force header blocks to be split across CONTINUATION frames.
// The minimum allowed max_frame_size per RFC 9113 is 16,384,
// but we set it artificially low here to make testing practical.
// The implementation should use remote_settings.max_frame_size
// to decide when to split.
fn connection_with_small_frame_size(role: h2_core.Role) -> h2_core.Connection {
  let conn = new_connection(role)
  let settings =
    Settings(
      ..conn.remote_settings,
      // Use a very small frame size to force splitting.
      // Note: 16,384 is the RFC minimum, but for testing we go lower.
      max_frame_size: 32,
    )
  Connection(..conn, remote_settings: settings)
}

// Generate enough headers to exceed a given frame size
fn large_headers() -> List(h2_core.Header) {
  // Each header with a unique name/value pair will take several bytes
  // when HPACK-encoded, easily exceeding 32 bytes total
  [
    Header(":method", "GET", WithIndexing),
    Header(":path", "/this/is/a/fairly/long/path/to/ensure/size", WithIndexing),
    Header(":scheme", "https", WithIndexing),
    Header(":authority", "www.example.com", WithIndexing),
    Header(
      "accept",
      "text/html,application/xhtml+xml,application/xml",
      WithIndexing,
    ),
    Header("user-agent", "h2_core-test/1.0", WithIndexing),
    Header("accept-language", "en-US,en;q=0.9", WithIndexing),
  ]
}

// --- Sending CONTINUATION ---

// RFC 9113 Section 6.2/6.10 - When the header block exceeds max_frame_size,
// send_headers should produce HEADERS + CONTINUATION frames

// Headers that fit in one frame still produce a single HEADERS frame
pub fn send_headers_small_block_no_continuation_test() {
  let conn = connection_with_small_frame_size(Client)
  // A single tiny header should fit in 32 bytes
  let headers = [Header(":method", "GET", WithIndexing)]
  let assert Ok(#(_conn, _events, to_send)) = send_headers(conn, headers, False)
  // Should parse as a single HEADERS frame with end_headers=True
  let assert Ok(#(frame, rest)) = h2_frame.parse(to_send)
  let assert h2_frame.Headers(
    stream_id: 1,
    end_stream: False,
    end_headers: True,
    priority: _,
    field_block_fragment: _,
  ) = frame
  assert rest == <<>>
}

// Large header block is split into HEADERS + CONTINUATION
pub fn send_headers_large_block_produces_continuation_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_conn, _events, to_send)) = send_headers(conn, headers, False)

  // First frame should be HEADERS with end_headers=False
  let assert Ok(#(frame, rest)) = h2_frame.parse(to_send)
  let assert h2_frame.Headers(
    stream_id: 1,
    end_stream: False,
    end_headers: False,
    priority: _,
    field_block_fragment: _,
  ) = frame
  // There should be remaining data (CONTINUATION frames)
  assert rest != <<>>
}

// The last CONTINUATION frame has end_headers=True
pub fn send_headers_continuation_last_has_end_headers_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_conn, _events, to_send)) = send_headers(conn, headers, False)

  // Parse all frames and check the last one has end_headers=True
  let frames = parse_all_frames(to_send, [])
  let assert Ok(last) = list.last(frames)
  case last {
    h2_frame.Continuation(end_headers: True, ..) -> Nil
    // If it fits in one frame, HEADERS should have end_headers=True
    h2_frame.Headers(end_headers: True, ..) -> Nil
    _ -> panic as "Last frame should have end_headers=True"
  }
}

// All CONTINUATION frames use the same stream_id as the HEADERS frame
pub fn send_headers_continuation_same_stream_id_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_conn, _events, to_send)) = send_headers(conn, headers, False)

  let frames = parse_all_frames(to_send, [])
  let assert [h2_frame.Headers(stream_id: sid, ..), ..continuations] = frames
  list.each(continuations, fn(frame) {
    let assert h2_frame.Continuation(stream_id: cont_sid, ..) = frame
    assert cont_sid == sid
  })
}

// Only the HEADERS frame carries end_stream, CONTINUATION frames do not
pub fn send_headers_end_stream_only_on_headers_frame_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(conn, _events, to_send)) = send_headers(conn, headers, True)

  let frames = parse_all_frames(to_send, [])
  let assert [h2_frame.Headers(end_stream: True, ..), ..continuations] = frames
  assert continuations != []

  // Stream should still be HalfClosedLocal
  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == HalfClosedLocal
}

// Stream state is correct after sending split headers without end_stream
pub fn send_headers_continuation_stream_state_open_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(conn, _events, to_send)) = send_headers(conn, headers, False)

  // Verify we actually got continuation frames
  let frames = parse_all_frames(to_send, [])
  assert list.length(frames) > 1

  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == Open
}

// The concatenated fragments from all frames can be HPACK-decoded
// by a receiver to recover the original headers
pub fn send_headers_continuation_round_trip_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_conn, _events, to_send)) = send_headers(conn, headers, False)

  // Verify there are continuation frames
  let frames = parse_all_frames(to_send, [])
  assert list.length(frames) > 1

  // Feed the entire output to a server and verify headers decode correctly
  let server = new_connection(Server)
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, to_send)
  let assert [
    HeadersReceived(stream_id: 1, headers: recv_headers, end_stream: False),
  ] = events
  // Check we got all headers back
  assert list.length(recv_headers) == list.length(headers)
  let assert [
    Header(":method", "GET", _),
    Header(":path", "/this/is/a/fairly/long/path/to/ensure/size", _),
    Header(":scheme", "https", _),
    Header(":authority", "www.example.com", _),
    Header("accept", "text/html,application/xhtml+xml,application/xml", _),
    Header("user-agent", "h2_core-test/1.0", _),
    Header("accept-language", "en-US,en;q=0.9", _),
  ] = recv_headers
}

// No intermediate CONTINUATION frames have end_headers=True
pub fn send_headers_intermediate_continuations_not_end_headers_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_conn, _events, to_send)) = send_headers(conn, headers, False)

  let frames = parse_all_frames(to_send, [])
  // All frames except the last should have end_headers=False
  let assert [_first, ..rest] = frames
  case list.reverse(rest) {
    [_last, ..middle_reversed] -> {
      list.each(list.reverse(middle_reversed), fn(frame) {
        let assert h2_frame.Continuation(end_headers: False, ..) = frame
      })
    }
    // Only one continuation frame, no middle frames to check
    [] -> Nil
  }
}

// HPACK encoder state is updated correctly even with split frames
pub fn send_headers_continuation_hpack_state_persists_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(conn, _events, first_send)) =
    send_headers(conn, headers, False)
  // Sending the same headers again should produce smaller output
  // because dynamic table entries were added on first send
  let assert Ok(#(_conn, _events, second_send)) =
    send_headers(conn, headers, False)

  let first_frames = parse_all_frames(first_send, [])
  let second_frames = parse_all_frames(second_send, [])
  // Second send should use fewer or equal frames
  assert list.length(second_frames) <= list.length(first_frames)
}

// --- Receiving CONTINUATION ---

// RFC 9113 Section 6.2 - A HEADERS frame without END_HEADERS MUST be followed
// by a CONTINUATION frame. Any other frame type is PROTOCOL_ERROR.

// Receiving a non-CONTINUATION frame while header block is incomplete is PROTOCOL_ERROR
pub fn receive_non_continuation_during_header_block_is_protocol_error_test() {
  let client = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_client, _events, to_send)) =
    send_headers(client, headers, False)

  // Parse just the first frame (HEADERS with end_headers=False)
  let assert Ok(#(h2_frame.Headers(end_headers: False, ..), rest)) =
    h2_frame.parse(to_send)

  // Re-encode just that HEADERS frame (without continuations)
  // by taking the original bytes minus the rest
  let headers_frame_size =
    bit_array.byte_size(to_send) - bit_array.byte_size(rest)
  let assert Ok(headers_only) = bit_array.slice(to_send, 0, headers_frame_size)

  // Append a PING frame instead of CONTINUATION
  let assert Ok(ping) = h2_frame.encode_ping(ack: False, data: <<1:64>>)
  let bad_data = <<headers_only:bits, ping:bits>>

  let server = new_connection(Server)
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, bad_data)
}

// Receiving SETTINGS while header block is incomplete is PROTOCOL_ERROR
pub fn receive_settings_during_header_block_is_protocol_error_test() {
  let client = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_client, _events, to_send)) =
    send_headers(client, headers, False)

  let assert Ok(#(h2_frame.Headers(end_headers: False, ..), rest)) =
    h2_frame.parse(to_send)

  let headers_frame_size =
    bit_array.byte_size(to_send) - bit_array.byte_size(rest)
  let assert Ok(headers_only) = bit_array.slice(to_send, 0, headers_frame_size)

  // Append a SETTINGS frame instead of CONTINUATION
  let assert Ok(settings) = h2_frame.encode_settings(ack: False, settings: [])
  let bad_data = <<headers_only:bits, settings:bits>>

  let server = new_connection(Server)
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, bad_data)
}

// Receiving CONTINUATION on a different stream is PROTOCOL_ERROR
pub fn receive_continuation_wrong_stream_is_protocol_error_test() {
  let client = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_client, _events, to_send)) =
    send_headers(client, headers, False)

  let assert Ok(#(h2_frame.Headers(end_headers: False, ..), rest)) =
    h2_frame.parse(to_send)

  let headers_frame_size =
    bit_array.byte_size(to_send) - bit_array.byte_size(rest)
  let assert Ok(headers_only) = bit_array.slice(to_send, 0, headers_frame_size)

  // Append a CONTINUATION for stream 99 instead of stream 1
  let assert Ok(wrong_cont) =
    h2_frame.encode_continuation(
      stream_id: 99,
      end_headers: True,
      field_block_fragment: <<>>,
    )
  let bad_data = <<headers_only:bits, wrong_cont:bits>>

  let server = new_connection(Server)
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, bad_data)
}

// Receiving CONTINUATION when no header block is pending is PROTOCOL_ERROR
pub fn receive_unexpected_continuation_is_protocol_error_test() {
  let server = new_connection(Server)
  let assert Ok(cont) =
    h2_frame.encode_continuation(
      stream_id: 1,
      end_headers: True,
      field_block_fragment: <<>>,
    )
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, cont)
}

// Helper: parse all frames from a BitArray
fn parse_all_frames(
  data: BitArray,
  acc: List(h2_frame.Frame),
) -> List(h2_frame.Frame) {
  case h2_frame.parse(data) {
    Ok(#(frame, rest)) -> parse_all_frames(rest, list.append(acc, [frame]))
    Error(_) -> acc
  }
}
