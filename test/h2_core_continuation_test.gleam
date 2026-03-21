import gleam/bit_array
import gleam/dict
import gleam/list
import h2_core.{
  Client, Closed, Connected, Connection, ConnectionError, HalfClosedLocal,
  HalfClosedRemote, Header, HeadersReceived, Open, Server, Settings, StreamReset,
  WithIndexing, open_stream, receive_data,
}
import h2_frame
import helper

// Helper: create a connection with a small remote max_frame_size
// to force header blocks to be split across CONTINUATION frames.
// The minimum allowed max_frame_size per RFC 9113 is 16,384,
// but we set it artificially low here to make testing practical.
// The implementation should use remote_settings.max_frame_size
// to decide when to split.
fn connection_with_small_frame_size(role: h2_core.Role) -> h2_core.Connection {
  let conn = helper.new_connection(role, Connected)
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
// open_stream should produce HEADERS + CONTINUATION frames

// Headers that fit in one frame still produce a single HEADERS frame
pub fn open_stream_small_block_no_continuation_test() {
  let conn = connection_with_small_frame_size(Client)
  // A single tiny header should fit in 32 bytes
  let headers = [Header(":method", "GET", WithIndexing)]
  let assert Ok(#(_conn, _events, to_send)) = open_stream(conn, headers, False)
  // Should parse as a single HEADERS frame with end_headers=True
  let assert Ok(#(frame_data, rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(frame) = h2_frame.decode_frame(frame_data)
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
pub fn open_stream_large_block_produces_continuation_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_conn, _events, to_send)) = open_stream(conn, headers, False)

  // First frame should be HEADERS with end_headers=False
  let assert Ok(#(frame_data, rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(frame) = h2_frame.decode_frame(frame_data)
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
pub fn open_stream_continuation_last_has_end_headers_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_conn, _events, to_send)) = open_stream(conn, headers, False)

  // Parse all frames and check the last one has end_headers=True
  let frames = helper.parse_all_frames(to_send, [])
  let assert Ok(last) = list.last(frames)
  case last {
    h2_frame.Continuation(end_headers: True, ..) -> Nil
    // If it fits in one frame, HEADERS should have end_headers=True
    h2_frame.Headers(end_headers: True, ..) -> Nil
    _ -> panic as "Last frame should have end_headers=True"
  }
}

// All CONTINUATION frames use the same stream_id as the HEADERS frame
pub fn open_stream_continuation_same_stream_id_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_conn, _events, to_send)) = open_stream(conn, headers, False)

  let frames = helper.parse_all_frames(to_send, [])
  let assert [h2_frame.Headers(stream_id: sid, ..), ..continuations] = frames
  list.each(continuations, fn(frame) {
    let assert h2_frame.Continuation(stream_id: cont_sid, ..) = frame
    assert cont_sid == sid
  })
}

// Only the HEADERS frame carries end_stream, CONTINUATION frames do not
pub fn open_stream_end_stream_only_on_headers_frame_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(conn, _events, to_send)) = open_stream(conn, headers, True)

  let frames = helper.parse_all_frames(to_send, [])
  let assert [h2_frame.Headers(end_stream: True, ..), ..continuations] = frames
  assert continuations != []

  // Stream should still be HalfClosedLocal
  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == HalfClosedLocal
}

// Stream state is correct after sending split headers without end_stream
pub fn open_stream_continuation_stream_state_open_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(conn, _events, to_send)) = open_stream(conn, headers, False)

  // Verify we actually got continuation frames
  let frames = helper.parse_all_frames(to_send, [])
  assert list.length(frames) > 1

  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == Open
}

// The concatenated fragments from all frames can be HPACK-decoded
// by a receiver to recover the original headers
pub fn open_stream_continuation_round_trip_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_conn, _events, to_send)) = open_stream(conn, headers, False)

  // Verify there are continuation frames
  let frames = helper.parse_all_frames(to_send, [])
  assert list.length(frames) > 1

  // Feed the entire output to a server and verify headers decode correctly
  let server = helper.new_connection(Server, Connected)
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
pub fn open_stream_intermediate_continuations_not_end_headers_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_conn, _events, to_send)) = open_stream(conn, headers, False)

  let frames = helper.parse_all_frames(to_send, [])
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
pub fn open_stream_continuation_hpack_state_persists_test() {
  let conn = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(conn, _events, first_send)) =
    open_stream(conn, headers, False)
  // Sending the same headers again should produce smaller output
  // because dynamic table entries were added on first send
  let assert Ok(#(_conn, _events, second_send)) =
    open_stream(conn, headers, False)

  let first_frames = helper.parse_all_frames(first_send, [])
  let second_frames = helper.parse_all_frames(second_send, [])
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
    open_stream(client, headers, False)

  // Parse just the first frame (HEADERS with end_headers=False)
  let assert Ok(#(frame_data, rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(h2_frame.Headers(end_headers: False, ..)) =
    h2_frame.decode_frame(frame_data)

  // Re-encode just that HEADERS frame (without continuations)
  // by taking the original bytes minus the rest
  let headers_frame_size =
    bit_array.byte_size(to_send) - bit_array.byte_size(rest)
  let assert Ok(headers_only) = bit_array.slice(to_send, 0, headers_frame_size)

  // Append a PING frame instead of CONTINUATION
  let assert Ok(ping) = h2_frame.encode_ping(ack: False, data: <<1:64>>)
  let bad_data = <<headers_only:bits, ping:bits>>

  let server = helper.new_connection(Server, Connected)
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, bad_data)
}

// Receiving SETTINGS while header block is incomplete is PROTOCOL_ERROR
pub fn receive_settings_during_header_block_is_protocol_error_test() {
  let client = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_client, _events, to_send)) =
    open_stream(client, headers, False)

  let assert Ok(#(frame_data, rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(h2_frame.Headers(end_headers: False, ..)) =
    h2_frame.decode_frame(frame_data)

  let headers_frame_size =
    bit_array.byte_size(to_send) - bit_array.byte_size(rest)
  let assert Ok(headers_only) = bit_array.slice(to_send, 0, headers_frame_size)

  // Append a SETTINGS frame instead of CONTINUATION
  let assert Ok(settings) = h2_frame.encode_settings(ack: False, settings: [])
  let bad_data = <<headers_only:bits, settings:bits>>

  let server = helper.new_connection(Server, Connected)
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, bad_data)
}

// Receiving CONTINUATION on a different stream is PROTOCOL_ERROR
pub fn receive_continuation_wrong_stream_is_protocol_error_test() {
  let client = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_client, _events, to_send)) =
    open_stream(client, headers, False)

  let assert Ok(#(frame_data, rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(h2_frame.Headers(end_headers: False, ..)) =
    h2_frame.decode_frame(frame_data)

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

  let server = helper.new_connection(Server, Connected)
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, bad_data)
}

// Receiving CONTINUATION when no header block is pending is PROTOCOL_ERROR
pub fn receive_unexpected_continuation_is_protocol_error_test() {
  let server = helper.new_connection(Server, Connected)
  let assert Ok(cont) =
    h2_frame.encode_continuation(
      stream_id: 1,
      end_headers: True,
      field_block_fragment: <<>>,
    )
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, cont)
}

// Pending header block persists across separate receive_data calls
pub fn receive_continuation_across_calls_test() {
  let client = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_client, _events, to_send)) =
    open_stream(client, headers, False)

  // Split: first frame in one call, rest in another
  let assert Ok(#(frame_data, rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(h2_frame.Headers(end_headers: False, ..)) =
    h2_frame.decode_frame(frame_data)
  let headers_frame_size =
    bit_array.byte_size(to_send) - bit_array.byte_size(rest)
  let assert Ok(headers_only) = bit_array.slice(to_send, 0, headers_frame_size)

  let server = helper.new_connection(Server, Connected)
  // First call: only the HEADERS frame (no events yet, block incomplete)
  let assert Ok(#(server, events1, _to_send)) =
    receive_data(server, headers_only)
  assert events1 == []

  // Second call: the remaining CONTINUATION frames
  let assert Ok(#(_server, events2, _to_send)) = receive_data(server, rest)
  let assert [
    HeadersReceived(stream_id: 1, headers: recv_headers, end_stream: False),
  ] = events2
  assert list.length(recv_headers) == list.length(headers)
}

// Header block split across 3+ frames (HEADERS + multiple CONTINUATIONs)
pub fn receive_multiple_continuation_frames_test() {
  // Use an even smaller frame size to force more splits
  let conn = helper.new_connection(Client, Connected)
  let settings = Settings(..conn.remote_settings, max_frame_size: 16)
  let conn = Connection(..conn, remote_settings: settings)
  let headers = large_headers()
  let assert Ok(#(_conn, _events, to_send)) = open_stream(conn, headers, False)

  // Verify we got at least 3 frames
  let frames = helper.parse_all_frames(to_send, [])
  assert list.length(frames) >= 3

  // Server should decode them all correctly
  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, to_send)
  let assert [
    HeadersReceived(stream_id: 1, headers: recv_headers, end_stream: False),
  ] = events
  assert list.length(recv_headers) == list.length(headers)
}

// After fully reassembling a HEADERS+CONTINUATION block, pending_header_blocks
// must be cleared. Otherwise subsequent non-CONTINUATION frames are rejected.
pub fn receive_continuation_clears_pending_state_test() {
  let client = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(client, _events, to_send)) =
    open_stream(client, headers, False)
  let frames = helper.parse_all_frames(to_send, [])
  assert list.length(frames) > 1

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, to_send)

  // A normal single-frame HEADERS on a new stream must work after reassembly
  let assert Ok(#(_client, _events, encoded2)) =
    open_stream(client, [Header(":method", "POST", WithIndexing)], False)
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded2)
  let assert [HeadersReceived(stream_id: 3, headers: _, end_stream: False)] =
    events
}

// end_stream from HEADERS is preserved through CONTINUATION reassembly
pub fn receive_continuation_preserves_end_stream_test() {
  let client = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_client, _events, to_send)) =
    open_stream(client, headers, True)

  // Verify it was actually split
  let frames = helper.parse_all_frames(to_send, [])
  assert list.length(frames) > 1

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, events, _to_send)) = receive_data(server, to_send)
  let assert [HeadersReceived(stream_id: 1, headers: _, end_stream: True)] =
    events
  // Stream should be HalfClosedRemote since end_stream was True
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == HalfClosedRemote
}

// Header order is preserved when split across multiple frames
pub fn receive_continuation_preserves_header_order_test() {
  let client = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_client, _events, to_send)) =
    open_stream(client, headers, False)

  // Verify it was actually split
  let frames = helper.parse_all_frames(to_send, [])
  assert list.length(frames) > 1

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, to_send)
  let assert [
    HeadersReceived(stream_id: 1, headers: recv_headers, end_stream: False),
  ] = events
  // Headers must arrive in exact same order
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

// RFC 9113 Section 4.3 - Invalid HPACK data in a CONTINUATION-reassembled
// block is a connection error of type COMPRESSION_ERROR.
pub fn receive_continuation_invalid_hpack_is_compression_error_test() {
  let client = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(_client, _events, to_send)) =
    open_stream(client, headers, False)

  // Parse the HEADERS frame, keep its bytes
  let assert Ok(#(frame_data, rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(h2_frame.Headers(end_headers: False, ..)) =
    h2_frame.decode_frame(frame_data)
  let headers_frame_size =
    bit_array.byte_size(to_send) - bit_array.byte_size(rest)
  let assert Ok(headers_only) = bit_array.slice(to_send, 0, headers_frame_size)

  // Append a CONTINUATION with invalid HPACK data and END_HEADERS
  let assert Ok(bad_cont) =
    h2_frame.encode_continuation(
      stream_id: 1,
      end_headers: True,
      field_block_fragment: <<0xFF, 0xFF, 0xFF>>,
    )
  let bad_data = <<headers_only:bits, bad_cont:bits>>

  let server = helper.new_connection(Server, Connected)
  let assert Error(ConnectionError(h2_frame.CompressionError)) =
    receive_data(server, bad_data)
}

// RFC 9113 Section 6.10 - CONTINUATION on stream 0 is PROTOCOL_ERROR
pub fn receive_continuation_on_stream_zero_is_protocol_error_test() {
  let server = helper.new_connection(Server, Connected)
  // Manually craft: Length=0, Type=0x09, Flags=0x04 (END_HEADERS), Stream ID=0
  let bad_cont = <<
    0:size(24),
    0x09:size(8),
    0x04:size(8),
    0:size(1),
    0:size(31),
  >>
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, bad_cont)
}

// --- Stream state validation for CONTINUATION reassembly path ---
// These mirror the stream state tests in h2_core_headers_recv_test but
// exercise the CONTINUATION code path (multi-frame header blocks).

// RFC 9113 Section 5.1 - Receiving HEADERS+CONTINUATION on an open stream
// is valid (e.g. trailers).
pub fn receive_continuation_on_open_stream_is_valid_test() {
  let client = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(client, _events, encoded1)) =
    open_stream(client, headers, False)

  let h2 = [
    Header(":method", "POST", WithIndexing),
    Header(":path", "/second/request/path", WithIndexing),
    Header("x-request-id", "aaaa-bbbb-cccc-dddd", WithIndexing),
  ]
  let assert Ok(#(_client, _events, encoded2)) = open_stream(client, h2, False)
  let frames = helper.parse_all_frames(encoded2, [])
  assert list.length(frames) > 1
  let patched = helper.patch_all_frames_stream_id(encoded2, 1)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Open

  let assert Ok(#(_server, events, to_send)) = receive_data(server, patched)
  let assert [HeadersReceived(stream_id: 1, headers: _, end_stream: False)] =
    events
  assert to_send == <<>>
}

// RFC 9113 Section 5.1 - Receiving HEADERS+CONTINUATION on a
// half-closed(local) stream is valid.
pub fn receive_continuation_on_half_closed_local_is_valid_test() {
  let client = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(client, _events, encoded1)) =
    open_stream(client, headers, False)

  let h2 = [
    Header(":method", "POST", WithIndexing),
    Header(":path", "/second/request/path", WithIndexing),
    Header("x-request-id", "aaaa-bbbb-cccc-dddd", WithIndexing),
  ]
  let assert Ok(#(_client, _events, encoded2)) = open_stream(client, h2, False)
  let frames = helper.parse_all_frames(encoded2, [])
  assert list.length(frames) > 1
  let patched = helper.patch_all_frames_stream_id(encoded2, 1)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  // Simulate server having sent END_STREAM
  let server = helper.set_stream_state(server, 1, HalfClosedLocal)

  let assert Ok(#(_server, events, to_send)) = receive_data(server, patched)
  let assert [HeadersReceived(stream_id: 1, headers: _, end_stream: False)] =
    events
  assert to_send == <<>>
}

// RFC 9113 Section 5.1 - An endpoint that sends RST_STREAM "MUST
// minimally process and then discard any frames it receives in this
// state. This means updating header compression state for HEADERS
// [...] frames." HEADERS+CONTINUATION on a closed stream must be
// silently discarded (HPACK decoded but no event, no response).
pub fn receive_continuation_on_closed_stream_is_discarded_test() {
  let client = connection_with_small_frame_size(Client)
  let headers = large_headers()
  let assert Ok(#(client, _events, encoded1)) =
    open_stream(client, headers, False)
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.Cancel)

  let h2 = [
    Header(":method", "POST", WithIndexing),
    Header(":path", "/second/request/path", WithIndexing),
    Header("x-request-id", "aaaa-bbbb-cccc-dddd", WithIndexing),
  ]
  let assert Ok(#(_client, _events, encoded2)) = open_stream(client, h2, False)
  let frames = helper.parse_all_frames(encoded2, [])
  assert list.length(frames) > 1
  let patched = helper.patch_all_frames_stream_id(encoded2, 1)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, rst)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Closed

  // Silently discarded — no events, no response
  let assert Ok(#(_server, events, to_send)) = receive_data(server, patched)
  assert events == []
  assert to_send == <<>>
}

// RFC 9113 Section 4.3 - HPACK state must be preserved when a
// HEADERS+CONTINUATION block is rejected. This tests the CONTINUATION
// reassembly path specifically (the HEADERS-only path is tested in
// h2_core_headers_recv_test).
pub fn receive_continuation_rejected_preserves_hpack_state_test() {
  let client = connection_with_small_frame_size(Client)
  // Use distinct headers for each send to prevent HPACK compression
  // from shrinking subsequent sends below the frame size limit
  let h1 = [
    Header(":method", "GET", WithIndexing),
    Header(":path", "/first/request/path", WithIndexing),
    Header("x-request-id", "aaaa-bbbb-cccc-dddd", WithIndexing),
  ]
  let h2 = [
    Header(":method", "POST", WithIndexing),
    Header(":path", "/second/request/path", WithIndexing),
    Header("x-request-id", "eeee-ffff-0000-1111", WithIndexing),
  ]
  let h3 = [
    Header(":method", "PUT", WithIndexing),
    Header(":path", "/third/request/path", WithIndexing),
    Header("x-request-id", "2222-3333-4444-5555", WithIndexing),
  ]
  // Block 1: stream 1 with END_STREAM (split across HEADERS+CONTINUATION)
  let assert Ok(#(client, _events, encoded1)) = open_stream(client, h1, True)
  // Block 2: stream 3 (split across HEADERS+CONTINUATION)
  let assert Ok(#(client, _events, encoded2)) = open_stream(client, h2, False)
  // Block 3: stream 5 (split across HEADERS+CONTINUATION)
  let assert Ok(#(_client, _events, encoded3)) = open_stream(client, h3, False)

  // Verify block 2 is actually split across multiple frames
  let frames = helper.parse_all_frames(encoded2, [])
  assert list.length(frames) > 1

  // Patch all frames in block 2 to target stream 1
  let patched2 = helper.patch_all_frames_stream_id(encoded2, 1)

  let server = helper.new_connection(Server, Connected)
  // Receive block 1 — stream 1 becomes HalfClosedRemote
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == HalfClosedRemote

  // Receive patched block 2 — rejected, but HPACK must still be decoded
  let assert Ok(#(server, events, _to_send)) = receive_data(server, patched2)
  let assert [StreamReset(stream_id: 1, error_code: h2_frame.StreamClosed)] =
    events

  // Block 3 on stream 5 proves HPACK state survived
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded3)
  let assert [HeadersReceived(stream_id: 5, headers: h, end_stream: False)] =
    events
  assert list.length(h) == list.length(h3)
}
