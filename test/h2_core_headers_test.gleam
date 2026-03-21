import gleam/bit_array
import gleam/dict
import gleam/option.{None}
import h2_core.{
  Client, Connected, HalfClosedLocal, Header, Open, Server, WithIndexing,
  open_stream,
}
import h2_frame
import helper

// RFC 9113 Section 6.2 - Sending HEADERS

// open_stream encodes headers and produces a HEADERS frame
pub fn open_stream_produces_encoded_frame_test() {
  let conn = helper.new_connection(Client, Connected)
  let headers = [
    Header(":method", "GET", WithIndexing),
    Header(":path", "/", WithIndexing),
    Header(":scheme", "https", WithIndexing),
  ]
  let assert Ok(#(_conn, _events, to_send)) = open_stream(conn, headers, False)
  // The output should be non-empty (actual HPACK-encoded HEADERS frame)
  assert to_send != <<>>
}

// open_stream opens a new stream in Open state
pub fn open_stream_opens_stream_test() {
  let conn = helper.new_connection(Client, Connected)
  let headers = helper.request_headers()
  let assert Ok(#(conn, _events, _to_send)) = open_stream(conn, headers, False)
  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == Open
}

// open_stream increments next_stream_id by 2
pub fn open_stream_increments_stream_id_test() {
  let conn = helper.new_connection(Client, Connected)
  let headers = helper.request_headers()
  let assert Ok(#(conn, _events, _to_send)) = open_stream(conn, headers, False)
  assert conn.next_stream_id == 3
  let assert Ok(#(conn, _events, _to_send)) = open_stream(conn, headers, False)
  assert conn.next_stream_id == 5
}

// Server uses even stream IDs
pub fn open_stream_server_uses_even_stream_ids_test() {
  let conn = helper.new_connection(Server, Connected)
  let headers = [Header(":status", "200", WithIndexing)]
  let assert Ok(#(conn, _events, _to_send)) = open_stream(conn, headers, False)
  let assert Ok(_stream) = dict.get(conn.streams, 2)
  assert conn.next_stream_id == 4
}

// RFC 9113 Section 6.2 - END_STREAM flag transitions stream to half-closed (local)
pub fn open_stream_with_end_stream_test() {
  let conn = helper.new_connection(Client, Connected)
  let headers = helper.request_headers()
  let assert Ok(#(conn, _events, _to_send)) = open_stream(conn, headers, True)
  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == HalfClosedLocal
}

// The encoded frame can be parsed back by h2_frame
pub fn open_stream_produces_parseable_frame_test() {
  let conn = helper.new_connection(Client, Connected)
  let headers = [
    Header(":method", "GET", WithIndexing),
    Header(":path", "/", WithIndexing),
  ]
  let assert Ok(#(_conn, _events, to_send)) = open_stream(conn, headers, False)
  // Parse the frame back
  let assert Ok(#(frame_data, _rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(frame) = h2_frame.decode_frame(frame_data)
  // Should be a Headers frame on stream 1
  let assert h2_frame.Headers(
    stream_id: 1,
    end_stream: False,
    end_headers: True,
    priority: None,
    field_block_fragment: _,
  ) = frame
}

// END_STREAM is reflected in the encoded frame
pub fn open_stream_end_stream_in_frame_test() {
  let conn = helper.new_connection(Client, Connected)
  let headers = helper.request_headers()
  let assert Ok(#(_conn, _events, to_send)) = open_stream(conn, headers, True)
  let assert Ok(#(frame_data, _rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(frame) = h2_frame.decode_frame(frame_data)
  let assert h2_frame.Headers(
    stream_id: 1,
    end_stream: True,
    end_headers: _,
    priority: _,
    field_block_fragment: _,
  ) = frame
}

// HPACK encoder state is updated after sending headers
pub fn open_stream_updates_hpack_encoder_test() {
  let conn = helper.new_connection(Client, Connected)
  let headers = [
    Header(":method", "GET", WithIndexing),
    Header("custom-header", "custom-value", WithIndexing),
  ]
  let assert Ok(#(conn, _events, first_send)) =
    open_stream(conn, headers, False)
  // Send the same headers again - HPACK should produce smaller output
  // because "custom-header: custom-value" is now in the dynamic table
  let assert Ok(#(_conn, _events, second_send)) =
    open_stream(conn, headers, False)
  assert bit_array.byte_size(second_send) < bit_array.byte_size(first_send)
}

// Sending headers with empty list produces a valid frame
pub fn open_stream_empty_headers_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Ok(#(conn, _events, to_send)) = open_stream(conn, [], False)
  assert to_send != <<>>
  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == Open
}
