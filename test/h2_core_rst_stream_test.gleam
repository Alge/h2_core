import gleam/dict
import h2_core.{
  Cancel, Client, Closed, Connected, ConnectionError, FrameSizeError,
  InternalError, Open, ProtocolError, Server, StreamReset, open_stream,
  receive_data, send_rst_stream,
}
import h2_frame
import helper

// RFC 9113 Section 6.4 - Sending RST_STREAM

pub fn send_rst_stream_returns_encoded_frame_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Ok(#(conn, _to_send)) =
    open_stream(conn, helper.request_headers(), False)
  let assert Ok(#(_conn, to_send)) = send_rst_stream(conn, 1, Cancel)
  let assert Ok(expected) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.Cancel)
  assert to_send == expected
}

pub fn send_rst_stream_with_different_error_codes_test() {
  let conn = helper.new_connection(Server, Connected)
  let assert Ok(#(conn, _to_send)) =
    open_stream(conn, helper.response_headers(), False)
  let assert Ok(#(_conn, to_send)) =
    send_rst_stream(conn, 2, InternalError)
  let assert Ok(expected) =
    h2_frame.encode_rst_stream(stream_id: 2, error_code: h2_frame.InternalError)
  assert to_send == expected
}

// RFC 9113 Section 6.4 - RST_STREAM on stream 0 is PROTOCOL_ERROR
pub fn send_rst_stream_on_stream_zero_is_error_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Error(ConnectionError(ProtocolError)) =
    send_rst_stream(conn, 0, Cancel)
}

// RFC 9113 Section 5.1 - "Either endpoint can send a RST_STREAM frame
// from this state, causing it to transition immediately to 'closed'."
pub fn send_rst_stream_transitions_to_closed_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Ok(#(conn, _to_send)) =
    open_stream(conn, helper.request_headers(), False)
  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == Open
  let assert Ok(#(conn, _to_send)) = send_rst_stream(conn, 1, Cancel)
  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == Closed
}

// RFC 9113 Section 6.4 - RST_STREAM MUST NOT be sent for idle stream
pub fn send_rst_stream_on_idle_stream_is_error_test() {
  let conn = helper.new_connection(Client, Connected)
  // Stream 1 was never opened, so it's idle
  let assert Error(ConnectionError(ProtocolError)) =
    send_rst_stream(conn, 1, Cancel)
}

// RFC 9113 Section 6.4 - Receiving RST_STREAM

pub fn receive_rst_stream_emits_event_test() {
  let conn = helper.new_connection(Client, Connected)
  // Open a stream first so it's not idle
  let assert Ok(#(conn, _to_send)) =
    open_stream(conn, helper.request_headers(), False)
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.Cancel)
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, rst)
  assert events == [StreamReset(stream_id: 1, error_code: Cancel)]
  assert to_send == <<>>
}

pub fn receive_rst_stream_with_internal_error_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Ok(#(conn, _to_send)) =
    open_stream(conn, helper.request_headers(), False)
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.InternalError)
  let assert Ok(#(_conn, events, _to_send)) = receive_data(conn, rst)
  assert events
    == [
      StreamReset(stream_id: 1, error_code: InternalError),
    ]
}

// RFC 9113 Section 6.4 - RST_STREAM transitions stream to closed
pub fn receive_rst_stream_closes_stream_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Ok(#(conn, _to_send)) =
    open_stream(conn, helper.request_headers(), False)
  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == Open
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.Cancel)
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, rst)
  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == Closed
}

// RFC 9113 Section 6.4 - RST_STREAM on stream 0 is PROTOCOL_ERROR
pub fn receive_rst_stream_on_stream_zero_is_protocol_error_test() {
  let conn = helper.new_connection(Client, Connected)
  // Manually craft RST_STREAM on stream 0
  // Length=4, Type=0x03, Flags=0, Stream ID=0, Error Code=0 (NoError)
  let bad_rst = <<
    4:size(24),
    0x03:size(8),
    0:size(8),
    0:size(1),
    0:size(31),
    0:size(32),
  >>
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(conn, bad_rst)
}

// RFC 9113 Section 6.4 - RST_STREAM on idle stream is PROTOCOL_ERROR
pub fn receive_rst_stream_on_idle_stream_is_protocol_error_test() {
  let conn = helper.new_connection(Client, Connected)
  // Stream 1 has never been opened, so it's idle
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.Cancel)
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(conn, rst)
}

// RFC 9113 Section 6.4 - Wrong frame size is FRAME_SIZE_ERROR
pub fn receive_rst_stream_wrong_length_test() {
  let conn = helper.new_connection(Client, Connected)
  // Manually craft RST_STREAM with 3 bytes payload instead of 4
  // Length=3, Type=0x03, Flags=0, Stream ID=1
  let bad_rst = <<
    3:size(24),
    0x03:size(8),
    0:size(8),
    0:size(1),
    1:size(31),
    1,
    2,
    3,
  >>
  let assert Error(ConnectionError(FrameSizeError)) =
    receive_data(conn, bad_rst)
}

// Receiving multiple frames in one receive_data call continues parsing
pub fn receive_rst_stream_continues_parse_loop_test() {
  let conn = helper.new_connection(Client, Connected)
  // Open two streams
  let assert Ok(#(conn, _to_send)) =
    open_stream(conn, helper.request_headers(), False)
  let assert Ok(#(conn, _to_send)) =
    open_stream(conn, helper.request_headers(), False)
  // Concatenate two RST_STREAM frames
  let assert Ok(rst1) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.Cancel)
  let assert Ok(rst2) =
    h2_frame.encode_rst_stream(stream_id: 3, error_code: h2_frame.InternalError)
  let combined = <<rst1:bits, rst2:bits>>
  let assert Ok(#(conn, events, _to_send)) = receive_data(conn, combined)
  assert events
    == [
      StreamReset(stream_id: 1, error_code: Cancel),
      StreamReset(stream_id: 3, error_code: InternalError),
    ]
  let assert Ok(s1) = dict.get(conn.streams, 1)
  let assert Ok(s3) = dict.get(conn.streams, 3)
  assert s1.state == Closed
  assert s3.state == Closed
}

// RST_STREAM does not send any response frame
pub fn receive_rst_stream_no_response_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Ok(#(conn, _to_send)) =
    open_stream(conn, helper.request_headers(), False)
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.NoError)
  let assert Ok(#(_conn, _events, to_send)) = receive_data(conn, rst)
  assert to_send == <<>>
}
