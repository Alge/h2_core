import gleam/option
import h2_core.{
  Cancel, Client, ConnectionError, FlowControlError, FrameSizeError, Header,
  InternalError, ProtocolError, Server, StreamClosed, StreamReset, UnknownStream,
  WithIndexing, open_stream, receive_data, send_rst_stream,
}
import h2_core/internal/stream.{Closed, HalfClosedRemote, Open}
import h2_frame
import helper

// RFC 9113 Section 6.4 - Sending RST_STREAM

pub fn send_rst_stream_returns_encoded_frame_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(#(conn, _to_send, _stream_id)) =
    open_stream(conn, helper.request_headers(), False)
  let assert Ok(#(_conn, to_send)) = send_rst_stream(conn, 1, Cancel)
  let assert Ok(expected) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.Cancel)
  assert to_send == expected
}

pub fn send_rst_stream_with_different_error_codes_test() {
  let #(server, _client) = helper.server_with_open_stream()
  let assert Ok(#(_conn, to_send)) = send_rst_stream(server, 1, InternalError)
  let assert Ok(expected) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.InternalError)
  assert to_send == expected
}

// RFC 9113 Section 6.4 - RST_STREAM on stream 0 is PROTOCOL_ERROR
pub fn send_rst_stream_on_stream_zero_is_error_test() {
  let conn = helper.connected_connection(Client)
  let assert Error(UnknownStream) = send_rst_stream(conn, 0, Cancel)
}

// RFC 9113 Section 5.1 - "Either endpoint can send a RST_STREAM frame
// from this state, causing it to transition immediately to 'closed'."
pub fn send_rst_stream_transitions_to_closed_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(#(conn, _to_send, _stream_id)) =
    open_stream(conn, helper.request_headers(), False)
  let assert Ok(Open) = h2_core.get_stream_state(conn, 1)
  let assert Ok(#(conn, _to_send)) = send_rst_stream(conn, 1, Cancel)
  let assert Ok(Closed) = h2_core.get_stream_state(conn, 1)
}

// RFC 9113 Section 6.4 - RST_STREAM MUST NOT be sent for idle stream
pub fn send_rst_stream_on_idle_stream_is_error_test() {
  let conn = helper.connected_connection(Client)
  // Stream 1 was never opened, so it's idle
  let assert Error(UnknownStream) = send_rst_stream(conn, 1, Cancel)
}

// RFC 9113 Section 6.4 - Receiving RST_STREAM

pub fn receive_rst_stream_emits_event_test() {
  let conn = helper.connected_connection(Client)
  // Open a stream first so it's not idle
  let assert Ok(#(conn, _to_send, _stream_id)) =
    open_stream(conn, helper.request_headers(), False)
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.Cancel)
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, rst)
  assert events == [StreamReset(stream_id: 1, error_code: Cancel)]
  assert to_send == <<>>
}

pub fn receive_rst_stream_with_internal_error_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(#(conn, _to_send, _stream_id)) =
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
  let conn = helper.connected_connection(Client)
  let assert Ok(#(conn, _to_send, _stream_id)) =
    open_stream(conn, helper.request_headers(), False)
  let assert Ok(Open) = h2_core.get_stream_state(conn, 1)
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.Cancel)
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, rst)
  let assert Ok(Closed) = h2_core.get_stream_state(conn, 1)
}

// RFC 9113 Section 6.4 - RST_STREAM on stream 0 is PROTOCOL_ERROR
pub fn receive_rst_stream_on_stream_zero_is_protocol_error_test() {
  let conn = helper.connected_connection(Client)
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
  let assert Error(ConnectionError(ProtocolError)) = receive_data(conn, bad_rst)
}

// RFC 9113 Section 6.4 - RST_STREAM on idle stream is PROTOCOL_ERROR
pub fn receive_rst_stream_on_idle_stream_is_protocol_error_test() {
  let conn = helper.connected_connection(Client)
  // Stream 1 has never been opened, so it's idle
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.Cancel)
  let assert Error(ConnectionError(ProtocolError)) = receive_data(conn, rst)
}

// RFC 9113 Section 6.4 - Wrong frame size is FRAME_SIZE_ERROR
pub fn receive_rst_stream_wrong_length_test() {
  let conn = helper.connected_connection(Client)
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
  let conn = helper.connected_connection(Client)
  // Open two streams
  let assert Ok(#(conn, _to_send, _stream_id)) =
    open_stream(conn, helper.request_headers(), False)
  let assert Ok(#(conn, _to_send, _stream_id)) =
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
  let assert Ok(Closed) = h2_core.get_stream_state(conn, 1)
  let assert Ok(Closed) = h2_core.get_stream_state(conn, 3)
}

// RST_STREAM does not send any response frame
pub fn receive_rst_stream_no_response_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(#(conn, _to_send, _stream_id)) =
    open_stream(conn, helper.request_headers(), False)
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.NoError)
  let assert Ok(#(_conn, _events, to_send)) = receive_data(conn, rst)
  assert to_send == <<>>
}

// =============================================================================
// RFC 9113 Section 6.4 - "The RST_STREAM frame fully terminates the
// referenced stream and causes it to enter the 'closed' state."
//
// When the library itself sends RST_STREAM in reaction to a protocol
// error (via the internal handle_rst_stream), the stream must also
// transition to Closed.
// =============================================================================

// Receiving HEADERS on a half-closed (remote) stream triggers an
// automatic RST_STREAM. The stream must be Closed afterwards.
pub fn handle_rst_stream_closes_stream_on_headers_half_closed_remote_test() {
  let #(server, _client) = helper.server_with_half_closed_remote_stream()
  let assert Ok(HalfClosedRemote) = h2_core.get_stream_state(server, 1)

  // Server sends response so we can craft a second HEADERS from the server
  // to trigger the error on the client side. But it's simpler to test from
  // the server side: send a raw HEADERS frame to the server on stream 1
  // which is half-closed (remote) - server should RST_STREAM it.
  let assert Ok(headers) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: option.None,
    )
  let assert Ok(#(server, events, _to_send)) = receive_data(server, headers)
  let assert [StreamReset(stream_id: 1, error_code: StreamClosed)] = events
  let assert Ok(Closed) = h2_core.get_stream_state(server, 1)
}

// DATA on a half-closed (remote) stream triggers automatic RST_STREAM.
// The stream must be Closed afterwards.
pub fn handle_rst_stream_closes_stream_on_data_half_closed_remote_test() {
  let #(server, _client) = helper.server_with_half_closed_remote_stream()
  let assert Ok(HalfClosedRemote) = h2_core.get_stream_state(server, 1)

  // Send a DATA frame on stream 1, which is half-closed (remote) on server
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"hello":utf8>>,
      padding: option.None,
    )
  let assert Ok(#(server, events, _to_send)) = receive_data(server, data_frame)
  let assert [StreamReset(stream_id: 1, error_code: StreamClosed)] = events
  let assert Ok(Closed) = h2_core.get_stream_state(server, 1)
}

// DATA exceeding content-length triggers automatic RST_STREAM.
// The stream must be Closed afterwards.
pub fn handle_rst_stream_closes_stream_on_content_length_exceeded_test() {
  let #(server, client) = helper.connected_pair()

  // Client opens stream with content-length: 5
  let assert Ok(#(client, headers_bytes, _stream_id)) =
    open_stream(
      client,
      [
        Header(":method", <<"POST":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
        Header("content-length", <<"5":utf8>>, WithIndexing),
      ],
      False,
    )
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, headers_bytes)
  let assert Ok(Open) = h2_core.get_stream_state(server, 1)

  // Client sends 10 bytes - exceeds content-length of 5
  let assert Ok(#(_client, data_bytes)) =
    h2_core.send_data(
      conn: client,
      stream_id: 1,
      data: <<"0123456789":utf8>>,
      end_stream: False,
      padding: option.None,
    )
  let assert Ok(#(server2, events, _to_send)) = receive_data(server, data_bytes)
  let assert [StreamReset(stream_id: 1, error_code: ProtocolError)] = events
  let assert Ok(Closed) = h2_core.get_stream_state(server2, 1)
}

// Stream-level flow control violation triggers automatic RST_STREAM.
// The stream must be Closed afterwards.
pub fn handle_rst_stream_closes_stream_on_flow_control_violation_test() {
  // Use a small initial window size so we can exceed it with a single frame.
  let small_window_settings =
    h2_core.Settings(..h2_core.default_settings(), initial_window_size: 100)
  let assert Ok(#(server, server_preface)) =
    h2_core.new_connection(Server, small_window_settings)
  let assert Ok(#(client, client_preface)) =
    h2_core.new_connection(Client, h2_core.default_settings())

  let assert Ok(#(server, _events, server_to_send)) =
    receive_data(server, client_preface)
  let assert Ok(#(client, _events, client_to_send)) =
    receive_data(client, server_preface)
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, client_to_send)
  let assert Ok(#(client, _events, _to_send)) =
    receive_data(client, server_to_send)

  // Client opens stream - server's recv window for the stream is 100 bytes
  let assert Ok(#(_client, headers_bytes, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, headers_bytes)
  let assert Ok(Open) = h2_core.get_stream_state(server, 1)

  // Craft a raw DATA frame with 200 bytes - exceeds the 100-byte window.
  // We bypass the client's send_data to avoid its flow control check.
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<0:size(1600)>>,
      padding: option.None,
    )
  let assert Ok(#(server, events, _to_send)) = receive_data(server, data_frame)
  let assert [StreamReset(stream_id: 1, error_code: FlowControlError)] = events
  let assert Ok(Closed) = h2_core.get_stream_state(server, 1)
}

// Malformed push promise headers trigger automatic RST_STREAM on the
// promised stream. The parent stream must remain open and unaffected.
pub fn handle_rst_stream_closes_stream_on_invalid_push_promise_test() {
  let #(_server, client) = helper.server_with_open_stream()

  // Send a PUSH_PROMISE with headers that have a pseudo-header after
  // a regular header - this is malformed per RFC 9113 Section 8.3.1.
  let bad_hpack = <<0x40, 0x05, "x-foo":utf8, 0x03, "bar":utf8, 0x82>>
  let assert Ok(pp) =
    h2_frame.encode_push_promise(
      stream_id: 1,
      end_headers: True,
      promised_stream_id: 2,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(client, events, _to_send)) = receive_data(client, pp)
  // Stream error targets the promised stream (2), not the parent (1).
  let assert [StreamReset(stream_id: 2, error_code: ProtocolError)] = events
  // Parent stream (1) is unaffected and remains open.
  let assert Ok(Open) = h2_core.get_stream_state(client, 1)
  // Promised stream (2) is never added to conn.streams - validation fails before creation.
  let assert Error(Nil) = h2_core.get_stream_state(client, 2)
}
