import gleam/dict
import gleam/option.{None, Some}
import h2_core.{
  type Connection, Client, ConnectionError, DataReceived, Header,
  HalfClosedRemote, Open, Server, Stream, StreamError, StreamReset, WithIndexing,
  new_connection, receive_data, send_headers,
}
import h2_frame

// Helper: create a server with an open client-initiated stream 1
fn server_with_open_stream() -> #(Connection, Connection) {
  let server = new_connection(Server)
  let client = new_connection(Client)
  let assert Ok(#(client, _events, headers)) =
    send_headers(client, [Header(":method", "GET", WithIndexing)], False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  #(server, client)
}

// Helper: create a server with a half-closed (remote) stream 1
// (client sent END_STREAM with headers)
fn server_with_half_closed_remote_stream() -> #(Connection, Connection) {
  let server = new_connection(Server)
  let client = new_connection(Client)
  let assert Ok(#(client, _events, headers)) =
    send_headers(client, [Header(":method", "GET", WithIndexing)], True)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  #(server, client)
}

// =============================================================================
// Receiving DATA frames
// =============================================================================

// RFC 9113 Section 6.1 - "DATA frames MUST be associated with a stream.
// If a DATA frame is received whose Stream Identifier field is 0x00, the
// recipient MUST respond with a connection error (Section 5.4.1) of type
// PROTOCOL_ERROR."
pub fn receive_data_on_stream_zero_is_protocol_error_test() {
  let #(server, _client) = server_with_open_stream()
  // Manually craft a DATA frame on stream 0
  // Length=5, Type=0x00, Flags=0, Stream ID=0
  let bad_data = <<
    5:size(24),
    0x00:size(8),
    0:size(8),
    0:size(1),
    0:size(31),
    "hello":utf8,
  >>
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, bad_data)
}

// RFC 9113 Section 6.1 - Basic DATA frame reception on an open stream
// emits a DataReceived event with the payload.
pub fn receive_data_on_open_stream_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"hello":utf8>>,
      padding: None,
    )
  let assert Ok(#(_server, events, _to_send)) =
    receive_data(server, data_frame)
  assert events == [DataReceived(stream_id: 1, data: <<"hello":utf8>>, end_stream: False)]
}

// RFC 9113 Section 6.1 - DATA with END_STREAM flag transitions stream to
// half-closed (remote) on the receiver side.
pub fn receive_data_with_end_stream_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: True,
      data: <<"goodbye":utf8>>,
      padding: None,
    )
  let assert Ok(#(server, events, _to_send)) =
    receive_data(server, data_frame)
  assert events == [DataReceived(stream_id: 1, data: <<"goodbye":utf8>>, end_stream: True)]
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == h2_core.HalfClosedRemote
}

// RFC 9113 Section 6.1 - DATA with END_STREAM on a half-closed (local)
// stream transitions to closed.
pub fn receive_data_with_end_stream_on_half_closed_local_closes_stream_test() {
  let server = new_connection(Server)
  let client = new_connection(Client)
  // Client opens stream 1
  let assert Ok(#(client, _events, headers)) =
    send_headers(client, [Header(":method", "GET", WithIndexing)], False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  // Server sends headers with END_STREAM, making it half-closed (local)
  let assert Ok(#(server, _events, response_headers)) =
    send_headers(server, [Header(":status", "200", WithIndexing)], True)
  let assert Ok(#(_client, _events, _to_send)) =
    receive_data(client, response_headers)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == h2_core.HalfClosedLocal

  // Client sends DATA with END_STREAM
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: True,
      data: <<"done":utf8>>,
      padding: None,
    )
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, data_frame)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == h2_core.Closed
}

// RFC 9113 Section 6.1 - "If a DATA frame is received whose stream is not
// in the 'open' or 'half-closed (local)' state, the recipient MUST respond
// with a stream error (Section 5.4.2) of type STREAM_CLOSED."
pub fn receive_data_on_half_closed_remote_is_stream_closed_error_test() {
  let #(server, _client) = server_with_half_closed_remote_stream()
  // Stream 1 is half-closed (remote) — client already sent END_STREAM
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"illegal":utf8>>,
      padding: None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal. The endpoint
  // sends RST_STREAM and continues processing.
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, data_frame)
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(
      stream_id: 1,
      error_code: h2_frame.StreamClosed,
    )
  assert to_send == expected_rst
  assert events == [StreamReset(stream_id: 1, error_code: h2_frame.StreamClosed)]
}

pub fn receive_data_on_idle_stream_is_protocol_error_test() {
  let server = new_connection(Server)
  // Stream 1 was never opened
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"bad":utf8>>,
      padding: None,
    )
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, data_frame)
}

// RFC 9113 Section 6.1 - Empty DATA frame is valid.
pub fn receive_empty_data_frame_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<>>,
      padding: None,
    )
  let assert Ok(#(_server, events, _to_send)) =
    receive_data(server, data_frame)
  assert events == [DataReceived(stream_id: 1, data: <<>>, end_stream: False)]
}

// RFC 9113 Section 6.1 - Empty DATA frame with END_STREAM is valid
// (used to close a stream without sending additional data).
pub fn receive_empty_data_frame_with_end_stream_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: True,
      data: <<>>,
      padding: None,
    )
  let assert Ok(#(server, events, _to_send)) =
    receive_data(server, data_frame)
  assert events == [DataReceived(stream_id: 1, data: <<>>, end_stream: True)]
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == h2_core.HalfClosedRemote
}

// =============================================================================
// Flow control - receiving DATA
// =============================================================================

// RFC 9113 Section 6.1 - "DATA frames are subject to flow control and can
// only be sent when a stream is in the 'open' or 'half-closed (remote)'
// state. The entire DATA frame payload is included in flow control,
// including the Pad Length and Padding fields if present."
//
// Section 5.2.1 - "A sender MUST respect flow-control limits imposed by
// a receiver."
//
// Receiving DATA must decrement both the stream and connection
// recv_window_size.
pub fn receive_data_decrements_recv_window_test() {
  let #(server, _client) = server_with_open_stream()
  let data = <<"hello world":utf8>>
  let data_size = 11
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: data,
      padding: None,
    )
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, data_frame)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.recv_window_size == 65_535 - data_size
  assert server.recv_window_size == 65_535 - data_size
}

// RFC 9113 Section 6.9 - "A receiver that receives a flow-controlled frame
// MUST always account for its contribution against the connection
// flow-control window, unless the receiver treats this as a connection error
// (Section 5.4.1). This is necessary even if the frame is in error."
pub fn receive_data_exceeding_stream_window_is_flow_control_error_test() {
  let #(server, _client) = server_with_open_stream()
  // Set stream recv_window_size to 5 bytes
  let server =
    h2_core.Connection(
      ..server,
      streams: dict.insert(
        server.streams,
        1,
        Stream(state: Open, send_window_size: 65_535, recv_window_size: 5),
      ),
    )
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"too much data":utf8>>,
      padding: None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal. The endpoint
  // sends RST_STREAM and continues processing.
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, data_frame)
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(
      stream_id: 1,
      error_code: h2_frame.FlowControlError,
    )
  assert to_send == expected_rst
  assert events == [StreamReset(stream_id: 1, error_code: h2_frame.FlowControlError)]
}

pub fn receive_data_exceeding_connection_window_is_flow_control_error_test() {
  let #(server, _client) = server_with_open_stream()
  // Set connection recv_window_size to 5 bytes
  let server = h2_core.Connection(..server, recv_window_size: 5)
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"too much data":utf8>>,
      padding: None,
    )
  let assert Error(ConnectionError(h2_frame.FlowControlError)) =
    receive_data(server, data_frame)
}

// =============================================================================
// Sending DATA frames
// =============================================================================

// RFC 9113 Section 6.1 - "DATA frames are subject to flow control and can
// only be sent when a stream is in the 'open' or 'half-closed (remote)'
// state."
pub fn send_data_on_open_stream_test() {
  let #(server, _client) = server_with_open_stream()
  // Server sends response headers first (non-END_STREAM)
  let assert Ok(#(server, _events, _to_send)) =
    send_headers(server, [Header(":status", "200", WithIndexing)], False)
  let assert Ok(#(server, _events, to_send)) =
    h2_core.send_data(server, 1, <<"hello":utf8>>, False)
  let assert Ok(expected) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"hello":utf8>>,
      padding: None,
    )
  assert to_send == expected
  // send_window_size should be decremented
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.send_window_size == 65_535 - 5
}

pub fn send_data_with_end_stream_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _events, _to_send)) =
    send_headers(server, [Header(":status", "200", WithIndexing)], False)
  let assert Ok(#(server, _events, _to_send)) =
    h2_core.send_data(server, 1, <<"done":utf8>>, True)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == h2_core.HalfClosedLocal
}

// send_data exceeding the send window should return an error
pub fn send_data_exceeding_stream_window_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _events, _to_send)) =
    send_headers(server, [Header(":status", "200", WithIndexing)], False)
  // Set stream send_window_size to 5
  let server =
    h2_core.Connection(
      ..server,
      streams: dict.insert(
        server.streams,
        1,
        Stream(state: Open, send_window_size: 5, recv_window_size: 65_535),
      ),
    )
  let assert Error(ConnectionError(h2_frame.FlowControlError)) =
    h2_core.send_data(server, 1, <<"too much data":utf8>>, False)
}

pub fn send_data_exceeding_connection_window_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _events, _to_send)) =
    send_headers(server, [Header(":status", "200", WithIndexing)], False)
  // Set connection send_window_size to 5
  let server = h2_core.Connection(..server, send_window_size: 5)
  let assert Error(ConnectionError(h2_frame.FlowControlError)) =
    h2_core.send_data(server, 1, <<"too much data":utf8>>, False)
}

// send_data on stream 0 should error
pub fn send_data_on_stream_zero_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    h2_core.send_data(server, 0, <<"bad":utf8>>, False)
}

// send_data on a stream that is not open or half-closed (remote) should error
pub fn send_data_on_half_closed_local_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  // Server sends headers with END_STREAM
  let assert Ok(#(server, _events, _to_send)) =
    send_headers(server, [Header(":status", "200", WithIndexing)], True)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == h2_core.HalfClosedLocal
  let assert Error(StreamError(1, h2_frame.StreamClosed)) =
    h2_core.send_data(server, 1, <<"bad":utf8>>, False)
}

// send_data should decrement both stream and connection send_window_size
pub fn send_data_decrements_send_window_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _events, _to_send)) =
    send_headers(server, [Header(":status", "200", WithIndexing)], False)
  let assert Ok(#(server, _events, _to_send)) =
    h2_core.send_data(server, 1, <<"hello world":utf8>>, False)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.send_window_size == 65_535 - 11
  assert server.send_window_size == 65_535 - 11
}

// send_data with empty data should be valid
pub fn send_data_empty_payload_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _events, _to_send)) =
    send_headers(server, [Header(":status", "200", WithIndexing)], False)
  let assert Ok(#(_server, _events, to_send)) =
    h2_core.send_data(server, 1, <<>>, False)
  let assert Ok(expected) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<>>,
      padding: None,
    )
  assert to_send == expected
}

// send_data with empty data and END_STREAM to close a stream
pub fn send_data_empty_with_end_stream_closes_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _events, _to_send)) =
    send_headers(server, [Header(":status", "200", WithIndexing)], False)
  let assert Ok(#(server, _events, _to_send)) =
    h2_core.send_data(server, 1, <<>>, True)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == h2_core.HalfClosedLocal
}

// =============================================================================
// get_send_window_size - public API for callers to check available window
// =============================================================================

// Returns the minimum of stream and connection send window
pub fn get_send_window_size_returns_stream_window_when_smaller_test() {
  let #(server, _client) = server_with_open_stream()
  // Set stream window smaller than connection window
  let server =
    h2_core.Connection(
      ..server,
      streams: dict.insert(
        server.streams,
        1,
        Stream(state: Open, send_window_size: 100, recv_window_size: 65_535),
      ),
    )
  let assert Ok(window) = h2_core.get_send_window_size(server, 1)
  assert window == 100
}

pub fn get_send_window_size_returns_connection_window_when_smaller_test() {
  let #(server, _client) = server_with_open_stream()
  // Set connection window smaller than stream window
  let server = h2_core.Connection(..server, send_window_size: 50)
  let assert Ok(window) = h2_core.get_send_window_size(server, 1)
  assert window == 50
}

pub fn get_send_window_size_default_values_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(window) = h2_core.get_send_window_size(server, 1)
  assert window == 65_535
}

pub fn get_send_window_size_unknown_stream_is_error_test() {
  let server = new_connection(Server)
  let assert Error(_) = h2_core.get_send_window_size(server, 99)
}

pub fn get_send_window_size_negative_window_returns_zero_test() {
  let #(server, _client) = server_with_open_stream()
  // Simulate a negative send window (from SETTINGS reduction after data sent)
  let server =
    h2_core.Connection(
      ..server,
      streams: dict.insert(
        server.streams,
        1,
        Stream(state: Open, send_window_size: -1000, recv_window_size: 65_535),
      ),
    )
  let assert Ok(window) = h2_core.get_send_window_size(server, 1)
  assert window == 0
}

// =============================================================================
// Flow control - DATA counts toward connection window even on errored streams
// =============================================================================

// RFC 9113 Section 6.9 - "A receiver that receives a flow-controlled frame
// MUST always account for its contribution against the connection
// flow-control window, unless the receiver treats this as a connection error
// (Section 5.4.1). This is necessary even if the frame is in error."
// RFC 9113 Section 6.9 - "A receiver that receives a flow-controlled frame
// MUST always account for its contribution against the connection
// flow-control window, unless the receiver treats this as a connection error
// (Section 5.4.1). This is necessary even if the frame is in error."
//
// RFC 9113 Section 5.1 (closed state) - "the content of DATA frames counts
// toward the connection flow-control window."
pub fn receive_data_on_closed_stream_still_counts_toward_connection_window_test() {
  let #(server, _client) = server_with_open_stream()
  // Close stream 1 by receiving END_STREAM
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: True,
      data: <<>>,
      padding: None,
    )
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, data_frame)

  // Now receive DATA on the closed stream — per RFC 5.1 closed state,
  // the endpoint MUST minimally process and discard. The DATA still
  // counts toward the connection flow-control window.
  let assert Ok(more_data) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"stale":utf8>>,
      padding: None,
    )
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, more_data)
  assert server.recv_window_size == 65_535 - 5
}

// =============================================================================
// Multiple DATA frames
// =============================================================================

pub fn receive_multiple_data_frames_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(frame1) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"part1":utf8>>,
      padding: None,
    )
  let assert Ok(frame2) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: True,
      data: <<"part2":utf8>>,
      padding: None,
    )
  let assert Ok(#(server, events, _to_send)) =
    receive_data(server, <<frame1:bits, frame2:bits>>)
  assert events == [
    DataReceived(stream_id: 1, data: <<"part1":utf8>>, end_stream: False),
    DataReceived(stream_id: 1, data: <<"part2":utf8>>, end_stream: True),
  ]
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == h2_core.HalfClosedRemote
}

// =============================================================================
// Padding
// =============================================================================

// RFC 9113 Section 6.1 - "DATA frames MAY also contain padding."
// Padded DATA frame should be received correctly, with only the actual
// data (not padding) in the DataReceived event.
pub fn receive_padded_data_frame_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"hello":utf8>>,
      padding: Some(10),
    )
  let assert Ok(#(_server, events, _to_send)) =
    receive_data(server, data_frame)
  assert events == [DataReceived(stream_id: 1, data: <<"hello":utf8>>, end_stream: False)]
}

// RFC 9113 Section 6.1 - "The entire DATA frame payload is included in flow
// control, including the Pad Length and Padding fields if present."
pub fn receive_padded_data_counts_full_payload_for_flow_control_test() {
  let #(server, _client) = server_with_open_stream()
  // 5 bytes data + 1 byte pad_length + 10 bytes padding = 16 bytes total payload
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"hello":utf8>>,
      padding: Some(10),
    )
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, data_frame)
  let assert Ok(stream) = dict.get(server.streams, 1)
  // Flow control counts the entire payload: pad_length(1) + data(5) + padding(10) = 16
  assert stream.recv_window_size == 65_535 - 16
  assert server.recv_window_size == 65_535 - 16
}

// =============================================================================
// Additional stream state tests
// =============================================================================

// RFC 9113 Section 6.1 - "DATA frames are subject to flow control and can
// only be sent when a stream is in the 'open' or 'half-closed (remote)' state."
// Verify that sending on half-closed (remote) succeeds.
pub fn send_data_on_half_closed_remote_succeeds_test() {
  let #(server, _client) = server_with_half_closed_remote_stream()
  // Stream 1 is half-closed (remote) — server can still send
  let assert Ok(#(server, _events, _to_send)) =
    send_headers(server, [Header(":status", "200", WithIndexing)], False)
  let assert Ok(#(_server, _events, _to_send)) =
    h2_core.send_data(server, 1, <<"response":utf8>>, False)
}

// RFC 9113 Section 6.1 - DATA on a closed stream.
// Per Section 5.1 closed state rules, endpoint must minimally process
// and discard. Not a connection error.
pub fn receive_data_on_closed_stream_is_handled_gracefully_test() {
  let #(server, _client) = server_with_open_stream()
  // Close stream 1: receive END_STREAM from client
  let assert Ok(end_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: True,
      data: <<>>,
      padding: None,
    )
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, end_frame)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == HalfClosedRemote

  // Send RST_STREAM to fully close
  let assert Ok(#(server, _events, _to_send)) =
    h2_core.send_rst_stream(server, 1, h2_frame.NoError)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == h2_core.Closed

  // Receive DATA on the now-closed stream — should not be a connection error
  let assert Ok(stale_data) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"late":utf8>>,
      padding: None,
    )
  let assert Ok(#(_server, _events, _to_send)) =
    receive_data(server, stale_data)
}

// RFC 9113 Section 6.1 - "The total number of padding octets is determined
// by the value of the Pad Length field. If the length of the padding is the
// length of the frame payload or greater, the recipient MUST treat this as a
// connection error (Section 5.4.1) of type PROTOCOL_ERROR."
pub fn receive_data_invalid_padding_length_is_protocol_error_test() {
  let #(server, _client) = server_with_open_stream()
  // Manually craft a DATA frame where pad_length >= payload length
  // Length=1, Type=0x00, Flags=0x08 (PADDED), Stream ID=1
  // Pad Length=1 — but remaining payload after pad_length field is 0 bytes,
  // so padding (1) >= remaining payload (0)
  let bad_padded = <<
    1:size(24),
    0x00:size(8),
    0x08:size(8),
    0:size(1),
    1:size(31),
    1:size(8),
  >>
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, bad_padded)
}
