import gleam/option.{None, Some}
import h2_core.{
  type Connection, Client, ConnectionError, DataReceived, FlowControlError,
  FrameSizeError, Header, HeadersReceived, NoError, ProtocolError, Server,
  StreamClosed, StreamError, StreamReset, WithIndexing,
  get_connection_recv_window_size, get_connection_send_window_size, open_stream,
  receive_data, send_headers,
}
import h2_core/internal/stream.{Closed, HalfClosedLocal, HalfClosedRemote}
import h2_frame
import helper

fn server_with_open_stream() -> #(Connection, Connection) {
  helper.server_with_open_stream()
}

fn server_with_half_closed_remote_stream() -> #(Connection, Connection) {
  helper.server_with_half_closed_remote_stream()
}

/// Adjust stream 1's send_window_size to `target` by having the peer
/// send SETTINGS [InitialWindowSize(target)].
/// Only works for non-negative targets (0..2^31-1).
fn set_stream_send_window(server: Connection, target: Int) -> Connection {
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.InitialWindowSize(target),
    ])
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, settings_frame)
  server
}

/// Adjust stream 1's recv_window_size to `target` by sending local
/// SETTINGS [InitialWindowSize(target)] and receiving the ACK.
fn set_stream_recv_window(server: Connection, target: Int) -> Connection {
  let assert Ok(#(server, _to_send)) =
    h2_core.send_settings(server, [h2_core.InitialWindowSize(target)])
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, settings_ack)
  server
}

/// Send `amount` bytes of zero-filled data from the server on stream 1,
/// in chunks respecting the default max frame size (16384).
/// Both connection and stream send windows are decremented by `amount`.
fn consume_send_window(server: Connection, amount: Int) -> Connection {
  case amount {
    0 -> server
    n if n <= 16_384 -> {
      let data = <<0:size(n)-unit(8)>>
      let assert Ok(#(server, _to_send)) =
        h2_core.send_data(server, 1, data, False, None)
      server
    }
    n -> {
      let data = <<0:size(16_384)-unit(8)>>
      let assert Ok(#(server, _to_send)) =
        h2_core.send_data(server, 1, data, False, None)
      consume_send_window(server, n - 16_384)
    }
  }
}

/// Consume `amount` bytes from the server's connection recv window by
/// having the peer send DATA frames on stream 1, in chunks respecting
/// the default max frame size (16384).
/// Both connection and stream recv windows are decremented by `amount`.
fn consume_recv_window(server: Connection, amount: Int) -> Connection {
  case amount {
    0 -> server
    n if n <= 16_384 -> {
      let assert Ok(data_frame) =
        h2_frame.encode_data(
          stream_id: 1,
          end_stream: False,
          data: <<0:size(n)-unit(8)>>,
          padding: None,
        )
      let assert Ok(#(server, _events, _to_send)) =
        receive_data(server, data_frame)
      server
    }
    n -> {
      let assert Ok(data_frame) =
        h2_frame.encode_data(
          stream_id: 1,
          end_stream: False,
          data: <<0:size(16_384)-unit(8)>>,
          padding: None,
        )
      let assert Ok(#(server, _events, _to_send)) =
        receive_data(server, data_frame)
      consume_recv_window(server, n - 16_384)
    }
  }
}

/// Restore stream 1's send_window_size by having the peer send a
/// WINDOW_UPDATE frame on stream 1.
fn restore_stream_send_window(server: Connection, increment: Int) -> Connection {
  let assert Ok(wu) =
    h2_frame.encode_window_update(
      stream_id: 1,
      window_size_increment: increment,
    )
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, wu)
  server
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
  let assert Error(ConnectionError(ProtocolError)) =
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
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, data_frame)
  assert events
    == [
      DataReceived(
        stream_id: 1,
        data: <<"hello":utf8>>,
        end_stream: False,
        flow_controlled_length: 5,
      ),
    ]
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
  let assert Ok(#(server, events, _to_send)) = receive_data(server, data_frame)
  assert events
    == [
      DataReceived(
        stream_id: 1,
        data: <<"goodbye":utf8>>,
        end_stream: True,
        flow_controlled_length: 7,
      ),
    ]
  let assert Ok(HalfClosedRemote) = h2_core.get_stream_state(server, 1)
}

// RFC 9113 Section 6.1 - DATA with END_STREAM on a half-closed (local)
// stream transitions to closed.
pub fn receive_data_with_end_stream_on_half_closed_local_closes_stream_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  // Client opens stream 1
  let assert Ok(#(client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  // Server sends headers with END_STREAM, making it half-closed (local)
  let assert Ok(#(server, response_headers)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      True,
    )
  let assert Ok(#(_client, _events, _to_send)) =
    receive_data(client, response_headers)
  let assert Ok(HalfClosedLocal) = h2_core.get_stream_state(server, 1)

  // Client sends DATA with END_STREAM
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: True,
      data: <<"done":utf8>>,
      padding: None,
    )
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, data_frame)
  let assert Ok(Closed) = h2_core.get_stream_state(server, 1)
}

// RFC 9113 Section 5.1: A stream in "half-closed (local)" can still receive
// DATA from the remote peer. Receiving DATA without END_STREAM must keep the
// stream in HalfClosedLocal.
pub fn receive_data_without_end_stream_on_half_closed_local_stays_half_closed_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  // Client opens stream 1
  let assert Ok(#(client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  // Server sends headers with END_STREAM, making it half-closed (local)
  let assert Ok(#(server, response_headers)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      True,
    )
  let assert Ok(#(_client, _events, _to_send)) =
    receive_data(client, response_headers)
  let assert Ok(HalfClosedLocal) = h2_core.get_stream_state(server, 1)

  // Client sends DATA without END_STREAM
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"partial":utf8>>,
      padding: None,
    )
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, data_frame)
  let assert Ok(HalfClosedLocal) = h2_core.get_stream_state(server, 1)
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
  // RFC 9113 Section 6.9 - The connection window must still be accounted for,
  // and since the data is discarded, a WINDOW_UPDATE must be sent to reclaim it.
  let assert Ok(#(_server, events, to_send)) = receive_data(server, data_frame)
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.StreamClosed)
  let assert Ok(expected_wu) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 7)
  assert to_send == <<expected_rst:bits, expected_wu:bits>>
  assert events == [StreamReset(stream_id: 1, error_code: StreamClosed)]
}

// RFC 9113 Section 5.1 - "Receiving any frame other than HEADERS or PRIORITY
// on a stream in this [idle] state MUST be treated as a connection error
// (Section 5.4.1) of type PROTOCOL_ERROR."
pub fn receive_data_on_idle_stream_is_protocol_error_test() {
  let server = helper.connected_connection(Server)
  // Stream 1 was never opened
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"bad":utf8>>,
      padding: None,
    )
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(server, data_frame)
}

// RFC 9113 Section 5.1 (reserved local) - "Receiving any type of frame other
// than RST_STREAM, PRIORITY, or WINDOW_UPDATE on a stream in this state MUST
// be treated as a connection error (Section 5.4.1) of type PROTOCOL_ERROR."
pub fn receive_data_on_reserved_local_is_protocol_error_test() {
  let #(server, _client, promised_id) =
    helper.server_with_reserved_local_stream()
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: promised_id,
      end_stream: False,
      data: <<"bad":utf8>>,
      padding: None,
    )
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(server, data_frame)
}

// RFC 9113 Section 5.1 (reserved remote) - "Receiving any type of frame other
// than HEADERS, RST_STREAM, or PRIORITY on a stream in this state MUST be
// treated as a connection error (Section 5.4.1) of type PROTOCOL_ERROR."
pub fn receive_data_on_reserved_remote_is_protocol_error_test() {
  let #(_server, client, promised_id) =
    helper.client_with_reserved_remote_stream()
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: promised_id,
      end_stream: False,
      data: <<"bad":utf8>>,
      padding: None,
    )
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(client, data_frame)
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
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, data_frame)
  assert events
    == [
      DataReceived(
        stream_id: 1,
        data: <<>>,
        end_stream: False,
        flow_controlled_length: 0,
      ),
    ]
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
  let assert Ok(#(server, events, _to_send)) = receive_data(server, data_frame)
  assert events
    == [
      DataReceived(
        stream_id: 1,
        data: <<>>,
        end_stream: True,
        flow_controlled_length: 0,
      ),
    ]
  let assert Ok(HalfClosedRemote) = h2_core.get_stream_state(server, 1)
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
// Receiving DATA must decrement the stream recv_window_size.
pub fn receive_data_decrements_stream_recv_window_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"hello world":utf8>>,
      padding: None,
    )
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, data_frame)
  let assert Ok(65_524) = h2_core.get_stream_recv_window_size(server, 1)
}

// RFC 9113 Section 5.2.1 - "A sender that sends a FLOW_CONTROLLED frame
// reduces the available space in both flow-control windows."
//
// Receiving DATA must decrement the connection recv_window_size.
pub fn receive_data_decrements_connection_recv_window_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"hello world":utf8>>,
      padding: None,
    )
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, data_frame)
  assert get_connection_recv_window_size(server) == 65_535 - 11
}

// RFC 9113 Section 6.9 - "A receiver that receives a flow-controlled frame
// MUST always account for its contribution against the connection
// flow-control window, unless the receiver treats this as a connection error
// (Section 5.4.1). This is necessary even if the frame is in error."
pub fn receive_data_exceeding_stream_window_is_flow_control_error_test() {
  let #(server, _client) = server_with_open_stream()
  // Set stream recv_window_size to 5 bytes via local SETTINGS change
  let server = set_stream_recv_window(server, 5)
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"too much data":utf8>>,
      padding: None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal. The endpoint
  // sends RST_STREAM and continues processing.
  // RFC 9113 Section 6.9 - The connection window must still be accounted for,
  // and since the data is discarded, a WINDOW_UPDATE must be sent to reclaim it.
  let assert Ok(#(_server, events, to_send)) = receive_data(server, data_frame)
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(
      stream_id: 1,
      error_code: h2_frame.FlowControlError,
    )
  let assert Ok(expected_wu) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 13)
  assert to_send == <<expected_rst:bits, expected_wu:bits>>
  assert events == [StreamReset(stream_id: 1, error_code: FlowControlError)]
}

// RFC 9113 Section 6.9 - "A change to SETTINGS_INITIAL_WINDOW_SIZE can cause
// the available space in a flow-control window to become negative... A sender
// MUST NOT allow a flow-control window to exceed 2^31-1 octets."
// Section 5.2.1 - "A sender MUST NOT send flow-controlled frames beyond the
// limits set by its peer."
//
// Receiving DATA that exceeds the connection flow-control window is a
// connection error of type FLOW_CONTROL_ERROR.
pub fn receive_data_exceeding_connection_window_is_flow_control_error_test() {
  let #(server, _client) = server_with_open_stream()
  // Consume 65530 bytes of the connection recv window by receiving DATA,
  // then restore the stream recv window so only the connection window is small.
  let server = consume_recv_window(server, 65_530)
  let assert Ok(#(server, _to_send)) =
    h2_core.send_window_update(server, 1, 65_530)
  // Now: connection recv_window = 5, stream recv_window = 65535
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"too much data":utf8>>,
      padding: None,
    )
  let assert Error(ConnectionError(FlowControlError)) =
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
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(#(server, to_send)) =
    h2_core.send_data(server, 1, <<"hello":utf8>>, False, None)
  let assert Ok(expected) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"hello":utf8>>,
      padding: None,
    )
  assert to_send == expected
  // send_window_size should be decremented
  let assert Ok(65_530) = h2_core.get_stream_send_window_size(server, 1)
}

pub fn send_data_with_end_stream_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(#(server, _to_send)) =
    h2_core.send_data(server, 1, <<"done":utf8>>, True, None)
  let assert Ok(HalfClosedLocal) = h2_core.get_stream_state(server, 1)
}

// send_data exceeding the send window should return an error
pub fn send_data_exceeding_stream_window_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  // Set stream send_window_size to 5 via peer SETTINGS
  let server = set_stream_send_window(server, 5)
  let assert Error(ConnectionError(FlowControlError)) =
    h2_core.send_data(server, 1, <<"too much data":utf8>>, False, None)
}

pub fn send_data_exceeding_connection_window_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  // Consume connection send window down to 5 by sending data,
  // then restore stream window via peer WINDOW_UPDATE
  let server = consume_send_window(server, 65_530)
  let server = restore_stream_send_window(server, 65_530)
  // Now: connection send_window = 5, stream send_window = 65535
  let assert Error(ConnectionError(FlowControlError)) =
    h2_core.send_data(server, 1, <<"too much data":utf8>>, False, None)
}

// send_data on stream 0 should error
pub fn send_data_on_stream_zero_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Error(ConnectionError(ProtocolError)) =
    h2_core.send_data(server, 0, <<"bad":utf8>>, False, None)
}

// RFC 9113 Section 6.1 - DATA can only be sent on open or half-closed (remote).
// Sending on a stream that was never opened (idle state) must be an error.
pub fn send_data_on_idle_stream_is_error_test() {
  let server = helper.connected_connection(Server)
  // Stream 99 was never opened — it is in idle state
  let assert Error(StreamError(99, StreamClosed)) =
    h2_core.send_data(server, 99, <<"bad":utf8>>, False, None)
}

// send_data on a stream that is not open or half-closed (remote) should error
pub fn send_data_on_half_closed_local_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  // Server sends headers with END_STREAM
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      True,
    )
  let assert Ok(HalfClosedLocal) = h2_core.get_stream_state(server, 1)
  let assert Error(StreamError(1, StreamClosed)) =
    h2_core.send_data(server, 1, <<"bad":utf8>>, False, None)
}

// RFC 9113 Section 5.1 (closed) - "An endpoint MUST NOT send frames other
// than PRIORITY on a closed stream."
pub fn send_data_on_closed_stream_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    h2_core.send_rst_stream(server, 1, NoError)
  let assert Error(StreamError(1, StreamClosed)) =
    h2_core.send_data(server, 1, <<"bad":utf8>>, False, None)
}

// RFC 9113 Section 5.1 (reserved local) - "An endpoint MUST NOT send any
// type of frame other than HEADERS, RST_STREAM, or PRIORITY in this state."
pub fn send_data_on_reserved_local_stream_is_error_test() {
  let #(server, _client, promised_id) =
    helper.server_with_reserved_local_stream()
  let assert Error(ConnectionError(ProtocolError)) =
    h2_core.send_data(server, promised_id, <<"bad":utf8>>, False, None)
}

// RFC 9113 Section 5.1 (reserved remote) - "An endpoint MUST NOT send any
// type of frame other than RST_STREAM, WINDOW_UPDATE, or PRIORITY in this
// state."
pub fn send_data_on_reserved_remote_stream_is_error_test() {
  let #(_server, client, promised_id) =
    helper.client_with_reserved_remote_stream()
  let assert Error(ConnectionError(ProtocolError)) =
    h2_core.send_data(client, promised_id, <<"bad":utf8>>, False, None)
}

// RFC 9113 Section 5.2.1 - "A sender that sends a FLOW_CONTROLLED frame
// reduces the available space in both flow-control windows."
// send_data should decrement the stream send_window_size.
pub fn send_data_decrements_stream_send_window_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(#(server, _to_send)) =
    h2_core.send_data(server, 1, <<"hello world":utf8>>, False, None)
  let assert Ok(65_524) = h2_core.get_stream_send_window_size(server, 1)
}

// RFC 9113 Section 5.2.1 - "A sender that sends a FLOW_CONTROLLED frame
// reduces the available space in both flow-control windows."
// send_data should decrement the connection send_window_size.
pub fn send_data_decrements_connection_send_window_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(#(server, _to_send)) =
    h2_core.send_data(server, 1, <<"hello world":utf8>>, False, None)
  assert get_connection_send_window_size(server) == 65_535 - 11
}

// send_data with empty data should be valid
pub fn send_data_empty_payload_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(#(_server, to_send)) =
    h2_core.send_data(server, 1, <<>>, False, None)
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
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(#(server, _to_send)) =
    h2_core.send_data(server, 1, <<>>, True, None)
  let assert Ok(HalfClosedLocal) = h2_core.get_stream_state(server, 1)
}

// RFC 9113 Section 5.1 - END_STREAM on a half-closed (remote) stream causes
// the stream to transition to "closed".
pub fn send_data_with_end_stream_on_half_closed_remote_closes_stream_test() {
  let #(server, _client) = server_with_half_closed_remote_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(#(server, _to_send)) =
    h2_core.send_data(server, 1, <<"done":utf8>>, True, None)
  let assert Ok(Closed) = h2_core.get_stream_state(server, 1)
}

// RFC 9113 Section 6.9.2 - SETTINGS_INITIAL_WINDOW_SIZE changes can cause the
// flow-control window to go negative. "A sender MUST NOT allow a flow-control
// window to exceed 2^31-1 octets." A negative window means no DATA can be sent
// until WINDOW_UPDATE frames restore it.
pub fn send_data_with_negative_window_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  // Create a negative stream window: send 100 bytes to consume window,
  // then peer reduces INITIAL_WINDOW_SIZE to 0, pushing the window negative.
  // stream_sw = 65535 - 100 = 65435, then delta = 0 - 65535 = -65535
  // final stream_sw = 65435 - 65535 = -100
  let server = consume_send_window(server, 100)
  let server = set_stream_send_window(server, 0)
  let assert Ok(-100) = h2_core.get_stream_send_window_size(server, 1)
  let assert Error(ConnectionError(FlowControlError)) =
    h2_core.send_data(server, 1, <<"hello":utf8>>, False, None)
}

// =============================================================================
// Sending DATA with padding
// =============================================================================

// RFC 9113 Section 6.1 - "DATA frames MAY also contain padding."
// A padded DATA frame should be correctly encoded with the padding included.
pub fn send_padded_data_frame_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(#(_server, to_send)) =
    h2_core.send_data(server, 1, <<"hello":utf8>>, False, Some(10))
  // Manually crafted padded DATA frame:
  // Length=16 (1 pad_length + 5 data + 10 padding), Type=0x00,
  // Flags=0x08 (PADDED), Stream ID=1, Pad Length=10, then data, then 10 zero bytes
  let expected = <<
    16:size(24),
    0x00:size(8),
    0x08:size(8),
    0:size(1),
    1:size(31),
    10:size(8),
    "hello":utf8,
    0:size(80),
  >>
  assert to_send == expected
}

// RFC 9113 Section 6.1 - "The entire DATA frame payload is included in flow
// control, including the Pad Length and Padding fields if present."
// When sending a padded DATA frame, the stream flow-control window MUST be
// decremented by the entire payload: pad_length(1) + data + padding.
pub fn send_padded_data_decrements_stream_send_window_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  // 5 bytes data + 1 byte pad_length + 10 bytes padding = 16 bytes total payload
  let assert Ok(#(server, _to_send)) =
    h2_core.send_data(server, 1, <<"hello":utf8>>, False, Some(10))
  let assert Ok(65_519) = h2_core.get_stream_send_window_size(server, 1)
}

// RFC 9113 Section 6.1 - "The entire DATA frame payload is included in flow
// control, including the Pad Length and Padding fields if present."
// When sending a padded DATA frame, the connection flow-control window MUST be
// decremented by the entire payload: pad_length(1) + data + padding.
pub fn send_padded_data_decrements_connection_send_window_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  // 5 bytes data + 1 byte pad_length + 10 bytes padding = 16 bytes total payload
  let assert Ok(#(server, _to_send)) =
    h2_core.send_data(server, 1, <<"hello":utf8>>, False, Some(10))
  assert get_connection_send_window_size(server) == 65_535 - 16
}

// RFC 9113 Section 4.2 - "An endpoint MUST send an error code of
// FRAME_SIZE_ERROR if a frame exceeds the size defined in
// SETTINGS_MAX_FRAME_SIZE"
// Data alone exceeding the max frame size (default 16384) must be rejected.
pub fn send_data_exceeding_max_frame_size_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  // Default max frame size is 16384. Send 16385 bytes — one over the limit.
  let big_data = <<0:size(16_385)-unit(8)>>
  let assert Error(ConnectionError(FrameSizeError)) =
    h2_core.send_data(server, 1, big_data, False, None)
}

// RFC 9113 Section 4.2 - "An endpoint MUST send an error code of
// FRAME_SIZE_ERROR if a frame exceeds the size defined in
// SETTINGS_MAX_FRAME_SIZE"
// Section 6.1 - The entire DATA frame payload (including Pad Length and
// Padding fields) counts toward the frame size limit.
// When data + padding + pad_length field exceeds SETTINGS_MAX_FRAME_SIZE,
// send_data MUST fail rather than produce an oversized frame.
pub fn send_padded_data_exceeding_max_frame_size_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  // Default max frame size is 16384.
  // Send 16380 bytes of data with 10 bytes of padding:
  // payload = 1 (pad_length) + 16380 (data) + 10 (padding) = 16391 > 16384
  let big_data = <<0:size(16_380)-unit(8)>>
  let assert Error(ConnectionError(FrameSizeError)) =
    h2_core.send_data(server, 1, big_data, False, Some(10))
}

// RFC 9113 Section 6.1 - "The entire DATA frame payload is included in flow
// control, including the Pad Length and Padding fields if present."
// When sending padded DATA that would exceed the stream flow-control window
// (counting the full payload including padding), it MUST be rejected.
pub fn send_padded_data_exceeding_stream_window_is_flow_control_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  // Set stream send_window_size to 10 bytes via peer SETTINGS
  let server = set_stream_send_window(server, 10)
  // 3 bytes data fits in the window alone, but with padding:
  // payload = 1 (pad_length) + 3 (data) + 10 (padding) = 14 > 10
  let assert Error(ConnectionError(FlowControlError)) =
    h2_core.send_data(server, 1, <<"hey":utf8>>, False, Some(10))
}

// RFC 9113 Section 6.1 - "The entire DATA frame payload is included in flow
// control, including the Pad Length and Padding fields if present."
// The Pad Length field itself (1 byte) counts toward flow control. This test
// ensures that data + padding fits in the window but the extra pad_length
// byte pushes it over.
pub fn send_padded_data_pad_length_field_counts_toward_flow_control_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  // Set stream send_window_size to exactly 13 bytes via peer SETTINGS
  let server = set_stream_send_window(server, 13)
  // 3 bytes data + 10 bytes padding = 13, which fits the window.
  // But the full payload is 1 (pad_length) + 3 (data) + 10 (padding) = 14 > 13.
  let assert Error(ConnectionError(FlowControlError)) =
    h2_core.send_data(server, 1, <<"hey":utf8>>, False, Some(10))
}

// RFC 9113 Section 6.1 - "The entire DATA frame payload is included in flow
// control, including the Pad Length and Padding fields if present."
// When sending padded DATA that would exceed the connection flow-control window
// (counting the full payload including padding), it MUST be rejected.
pub fn send_padded_data_exceeding_connection_window_is_flow_control_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  // Consume connection send window down to 10 by sending data,
  // then restore stream window via peer WINDOW_UPDATE
  let server = consume_send_window(server, 65_525)
  let server = restore_stream_send_window(server, 65_525)
  // Now: connection send_window = 10, stream send_window = 65535
  // 3 bytes data fits in the window alone, but with padding:
  // payload = 1 (pad_length) + 3 (data) + 10 (padding) = 14 > 10
  let assert Error(ConnectionError(FlowControlError)) =
    h2_core.send_data(server, 1, <<"hey":utf8>>, False, Some(10))
}

// RFC 9113 Section 5.2.1 - "A sender MUST NOT allow a flow-control window
// to exceed 2^31-1 octets."
// Sending data exactly equal to the window size should succeed.
pub fn send_data_exactly_at_window_boundary_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  // Consume 65530 bytes to bring both windows down to 5
  let server = consume_send_window(server, 65_530)
  let assert Ok(5) = h2_core.get_stream_send_window_size(server, 1)
  assert get_connection_send_window_size(server) == 5
  let assert Ok(#(server, _to_send)) =
    h2_core.send_data(server, 1, <<"hello":utf8>>, False, None)
  let assert Ok(0) = h2_core.get_stream_send_window_size(server, 1)
  assert get_connection_send_window_size(server) == 0
}

// RFC 9113 Section 6.9.1 - "Frames with zero length with the END_STREAM
// flag set (that is, an empty DATA frame) MAY be sent if there is no
// available space in either flow-control window."
//
// When the flow-control window is exhausted, an empty DATA frame with
// END_STREAM should still be allowed to close the stream.
pub fn send_empty_data_with_end_stream_when_window_exhausted_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  // Consume all 65535 bytes to bring both windows down to 0
  let server = consume_send_window(server, 65_535)
  let assert Ok(0) = h2_core.get_stream_send_window_size(server, 1)
  assert get_connection_send_window_size(server) == 0
  let assert Ok(#(server, _to_send)) =
    h2_core.send_data(server, 1, <<>>, True, None)
  let assert Ok(HalfClosedLocal) = h2_core.get_stream_state(server, 1)
}

// RFC 9113 Section 5.2 - Flow control is based on both stream and connection
// windows. When the connection window is smaller than the stream window,
// sending data that fits the stream window but exceeds the connection window
// MUST be rejected.
pub fn send_data_connection_window_smaller_than_stream_window_is_error_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  // Consume connection send window down to 5, then restore stream window
  let server = consume_send_window(server, 65_530)
  let server = restore_stream_send_window(server, 65_530)
  // Now: connection send_window = 5, stream send_window = 65535
  // 13 bytes > 5 byte connection window
  let assert Error(ConnectionError(FlowControlError)) =
    h2_core.send_data(server, 1, <<"too much data":utf8>>, False, None)
}

// =============================================================================
// get_send_window_size - public API for callers to check available window
// =============================================================================

// Returns the minimum of stream and connection send window
pub fn get_send_window_size_returns_stream_window_when_smaller_test() {
  let #(server, _client) = server_with_open_stream()
  // Set stream window smaller than connection window via peer SETTINGS
  let server = set_stream_send_window(server, 100)
  let assert Ok(window) = h2_core.get_send_window_size(server, 1)
  assert window == 100
}

pub fn get_send_window_size_returns_connection_window_when_smaller_test() {
  let #(server, _client) = server_with_open_stream()
  // Consume connection send window down to 50, then restore stream window
  let server = consume_send_window(server, 65_485)
  let server = restore_stream_send_window(server, 65_485)
  // Now: connection send_window = 50, stream send_window = 65535
  let assert Ok(window) = h2_core.get_send_window_size(server, 1)
  assert window == 50
}

pub fn get_send_window_size_default_values_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(window) = h2_core.get_send_window_size(server, 1)
  assert window == 65_535
}

pub fn get_send_window_size_unknown_stream_is_error_test() {
  let server = helper.connected_connection(Server)
  let assert Error(Nil) = h2_core.get_send_window_size(server, 99)
}

pub fn get_send_window_size_negative_window_returns_zero_test() {
  let #(server, _client) = server_with_open_stream()
  // Create a negative send window: send 1000 bytes to consume window,
  // then peer reduces INITIAL_WINDOW_SIZE to 0, pushing the window negative.
  // stream_sw = 65535 - 1000 = 64535, then delta = 0 - 65535 = -65535
  // final stream_sw = 64535 - 65535 = -1000
  let server = consume_send_window(server, 1000)
  let server = set_stream_send_window(server, 0)
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
//
// When DATA exceeds the stream window (a stream error, not connection error),
// the connection flow-control window MUST still be decremented.
pub fn receive_data_exceeding_stream_window_still_decrements_connection_window_test() {
  let #(server, _client) = server_with_open_stream()
  // Set stream recv_window_size to 5 bytes via local SETTINGS change,
  // leaving connection window at default
  let server = set_stream_recv_window(server, 5)
  let data = <<"too much data":utf8>>
  let data_size = 13
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: data,
      padding: None,
    )
  // Stream error (not connection error) — receive_data returns Ok with RST_STREAM
  let assert Ok(#(server, _events, to_send)) = receive_data(server, data_frame)
  // Connection window MUST still be decremented despite the stream error
  assert get_connection_recv_window_size(server) == 65_535 - data_size
  // Data is discarded, so auto-reclaim via WINDOW_UPDATE
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(
      stream_id: 1,
      error_code: h2_frame.FlowControlError,
    )
  let assert Ok(expected_wu) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 13)
  assert to_send == <<expected_rst:bits, expected_wu:bits>>
}

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
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, data_frame)

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
  let assert Ok(#(server, _events, to_send)) = receive_data(server, more_data)
  // Connection window is decremented then reclaimed via auto WINDOW_UPDATE
  assert get_connection_recv_window_size(server) == 65_535 - 5
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.StreamClosed)
  let assert Ok(expected_wu) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 5)
  assert to_send == <<expected_rst:bits, expected_wu:bits>>
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
  assert events
    == [
      DataReceived(
        stream_id: 1,
        data: <<"part1":utf8>>,
        end_stream: False,
        flow_controlled_length: 5,
      ),
      DataReceived(
        stream_id: 1,
        data: <<"part2":utf8>>,
        end_stream: True,
        flow_controlled_length: 5,
      ),
    ]
  let assert Ok(HalfClosedRemote) = h2_core.get_stream_state(server, 1)
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
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, data_frame)
  assert events
    == [
      DataReceived(
        stream_id: 1,
        data: <<"hello":utf8>>,
        end_stream: False,
        flow_controlled_length: 16,
      ),
    ]
}

// RFC 9113 Section 6.1 - "The entire DATA frame payload is included in flow
// control, including the Pad Length and Padding fields if present."
// Stream recv window must be decremented by the full payload including padding.
pub fn receive_padded_data_decrements_stream_recv_window_test() {
  let #(server, _client) = server_with_open_stream()
  // 5 bytes data + 1 byte pad_length + 10 bytes padding = 16 bytes total payload
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"hello":utf8>>,
      padding: Some(10),
    )
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, data_frame)
  let assert Ok(65_519) = h2_core.get_stream_recv_window_size(server, 1)
}

// RFC 9113 Section 6.1 - "The entire DATA frame payload is included in flow
// control, including the Pad Length and Padding fields if present."
// Connection recv window must be decremented by the full payload including padding.
pub fn receive_padded_data_decrements_connection_recv_window_test() {
  let #(server, _client) = server_with_open_stream()
  // 5 bytes data + 1 byte pad_length + 10 bytes padding = 16 bytes total payload
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"hello":utf8>>,
      padding: Some(10),
    )
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, data_frame)
  assert get_connection_recv_window_size(server) == 65_535 - 16
}

// RFC 9113 Section 6.1 - "The entire DATA frame payload is included in flow
// control, including the Pad Length and Padding fields if present."
// The Pad Length field itself (1 byte) counts toward flow control. This test
// ensures that data + padding fits in the window but the extra pad_length
// byte pushes it over.
pub fn receive_padded_data_pad_length_field_counts_toward_flow_control_test() {
  let #(server, _client) = server_with_open_stream()
  // Set stream recv_window_size to exactly 13 bytes via local SETTINGS change
  let server = set_stream_recv_window(server, 13)
  // 3 bytes data + 10 bytes padding = 13, which fits the window.
  // But the full payload is 1 (pad_length) + 3 (data) + 10 (padding) = 14 > 13.
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"hey":utf8>>,
      padding: Some(10),
    )
  // Stream error (not connection error) — receive_data returns Ok with RST_STREAM
  let assert Ok(#(_server, events, to_send)) = receive_data(server, data_frame)
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(
      stream_id: 1,
      error_code: h2_frame.FlowControlError,
    )
  // Full payload is 14 bytes (1 + 3 + 10), auto-reclaim via WINDOW_UPDATE
  let assert Ok(expected_wu) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 14)
  assert to_send == <<expected_rst:bits, expected_wu:bits>>
  assert events == [StreamReset(stream_id: 1, error_code: FlowControlError)]
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
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(#(_server, _to_send)) =
    h2_core.send_data(server, 1, <<"response":utf8>>, False, None)
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
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, end_frame)
  let assert Ok(HalfClosedRemote) = h2_core.get_stream_state(server, 1)

  // Send RST_STREAM to fully close
  let assert Ok(#(server, _to_send)) =
    h2_core.send_rst_stream(server, 1, NoError)
  let assert Ok(Closed) = h2_core.get_stream_state(server, 1)

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

// RFC 9113 Section 6.1 - "A receiver is not obligated to verify padding but
// MAY treat non-zero padding as a connection error (Section 5.4.1) of type
// PROTOCOL_ERROR."
//
// By default, non-zero padding bytes MUST be silently accepted (the receiver
// is not obligated to verify them).
pub fn receive_data_nonzero_padding_bytes_accepted_by_default_test() {
  let #(server, _client) = server_with_open_stream()
  // Manually craft a DATA frame with PADDED flag, pad_length=3,
  // 5 bytes of data, then 3 non-zero padding bytes (all 0xFF).
  // Length=9 (1 pad_length + 5 data + 3 padding), Type=0x00, Flags=0x08 (PADDED), Stream ID=1
  let non_zero_padded = <<
    9:size(24),
    0x00:size(8),
    0x08:size(8),
    0:size(1),
    1:size(31),
    3:size(8),
    "hello":utf8,
    0xFF,
    0xFF,
    0xFF,
  >>
  let assert Ok(#(_server, events, _to_send)) =
    receive_data(server, non_zero_padded)
  assert events
    == [
      DataReceived(
        stream_id: 1,
        data: <<"hello":utf8>>,
        end_stream: False,
        flow_controlled_length: 9,
      ),
    ]
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
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(server, bad_padded)
}

// RFC 9113 Section 4.2 - "An endpoint MUST send an error code of
// FRAME_SIZE_ERROR if a frame exceeds the size defined in
// SETTINGS_MAX_FRAME_SIZE, exceeds any limit defined for the frame
// type, or is too small to contain mandatory frame data. A frame size
// error in a frame that could alter the state of the entire
// connection MUST be treated as a connection error (Section 5.4.1)."
//
// A DATA frame exceeding SETTINGS_MAX_FRAME_SIZE (default 16384) must
// be treated as a connection error of type FRAME_SIZE_ERROR.
pub fn receive_data_exceeding_max_frame_size_is_frame_size_error_test() {
  let #(server, _client) = server_with_open_stream()
  // Craft a DATA frame with payload of 16385 bytes — one above the default max
  // Length=16385, Type=0x00, Flags=0, Stream ID=1
  let oversized_payload = <<0:size(16_385)-unit(8)>>
  let oversized_frame = <<
    16_385:size(24),
    0x00:size(8),
    0:size(8),
    0:size(1),
    1:size(31),
    oversized_payload:bits,
  >>
  let assert Error(ConnectionError(FrameSizeError)) =
    receive_data(server, oversized_frame)
}

// =============================================================================
// Content-length validation
// =============================================================================

// RFC 9113 Section 8.1.1 - "A request or response is also malformed if
// the value of a content-length header field does not equal the sum of
// the DATA frame payload lengths that form the content, unless the
// message is defined as having no content."
//
// Receiving more DATA than declared in content-length is malformed.
pub fn receive_data_exceeding_content_length_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  // Client sends request with content-length: 5
  let assert Ok(#(_client, headers, _stream_id)) =
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
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  // Send 10 bytes of data — exceeds content-length of 5
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: True,
      data: <<"0123456789":utf8>>,
      padding: None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal.
  // Data is discarded so a WINDOW_UPDATE is also sent to reclaim the
  // connection flow-control window.
  let assert Ok(#(_server, events, to_send)) = receive_data(server, data_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  let assert Ok(expected_wu) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 10)
  assert to_send == <<expected_rst:bits, expected_wu:bits>>
}

// RFC 9113 Section 8.1.1 - Receiving less DATA than declared in
// content-length with END_STREAM is malformed.
pub fn receive_data_less_than_content_length_with_end_stream_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  // Client sends request with content-length: 10
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(
      client,
      [
        Header(":method", <<"POST":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
        Header("content-length", <<"10":utf8>>, WithIndexing),
      ],
      False,
    )
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  // Send only 5 bytes with END_STREAM — less than content-length of 10
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: True,
      data: <<"hello":utf8>>,
      padding: None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal.
  // Data is discarded so a WINDOW_UPDATE is also sent to reclaim the
  // connection flow-control window.
  let assert Ok(#(_server, events, to_send)) = receive_data(server, data_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  let assert Ok(expected_wu) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 5)
  assert to_send == <<expected_rst:bits, expected_wu:bits>>
}

// RFC 9113 Section 8.1.1 - Content-length matching exactly should succeed.
pub fn receive_data_matching_content_length_succeeds_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
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
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: True,
      data: <<"hello":utf8>>,
      padding: None,
    )
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, data_frame)
  let assert [DataReceived(stream_id: 1, data: <<"hello":utf8>>, ..)] = events
}

// RFC 9113 Section 8.1.1 - Multiple DATA frames totaling the correct
// content-length should succeed.
pub fn receive_multiple_data_matching_content_length_succeeds_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(
      client,
      [
        Header(":method", <<"POST":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
        Header("content-length", <<"10":utf8>>, WithIndexing),
      ],
      False,
    )
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  // First DATA: 5 bytes
  let assert Ok(frame1) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<"hello":utf8>>,
      padding: None,
    )
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, frame1)

  // Second DATA: 5 bytes with END_STREAM — total 10 matches content-length
  let assert Ok(frame2) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: True,
      data: <<"world":utf8>>,
      padding: None,
    )
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, frame2)
  let assert [DataReceived(stream_id: 1, data: <<"world":utf8>>, ..)] = events
}

// RFC 9113 Section 8.1.1 - Content-length of 0 with no DATA and
// END_STREAM on HEADERS should succeed.
pub fn receive_content_length_zero_no_data_succeeds_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(
      client,
      [
        Header(":method", <<"POST":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
        Header("content-length", <<"0":utf8>>, WithIndexing),
      ],
      True,
    )
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, headers)
  let assert [HeadersReceived(stream_id: 1, ..)] = events
}

// RFC 9113 Section 8.1.1 - A non-numeric content-length value is
// malformed.
pub fn receive_headers_invalid_content_length_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(
      client,
      [
        Header(":method", <<"POST":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
        Header("content-length", <<"abc":utf8>>, WithIndexing),
      ],
      False,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal.
  let assert Ok(#(_server, events, to_send)) = receive_data(server, headers)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}
