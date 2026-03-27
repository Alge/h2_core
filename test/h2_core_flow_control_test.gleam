import gleam/option.{None}
import h2_core.{
  Client, ConnectionError, FlowControlError, FrameSizeError, Header,
  HeadersReceived, ProtocolError, Server, StreamReset, WithIndexing,
  acknowledge_data, get_connection_recv_window_size,
  get_connection_send_window_size, get_stream_state, open_stream, receive_data,
}
import h2_core/internal/stream.{HalfClosedLocal, HalfClosedRemote, Open}
import h2_frame
import helper

// RFC 9113 Section 6.9.2 - Initial flow control window is 65,535
pub fn new_connection_default_window_sizes_test() {
  let conn = helper.connected_connection(Client)
  assert get_connection_send_window_size(conn) == 65_535
  assert get_connection_recv_window_size(conn) == 65_535
}

// RFC 9113 Section 6.9.2 - New streams start with the initial window
// size of 65,535.
pub fn new_stream_default_window_sizes_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  let assert Ok(65_535) = h2_core.get_stream_send_window_size(server, 1)
  let assert Ok(65_535) = h2_core.get_stream_recv_window_size(server, 1)
}

// =============================================================================
// acknowledge_data — sending WINDOW_UPDATE for both stream and connection
// =============================================================================

// RFC 9113 Section 6.9 - "Separate WINDOW_UPDATE frames are sent for the
// stream- and connection-level flow-control windows."
//
// acknowledge_data sends two WINDOW_UPDATE frames: one for the stream and
// one for the connection (stream 0), both with the same increment.
pub fn acknowledge_data_sends_two_window_update_frames_test() {
  let #(server, _client) = helper.server_with_open_stream()
  let assert Ok(#(_server, to_send)) = acknowledge_data(server, 1, 1000)
  let assert Ok(expected_conn) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 1000)
  let assert Ok(expected_stream) =
    h2_frame.encode_window_update(stream_id: 1, window_size_increment: 1000)
  assert to_send == <<expected_conn:bits, expected_stream:bits>>
}

// acknowledge_data updates both the stream recv_window_size and the
// connection recv_window_size by the given increment.
pub fn acknowledge_data_updates_both_recv_windows_test() {
  let #(server, _client) = helper.server_with_open_stream()
  assert get_connection_recv_window_size(server) == 65_535
  let assert Ok(65_535) = h2_core.get_stream_recv_window_size(server, 1)

  let assert Ok(#(server, _to_send)) = acknowledge_data(server, 1, 10_000)
  assert get_connection_recv_window_size(server) == 75_535
  let assert Ok(75_535) = h2_core.get_stream_recv_window_size(server, 1)
}

// RFC 9113 Section 6.9 - Multiple acknowledge_data calls accumulate on
// both the stream and connection recv_window_size.
pub fn acknowledge_data_accumulates_on_both_windows_test() {
  let #(server, _client) = helper.server_with_open_stream()
  let assert Ok(#(server, _to_send)) = acknowledge_data(server, 1, 1000)
  let assert Ok(#(server, _to_send)) = acknowledge_data(server, 1, 2000)
  assert get_connection_recv_window_size(server) == 68_535
  let assert Ok(68_535) = h2_core.get_stream_recv_window_size(server, 1)
}

// RFC 9113 Section 6.9 - Minimum valid increment (1)
pub fn acknowledge_data_minimum_increment_test() {
  let #(server, _client) = helper.server_with_open_stream()
  let assert Ok(#(server, to_send)) = acknowledge_data(server, 1, 1)
  assert get_connection_recv_window_size(server) == 65_536
  let assert Ok(65_536) = h2_core.get_stream_recv_window_size(server, 1)
  let assert Ok(expected_conn) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 1)
  let assert Ok(expected_stream) =
    h2_frame.encode_window_update(stream_id: 1, window_size_increment: 1)
  assert to_send == <<expected_conn:bits, expected_stream:bits>>
}

// RFC 9113 Section 6.9 - "The legal range for the increment to the
// flow-control window is 1 to 2^31-1 (2,147,483,647) octets."
// Maximum valid increment from default window:
// 2^31-1 (2_147_483_647) minus initial window (65_535) = 2_147_418_112
pub fn acknowledge_data_max_increment_test() {
  let #(server, _client) = helper.server_with_open_stream()
  let max_increment = 2_147_483_647 - 65_535
  let assert Ok(#(server, _to_send)) =
    acknowledge_data(server, 1, max_increment)
  assert get_connection_recv_window_size(server) == 2_147_483_647
  let assert Ok(2_147_483_647) = h2_core.get_stream_recv_window_size(server, 1)
}

// --- Rejection of stream_id=0 ---

// acknowledge_data only works on existing streams. stream_id=0 refers to
// the connection itself and is not a valid stream. It must be rejected.
pub fn acknowledge_data_rejects_stream_id_zero_test() {
  let conn = helper.connected_connection(Client)
  let assert Error(ConnectionError(ProtocolError)) =
    acknowledge_data(conn, 0, 1000)
}

// --- Stream existence validation ---

// acknowledge_data must verify the stream exists. Calling it with a
// stream_id that was never opened must be rejected.
pub fn acknowledge_data_rejects_nonexistent_stream_test() {
  let server = helper.connected_connection(Server)
  let assert Error(ConnectionError(ProtocolError)) =
    acknowledge_data(server, 99, 1000)
}

// --- Overflow protection ---

// RFC 9113 Section 6.9.1 - "A sender MUST NOT allow a flow-control window
// to exceed 2^31-1 octets."
// When the increment would cause the stream recv window to overflow,
// acknowledge_data must return a FLOW_CONTROL_ERROR.
pub fn acknowledge_data_stream_overflow_is_flow_control_error_test() {
  let #(server, _client) = helper.server_with_open_stream()
  // Default stream recv window is 65_535. An increment of 2^31-1 would overflow.
  let assert Error(ConnectionError(FlowControlError)) =
    acknowledge_data(server, 1, 2_147_483_647)
}

// RFC 9113 Section 6.9.1 - "A sender MUST NOT allow a flow-control window
// to exceed 2^31-1 octets."
// When the increment would cause the connection recv window to overflow
// (but the stream window is fine because it was consumed), acknowledge_data
// must return a FLOW_CONTROL_ERROR.
pub fn acknowledge_data_connection_overflow_is_flow_control_error_test() {
  let #(server, client) = helper.server_with_open_stream()
  // Both windows start at 65_535 and get the same increment, so in the
  // default case both would overflow symmetrically. We verify that even if
  // the stream window fits, the connection overflow is caught.
  // Use a second stream to create asymmetry:
  // 1. Open stream 3
  // 2. acknowledge_data on stream 1 to grow connection window near max
  // 3. Then acknowledge_data on stream 3 to tip connection over the edge
  let assert Ok(#(_client, headers3, _stream_id)) =
    open_stream(
      client,
      [
        Header(":method", <<"POST":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
      ],
      False,
    )
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers3)

  // Grow connection window close to max via stream 1
  let max_increment = 2_147_483_647 - 65_535
  let assert Ok(#(server, _to_send)) =
    acknowledge_data(server, 1, max_increment)
  // Connection window is now at 2_147_483_647, stream 1 at 2_147_483_647
  // Stream 3 is still at 65_535

  // Now try to acknowledge on stream 3 — stream 3 has room but connection
  // would overflow past 2^31-1
  let assert Error(ConnectionError(FlowControlError)) =
    acknowledge_data(server, 3, 1)
}

// --- Closed stream ---

// RFC 9113 Section 5.1 (closed): "An endpoint MUST NOT send frames other
// than PRIORITY on a closed stream."
// acknowledge_data on a closed stream must be rejected.
pub fn acknowledge_data_on_closed_stream_is_error_test() {
  let #(server, _client) = helper.server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    h2_core.send_rst_stream(server, 1, h2_core.NoError)
  let assert Error(ConnectionError(ProtocolError)) =
    acknowledge_data(server, 1, 1024)
}

// --- Stream state: half-closed (remote) ---

// RFC 9113 Section 5.1 (half-closed remote): The server has received
// END_STREAM from the client but may still need to acknowledge previously
// received data. acknowledge_data should work on half-closed (remote) streams.
pub fn acknowledge_data_on_half_closed_remote_stream_test() {
  let #(server, _client) = helper.server_with_half_closed_remote_stream()
  let assert Ok(HalfClosedRemote) = get_stream_state(server, 1)
  let assert Ok(#(server, _to_send)) = acknowledge_data(server, 1, 1000)
  assert get_connection_recv_window_size(server) == 66_535
  let assert Ok(66_535) = h2_core.get_stream_recv_window_size(server, 1)
}

// --- Stream state: open ---

// acknowledge_data on an open stream should succeed (typical case: server
// receives data from client and acknowledges it).
pub fn acknowledge_data_on_open_stream_test() {
  let #(server, _client) = helper.server_with_open_stream()
  let assert Ok(Open) = get_stream_state(server, 1)
  let assert Ok(#(server, _to_send)) = acknowledge_data(server, 1, 5000)
  assert get_connection_recv_window_size(server) == 70_535
  let assert Ok(70_535) = h2_core.get_stream_recv_window_size(server, 1)
}

// --- Stream state: half-closed (local) ---

// RFC 9113 Section 5.1 (half-closed local): The server sent END_STREAM but
// the client may still send DATA. acknowledge_data must work in this state.
pub fn acknowledge_data_on_half_closed_local_stream_test() {
  let #(server, _client) = helper.server_with_half_closed_local_stream()
  let assert Ok(HalfClosedLocal) = get_stream_state(server, 1)
  let assert Ok(#(server, _to_send)) = acknowledge_data(server, 1, 1000)
  assert get_connection_recv_window_size(server) == 66_535
  let assert Ok(66_535) = h2_core.get_stream_recv_window_size(server, 1)
}

// --- Zero and negative increments ---

// RFC 9113 Section 6.9 - "A receiver MUST treat the receipt of a
// WINDOW_UPDATE frame with an increment of 0 as a stream error of type
// PROTOCOL_ERROR." Sending a zero-increment WINDOW_UPDATE is therefore
// forbidden — the peer would reject it.
pub fn acknowledge_data_zero_increment_is_error_test() {
  let #(server, _client) = helper.server_with_open_stream()
  let assert Error(ConnectionError(ProtocolError)) =
    acknowledge_data(server, 1, 0)
}

// RFC 9113 Section 6.9 - "The legal range for the increment to the
// flow-control window is 1 to 2^31-1 (2,147,483,647) octets."
// Negative increments are outside this range and must be rejected.
pub fn acknowledge_data_negative_increment_is_error_test() {
  let #(server, _client) = helper.server_with_open_stream()
  let assert Error(ConnectionError(ProtocolError)) =
    acknowledge_data(server, 1, -1)
}

// RFC 9113 Section 6.9.2 - "A change to SETTINGS_INITIAL_WINDOW_SIZE can
// cause the available space in a flow-control window to become negative."
// When the recv window is negative, a result-overflow check alone is not
// sufficient: an increment above 2^31-1 can produce a sum below 2^31-1
// and slip through. The increment must be validated independently.
//
// Example: recv_window = -1000, increment = 2^31 (= 2_147_483_648, one
// above the RFC maximum). Sum = 2_147_482_648 — below the overflow limit,
// so a result-only check would pass. An explicit range check catches it.
pub fn acknowledge_data_oversized_increment_with_negative_window_is_error_test() {
  let #(server, _client) = helper.server_with_open_stream()
  // Receive 1000 bytes to move the stream recv window from 65535 to 64535.
  let assert Ok(data_frame) =
    h2_frame.encode_data(
      stream_id: 1,
      end_stream: False,
      data: <<0:size(1000)-unit(8)>>,
      padding: None,
    )
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, data_frame)
  // Reduce SETTINGS_INITIAL_WINDOW_SIZE to 0: delta = 0 - 65535 = -65535,
  // so stream recv_window = 64535 - 65535 = -1000.
  let assert Ok(#(server, _to_send)) =
    h2_core.send_settings(server, [h2_core.InitialWindowSize(0)])
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, settings_ack)
  // 2^31-1 + 1 = 2_147_483_648 exceeds the RFC maximum increment.
  let assert Error(ConnectionError(ProtocolError)) =
    acknowledge_data(server, 1, 2_147_483_648)
}

// --- Reserved streams ---

// RFC 9113 Section 5.1 (reserved local/remote): No DATA is exchanged on
// reserved streams, so acknowledging data on them makes no sense.
pub fn acknowledge_data_on_reserved_local_stream_is_error_test() {
  let #(server, _client, promised_id) =
    helper.server_with_reserved_local_stream()
  let assert Error(ConnectionError(ProtocolError)) =
    acknowledge_data(server, promised_id, 1000)
}

pub fn acknowledge_data_on_reserved_remote_stream_is_error_test() {
  let #(_server, client, promised_id) =
    helper.client_with_reserved_remote_stream()
  let assert Error(ConnectionError(ProtocolError)) =
    acknowledge_data(client, promised_id, 1000)
}

// =============================================================================
// Receiving WINDOW_UPDATE — RFC 9113 Section 6.9
// =============================================================================

// Connection-level WINDOW_UPDATE increases send_window_size
pub fn receive_window_update_connection_level_test() {
  let conn = helper.connected_connection(Client)
  assert get_connection_send_window_size(conn) == 65_535
  let assert Ok(wu) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 1000)
  let assert Ok(#(conn, events, to_send)) = receive_data(conn, wu)
  assert get_connection_send_window_size(conn) == 66_535
  assert events == []
  assert to_send == <<>>
}

// Multiple connection-level WINDOW_UPDATEs accumulate
pub fn receive_window_update_accumulates_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(wu) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 1000)
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, wu)
  let assert Ok(wu2) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 500)
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, wu2)
  assert get_connection_send_window_size(conn) == 67_035
}

// RFC 9113 Section 6.9 - Increment of 0 on stream 0 is connection error PROTOCOL_ERROR
pub fn receive_window_update_zero_increment_connection_test() {
  let conn = helper.connected_connection(Client)
  // Manually craft: Length=4, Type=0x08, Flags=0, Stream ID=0, Increment=0
  let bad_wu = <<
    4:size(24),
    0x08:size(8),
    0:size(8),
    0:size(1),
    0:size(31),
    0:size(1),
    0:size(31),
  >>
  let assert Error(ConnectionError(ProtocolError)) = receive_data(conn, bad_wu)
}

// RFC 9113 Section 6.9 - Increment of 0 on a stream is stream error PROTOCOL_ERROR
pub fn receive_window_update_zero_increment_stream_test() {
  let conn = helper.connected_connection(Client)
  // Manually craft: Length=4, Type=0x08, Flags=0, Stream ID=1, Increment=0
  let bad_wu = <<
    4:size(24),
    0x08:size(8),
    0:size(8),
    0:size(1),
    1:size(31),
    0:size(1),
    0:size(31),
  >>
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, bad_wu)
  let assert [StreamReset(stream_id: 1, error_code: ProtocolError)] = events
  let assert Ok(#(frame_data, _rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(h2_frame.RstStream(1, h2_frame.ProtocolError)) =
    h2_frame.decode_frame(frame_data)
}

// RFC 9113 Section 6.9 - Wrong frame size is connection error FRAME_SIZE_ERROR
pub fn receive_window_update_wrong_length_test() {
  let conn = helper.connected_connection(Client)
  // Manually craft a WINDOW_UPDATE with 3 bytes payload instead of 4
  // Length=3, Type=0x08, Flags=0, Stream ID=0
  let bad_wu = <<
    3:size(24),
    0x08:size(8),
    0:size(8),
    0:size(1),
    0:size(31),
    1,
    2,
    3,
  >>
  let assert Error(ConnectionError(FrameSizeError)) = receive_data(conn, bad_wu)
}

// RFC 9113 Section 6.9.1 - Flow control window MUST NOT exceed 2^31-1
pub fn receive_window_update_overflow_connection_test() {
  let conn = helper.connected_connection(Client)
  // Default is 65_535. Send an increment that would push past 2^31-1
  let increment = 2_147_483_647
  let assert Ok(wu) =
    h2_frame.encode_window_update(
      stream_id: 0,
      window_size_increment: increment,
    )
  let assert Error(ConnectionError(FlowControlError)) = receive_data(conn, wu)
}

// WINDOW_UPDATE does not send any response frame
pub fn receive_window_update_no_response_test() {
  let conn = helper.connected_connection(Server)
  let assert Ok(wu) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 100)
  let assert Ok(#(_conn, _events, to_send)) = receive_data(conn, wu)
  assert to_send == <<>>
}

// RFC 9113 Section 6.9 - Stream-level WINDOW_UPDATE increases the
// stream's send window.
pub fn receive_window_update_stream_level_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  let assert Ok(wu) =
    h2_frame.encode_window_update(stream_id: 1, window_size_increment: 1000)
  let assert Ok(#(server, events, to_send)) = receive_data(server, wu)
  assert events == []
  assert to_send == <<>>
  let assert Ok(66_535) = h2_core.get_stream_send_window_size(server, 1)
}

// RFC 9113 Section 6.9 - Stream and connection flow control windows
// are independent. Stream-level WINDOW_UPDATE does not affect
// connection send window.
pub fn receive_window_update_stream_does_not_affect_connection_window_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  let assert Ok(wu) =
    h2_frame.encode_window_update(stream_id: 1, window_size_increment: 1000)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, wu)
  assert get_connection_send_window_size(server) == 65_535
  let assert Ok(66_535) = h2_core.get_stream_send_window_size(server, 1)
}

// RFC 9113 Section 6.9 - Multiple stream-level WINDOW_UPDATEs
// accumulate.
pub fn receive_window_update_stream_accumulates_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  let assert Ok(wu1) =
    h2_frame.encode_window_update(stream_id: 1, window_size_increment: 1000)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, wu1)
  let assert Ok(wu2) =
    h2_frame.encode_window_update(stream_id: 1, window_size_increment: 500)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, wu2)
  let assert Ok(67_035) = h2_core.get_stream_send_window_size(server, 1)
}

// RFC 9113 Section 6.9.1 - Stream window overflow is a stream error
// of type FLOW_CONTROL_ERROR. Per Section 5.4.2, stream errors are
// non-fatal: send RST_STREAM and continue.
pub fn receive_window_update_stream_overflow_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  let assert Ok(wu) =
    h2_frame.encode_window_update(
      stream_id: 1,
      window_size_increment: 2_147_483_647,
    )
  let assert Ok(#(_server, events, to_send)) = receive_data(server, wu)
  let assert [StreamReset(stream_id: 1, error_code: FlowControlError)] = events
  let assert Ok(#(frame_data, _rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(h2_frame.RstStream(1, h2_frame.FlowControlError)) =
    h2_frame.decode_frame(frame_data)
}

// --- Stream state validation ---

// RFC 9113 Section 5.1 - "Receiving any frame other than HEADERS or
// PRIORITY on a stream in [idle] state MUST be treated as a connection
// error of type PROTOCOL_ERROR."
pub fn receive_window_update_idle_stream_is_protocol_error_test() {
  let conn = helper.connected_connection(Server)
  let assert Ok(wu) =
    h2_frame.encode_window_update(stream_id: 1, window_size_increment: 1000)
  let assert Error(ConnectionError(ProtocolError)) = receive_data(conn, wu)
}

// RFC 9113 Section 5.1 - WINDOW_UPDATE on an "open" stream is valid.
pub fn receive_window_update_on_open_stream_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  let assert Ok(Open) = get_stream_state(server, 1)

  let assert Ok(wu) =
    h2_frame.encode_window_update(stream_id: 1, window_size_increment: 1000)
  let assert Ok(#(server, events, to_send)) = receive_data(server, wu)
  assert events == []
  assert to_send == <<>>
  let assert Ok(66_535) = h2_core.get_stream_send_window_size(server, 1)
}

// RFC 9113 Section 5.1 - WINDOW_UPDATE on a "half-closed (local)"
// stream is valid. "An endpoint can receive any type of frame in this
// state."
pub fn receive_window_update_on_half_closed_local_stream_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  let assert Ok(#(server, _to_send)) =
    h2_core.send_headers(server, 1, helper.response_headers(), True)

  let assert Ok(wu) =
    h2_frame.encode_window_update(stream_id: 1, window_size_increment: 1000)
  let assert Ok(#(server, events, to_send)) = receive_data(server, wu)
  assert events == []
  assert to_send == <<>>
  let assert Ok(66_535) = h2_core.get_stream_send_window_size(server, 1)
}

// RFC 9113 Section 5.1 - WINDOW_UPDATE is explicitly allowed on
// "half-closed (remote)" streams. "If an endpoint receives additional
// frames, other than WINDOW_UPDATE, PRIORITY, or RST_STREAM, [...]
// it MUST respond with a stream error of type STREAM_CLOSED."
// WINDOW_UPDATE is in the exception list.
pub fn receive_window_update_on_half_closed_remote_stream_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), True)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  let assert Ok(HalfClosedRemote) = get_stream_state(server, 1)

  let assert Ok(wu) =
    h2_frame.encode_window_update(stream_id: 1, window_size_increment: 1000)
  let assert Ok(#(server, events, to_send)) = receive_data(server, wu)
  assert events == []
  assert to_send == <<>>
  let assert Ok(66_535) = h2_core.get_stream_send_window_size(server, 1)
}

// RFC 9113 Section 5.1 - WINDOW_UPDATE on a closed stream. The RFC
// specifically notes: "An endpoint that sends a frame with the
// END_STREAM flag set or a RST_STREAM frame might receive a
// WINDOW_UPDATE [...] from its peer." and "An endpoint MUST minimally
// process and then discard any frames it receives in this state."
// WINDOW_UPDATE on a closed stream should be silently discarded.
pub fn receive_window_update_on_closed_stream_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  let assert Ok(#(server, _to_send)) =
    h2_core.send_rst_stream(server, 1, h2_core.NoError)

  let assert Ok(wu) =
    h2_frame.encode_window_update(stream_id: 1, window_size_increment: 1000)
  // Silently discarded — no events, no response
  let assert Ok(#(_server, events, to_send)) = receive_data(server, wu)
  assert events == []
  assert to_send == <<>>
}

// RFC 9113 Section 5.4.2 - Stream errors are non-fatal. After a stream
// error (RST_STREAM sent for overflow), the connection must continue
// processing new streams.
pub fn receive_window_update_connection_survives_stream_overflow_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(client, headers1, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers1)

  // Overflow stream 1's window — should get RST_STREAM but connection lives
  let assert Ok(wu) =
    h2_frame.encode_window_update(
      stream_id: 1,
      window_size_increment: 2_147_483_647,
    )
  let assert Ok(#(server, events, _to_send)) = receive_data(server, wu)
  let assert [StreamReset(stream_id: 1, error_code: FlowControlError)] = events

  // Connection should still work — open stream 3
  let assert Ok(#(_client, headers3, _stream_id)) =
    open_stream(
      client,
      [
        Header(":method", <<"POST":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
      ],
      False,
    )
  let assert Ok(#(server, events, _to_send)) = receive_data(server, headers3)
  let assert [HeadersReceived(stream_id: 3, headers: _, end_stream: False)] =
    events
  let assert Ok(Open) = get_stream_state(server, 3)
}
