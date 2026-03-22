import h2_core.{
  Client, ConnectionError, FlowControlError, FrameSizeError, Header,
  HeadersReceived, ProtocolError, Server, StreamReset, WithIndexing,
  get_stream_state, open_stream, receive_data, send_window_update,
}
import h2_core/internal/stream.{Closed, HalfClosedLocal, HalfClosedRemote, Open}
import h2_frame
import helper

// RFC 9113 Section 6.9.2 - Initial flow control window is 65,535
pub fn new_connection_default_window_sizes_test() {
  let conn = helper.connected_connection(Client)
  assert conn.send_window_size == 65_535
  assert conn.recv_window_size == 65_535
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

// --- Sending WINDOW_UPDATE ---

// Connection-level window update (stream 0)
pub fn send_window_update_connection_level_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(#(_conn, to_send)) = send_window_update(conn, 0, 65_535)
  let assert Ok(expected) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 65_535)
  assert to_send == expected
}

// Stream-level window update
pub fn send_window_update_stream_level_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  let assert Ok(#(_server, to_send)) = send_window_update(server, 1, 32_768)
  let assert Ok(expected) =
    h2_frame.encode_window_update(stream_id: 1, window_size_increment: 32_768)
  assert to_send == expected
}

// RFC 9113 Section 6.9 - Maximum valid increment from default window
// 2^31-1 (2_147_483_647) minus initial window (65_535) = 2_147_418_112
pub fn send_window_update_max_increment_test() {
  let conn = helper.connected_connection(Client)
  let max_increment = 2_147_483_647 - 65_535
  let assert Ok(#(conn, to_send)) = send_window_update(conn, 0, max_increment)
  assert conn.recv_window_size == 2_147_483_647
  let assert Ok(expected) =
    h2_frame.encode_window_update(
      stream_id: 0,
      window_size_increment: max_increment,
    )
  assert to_send == expected
}

// RFC 9113 Section 6.9 - Minimum valid increment (1)
pub fn send_window_update_min_increment_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(#(_conn, to_send)) = send_window_update(conn, 0, 1)
  let assert Ok(expected) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 1)
  assert to_send == expected
}

// Sending connection-level WINDOW_UPDATE increases recv_window_size
pub fn send_window_update_increases_recv_window_test() {
  let conn = helper.connected_connection(Client)
  assert conn.recv_window_size == 65_535
  let assert Ok(#(conn, _to_send)) = send_window_update(conn, 0, 10_000)
  assert conn.recv_window_size == 75_535
}

// Multiple sent WINDOW_UPDATEs accumulate on recv_window_size
pub fn send_window_update_recv_window_accumulates_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(#(conn, _to_send)) = send_window_update(conn, 0, 1000)
  let assert Ok(#(conn, _to_send)) = send_window_update(conn, 0, 2000)
  assert conn.recv_window_size == 68_535
}

// RFC 9113 Section 6.9.1 - Sending WINDOW_UPDATE that would overflow recv_window_size
pub fn send_window_update_overflow_recv_window_test() {
  let conn = helper.connected_connection(Client)
  // Default is 65_535. Incrementing by 2^31-1 would exceed max
  let assert Error(ConnectionError(FlowControlError)) =
    send_window_update(conn, 0, 2_147_483_647)
}

// Stream-level WINDOW_UPDATE should not affect connection recv_window_size
pub fn send_window_update_stream_does_not_affect_connection_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  let assert Ok(#(server, _to_send)) = send_window_update(server, 1, 10_000)
  assert server.recv_window_size == 65_535
  let assert Ok(75_535) = h2_core.get_stream_recv_window_size(server, 1)
}

// RFC 9113 Section 6.9 - Multiple stream-level WINDOW_UPDATEs accumulate.
pub fn send_window_update_stream_recv_window_accumulates_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  let assert Ok(#(server, _to_send)) = send_window_update(server, 1, 1000)
  let assert Ok(#(server, _to_send)) = send_window_update(server, 1, 2000)
  let assert Ok(68_535) = h2_core.get_stream_recv_window_size(server, 1)
}

// RFC 9113 Section 6.9.1 - Stream recv window overflow is
// FLOW_CONTROL_ERROR.
pub fn send_window_update_stream_overflow_recv_window_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  let assert Error(ConnectionError(FlowControlError)) =
    send_window_update(server, 1, 2_147_483_647)
}

// RFC 9113 Section 6.9 - Receiving WINDOW_UPDATE

// Connection-level WINDOW_UPDATE increases send_window_size
pub fn receive_window_update_connection_level_test() {
  let conn = helper.connected_connection(Client)
  assert conn.send_window_size == 65_535
  let assert Ok(wu) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 1000)
  let assert Ok(#(conn, events, to_send)) = receive_data(conn, wu)
  assert conn.send_window_size == 66_535
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
  assert conn.send_window_size == 67_035
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
  assert server.send_window_size == 65_535
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
  let server = helper.set_stream_state(server, 1, HalfClosedLocal)

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
  let server = helper.set_stream_state(server, 1, Closed)

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
        Header(":method", "POST", WithIndexing),
        Header(":scheme", "https", WithIndexing),
        Header(":path", "/", WithIndexing),
      ],
      False,
    )
  let assert Ok(#(server, events, _to_send)) = receive_data(server, headers3)
  let assert [HeadersReceived(stream_id: 3, headers: _, end_stream: False)] =
    events
  let assert Ok(Open) = get_stream_state(server, 3)
}
