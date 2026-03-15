import h2_core.{
  Client, ConnectionError, Server, new_connection, receive_data,
  send_window_update,
}
import h2_frame

// RFC 9113 Section 6.9.2 - Initial flow control window is 65,535
pub fn new_connection_default_window_sizes_test() {
  let conn = new_connection(Client)
  assert conn.send_window_size == 65_535
  assert conn.recv_window_size == 65_535
}

// RFC 9113 Section 6.9 - Sending WINDOW_UPDATE

// Connection-level window update (stream 0)
pub fn send_window_update_connection_level_test() {
  let conn = new_connection(Client)
  let assert Ok(#(_conn, events, to_send)) = send_window_update(conn, 0, 65_535)
  assert events == []
  let assert Ok(expected) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 65_535)
  assert to_send == expected
}

// Stream-level window update
pub fn send_window_update_stream_level_test() {
  let conn = new_connection(Server)
  let assert Ok(#(_conn, events, to_send)) = send_window_update(conn, 1, 32_768)
  assert events == []
  let assert Ok(expected) =
    h2_frame.encode_window_update(stream_id: 1, window_size_increment: 32_768)
  assert to_send == expected
}

// RFC 9113 Section 6.9 - Maximum valid increment from default window
// 2^31-1 (2_147_483_647) minus initial window (65_535) = 2_147_418_112
pub fn send_window_update_max_increment_test() {
  let conn = new_connection(Client)
  let max_increment = 2_147_483_647 - 65_535
  let assert Ok(#(conn, _events, to_send)) =
    send_window_update(conn, 0, max_increment)
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
  let conn = new_connection(Client)
  let assert Ok(#(_conn, _events, to_send)) = send_window_update(conn, 0, 1)
  let assert Ok(expected) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 1)
  assert to_send == expected
}

// Sending connection-level WINDOW_UPDATE increases recv_window_size
pub fn send_window_update_increases_recv_window_test() {
  let conn = new_connection(Client)
  assert conn.recv_window_size == 65_535
  let assert Ok(#(conn, _events, _to_send)) =
    send_window_update(conn, 0, 10_000)
  assert conn.recv_window_size == 75_535
}

// Multiple sent WINDOW_UPDATEs accumulate on recv_window_size
pub fn send_window_update_recv_window_accumulates_test() {
  let conn = new_connection(Client)
  let assert Ok(#(conn, _events, _to_send)) = send_window_update(conn, 0, 1000)
  let assert Ok(#(conn, _events, _to_send)) = send_window_update(conn, 0, 2000)
  assert conn.recv_window_size == 68_535
}

// RFC 9113 Section 6.9.1 - Sending WINDOW_UPDATE that would overflow recv_window_size
pub fn send_window_update_overflow_recv_window_test() {
  let conn = new_connection(Client)
  // Default is 65_535. Incrementing by 2^31-1 would exceed max
  let assert Error(ConnectionError(h2_frame.FlowControlError)) =
    send_window_update(conn, 0, 2_147_483_647)
}

// Stream-level WINDOW_UPDATE should not affect connection recv_window_size
pub fn send_window_update_stream_does_not_affect_connection_test() {
  let conn = new_connection(Client)
  let assert Ok(#(conn, _events, _to_send)) =
    send_window_update(conn, 1, 10_000)
  assert conn.recv_window_size == 65_535
}

// RFC 9113 Section 6.9 - Receiving WINDOW_UPDATE

// Connection-level WINDOW_UPDATE increases send_window_size
pub fn receive_window_update_connection_level_test() {
  let conn = new_connection(Client)
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
  let conn = new_connection(Client)
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
  let conn = new_connection(Client)
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
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(conn, bad_wu)
}

// RFC 9113 Section 6.9 - Increment of 0 on a stream is stream error PROTOCOL_ERROR
pub fn receive_window_update_zero_increment_stream_test() {
  let conn = new_connection(Client)
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
  let assert Error(h2_core.StreamError(1, h2_frame.ProtocolError)) =
    receive_data(conn, bad_wu)
}

// RFC 9113 Section 6.9 - Wrong frame size is connection error FRAME_SIZE_ERROR
pub fn receive_window_update_wrong_length_test() {
  let conn = new_connection(Client)
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
  let assert Error(ConnectionError(h2_frame.FrameSizeError)) =
    receive_data(conn, bad_wu)
}

// RFC 9113 Section 6.9.1 - Flow control window MUST NOT exceed 2^31-1
pub fn receive_window_update_overflow_connection_test() {
  let conn = new_connection(Client)
  // Default is 65_535. Send an increment that would push past 2^31-1
  let increment = 2_147_483_647
  let assert Ok(wu) =
    h2_frame.encode_window_update(
      stream_id: 0,
      window_size_increment: increment,
    )
  let assert Error(ConnectionError(h2_frame.FlowControlError)) =
    receive_data(conn, wu)
}

// WINDOW_UPDATE does not send any response frame
pub fn receive_window_update_no_response_test() {
  let conn = new_connection(Server)
  let assert Ok(wu) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 100)
  let assert Ok(#(_conn, _events, to_send)) = receive_data(conn, wu)
  assert to_send == <<>>
}
