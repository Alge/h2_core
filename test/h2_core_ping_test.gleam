import h2_core.{
  Client, ConnectionError, FrameSizeError, PingAcknowledged, ProtocolError,
  receive_data, send_ping,
}
import h2_frame
import helper

// RFC 9113 Section 6.7 - PING
pub fn receive_ping_sends_ack_test() {
  let conn = helper.connected_connection(Client)
  let ping_data = <<1, 2, 3, 4, 5, 6, 7, 8>>
  let assert Ok(ping_frame) = h2_frame.encode_ping(ack: False, data: ping_data)
  let assert Ok(#(conn, events, to_send)) = receive_data(conn, ping_frame)
  assert events == []
  assert h2_core.get_recv_buffer(conn) == <<>>
  let assert Ok(expected) = h2_frame.encode_ping(ack: True, data: ping_data)
  assert to_send == expected
}

pub fn receive_ping_ack_emits_event_test() {
  let conn = helper.connected_connection(Client)
  let ping_data = <<1, 2, 3, 4, 5, 6, 7, 8>>
  let assert Ok(ping_ack) = h2_frame.encode_ping(ack: True, data: ping_data)
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, ping_ack)
  assert events == [PingAcknowledged(ping_data)]
  assert to_send == <<>>
}

// RFC 9113 Section 6.7 - PING with wrong length is FRAME_SIZE_ERROR
pub fn receive_ping_wrong_length_test() {
  let conn = helper.connected_connection(Client)
  // Manually craft a PING frame with 4 bytes payload instead of 8
  // Length=4, Type=0x06 (PING), Flags=0, Reserved=0, Stream ID=0
  let bad_ping = <<
    4:size(24),
    0x06:size(8),
    0:size(8),
    0:size(1),
    0:size(31),
    1,
    2,
    3,
    4,
  >>
  let assert Error(ConnectionError(FrameSizeError)) =
    receive_data(conn, bad_ping)
}

// RFC 9113 Section 6.7 - PING on non-zero stream is PROTOCOL_ERROR
pub fn receive_ping_nonzero_stream_test() {
  let conn = helper.connected_connection(Client)
  // Manually craft a PING frame on stream 1
  // Length=8, Type=0x06 (PING), Flags=0, Reserved=0, Stream ID=1
  let bad_ping = <<
    8:size(24),
    0x06:size(8),
    0:size(8),
    0:size(1),
    1:size(31),
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
  >>
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(conn, bad_ping)
}

pub fn send_ping_returns_encoded_frame_test() {
  let conn = helper.connected_connection(Client)
  let ping_data = <<1, 2, 3, 4, 5, 6, 7, 8>>
  let assert Ok(#(_conn, to_send)) = send_ping(conn, ping_data)
  let assert Ok(expected) = h2_frame.encode_ping(ack: False, data: ping_data)
  assert to_send == expected
}

// PING round-trip: send ping, receive ack, verify event
pub fn ping_round_trip_test() {
  let conn = helper.connected_connection(Client)
  let ping_data = <<10, 20, 30, 40, 50, 60, 70, 80>>
  // Send a ping
  let assert Ok(#(conn, _to_send)) = send_ping(conn, ping_data)
  // Simulate receiving the ack back
  let assert Ok(ping_ack) = h2_frame.encode_ping(ack: True, data: ping_data)
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, ping_ack)
  assert events == [PingAcknowledged(ping_data)]
  assert to_send == <<>>
}
