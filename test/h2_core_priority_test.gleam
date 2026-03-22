import h2_core.{
  Client, ConnectionError, FrameSizeError, ProtocolError, StreamReset,
  receive_data,
}
import h2_frame
import helper

// RFC 9113 Section 6.3 - PRIORITY
// "The PRIORITY frame (type=0x02) is deprecated; see Section 5.3.2."
// "Sending or receiving a PRIORITY frame does not affect the state of
// any stream (Section 5.1)."
// A valid PRIORITY frame should be accepted and silently ignored.
pub fn receive_priority_accepted_and_ignored_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(priority_frame) =
    h2_frame.encode_priority(
      stream_id: 1,
      exclusive: False,
      stream_dependency: 0,
      weight: 16,
    )
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, priority_frame)
  assert events == []
  assert to_send == <<>>
}

// RFC 9113 Section 6.3
// "If a PRIORITY frame is received with a stream identifier of 0x00,
// the recipient MUST respond with a connection error (Section 5.4.1)
// of type PROTOCOL_ERROR."
pub fn receive_priority_stream_zero_is_protocol_error_test() {
  let conn = helper.connected_connection(Client)
  // Manually craft a PRIORITY frame on stream 0
  // h2_frame.encode_priority guards against stream_id 0, so we craft it by hand.
  // Length=5, Type=0x02 (PRIORITY), Flags=0, Reserved=0, Stream ID=0
  let bad_priority = <<
    5:size(24),
    0x02:size(8),
    0:size(8),
    0:size(1),
    0:size(31),
    0:size(1),
    0:size(31),
    16:size(8),
  >>
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(conn, bad_priority)
}

// RFC 9113 Section 6.3
// "A PRIORITY frame with a length other than 5 octets MUST be treated
// as a stream error (Section 5.4.2) of type FRAME_SIZE_ERROR."
pub fn receive_priority_wrong_length_is_stream_error_test() {
  let conn = helper.connected_connection(Client)
  // Manually craft a PRIORITY frame with 4 bytes payload instead of 5
  // Length=4, Type=0x02 (PRIORITY), Flags=0, Reserved=0, Stream ID=1
  let bad_priority = <<
    4:size(24),
    0x02:size(8),
    0:size(8),
    0:size(1),
    1:size(31),
    0:size(1),
    0:size(31),
  >>
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, bad_priority)
  assert events == [StreamReset(stream_id: 1, error_code: FrameSizeError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(
      stream_id: 1,
      error_code: h2_frame.FrameSizeError,
    )
  assert to_send == expected_rst
}

// RFC 9113 Section 6.3
// "The PRIORITY frame can be sent on a stream in any state, including
// 'idle' or 'closed'."
// Receiving PRIORITY on an idle stream (never opened) should succeed.
pub fn receive_priority_on_idle_stream_test() {
  let conn = helper.connected_connection(Client)
  // Stream 3 has never been opened — it is idle
  let assert Ok(priority_frame) =
    h2_frame.encode_priority(
      stream_id: 3,
      exclusive: True,
      stream_dependency: 0,
      weight: 255,
    )
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, priority_frame)
  assert events == []
  assert to_send == <<>>
}

// RFC 9113 Section 6.3
// "Sending or receiving a PRIORITY frame does not affect the state of
// any stream (Section 5.1)."
// Verify the exclusive flag variant is also accepted without error.
pub fn receive_priority_with_exclusive_flag_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(priority_frame) =
    h2_frame.encode_priority(
      stream_id: 1,
      exclusive: True,
      stream_dependency: 5,
      weight: 128,
    )
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, priority_frame)
  assert events == []
  assert to_send == <<>>
}
