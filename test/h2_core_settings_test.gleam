import gleam/dict
import gleam/option.{None, Some}
import h2_core.{
  Client, ConnectionError, Header, RemoteSettingsChanged, Server,
  SettingsAcknowledged, WithIndexing, new_connection, receive_data,
  send_headers, send_settings,
}
import h2_frame

// RFC 9113 Section 6.5 - Sending SETTINGS
pub fn send_settings_returns_encoded_frame_test() {
  let conn = new_connection(Client)
  let settings = [h2_frame.MaxConcurrentStreams(100)]
  let assert Ok(#(_conn, events, to_send)) = send_settings(conn, settings)
  assert events == []
  let assert Ok(expected) =
    h2_frame.encode_settings(ack: False, settings: settings)
  assert to_send == expected
}

// RFC 9113 Section 6.5.3 - Sent settings are pending until acked
pub fn send_settings_adds_to_pending_test() {
  let conn = new_connection(Client)
  let settings = [h2_frame.MaxConcurrentStreams(100)]
  let assert Ok(#(conn, _events, _to_send)) = send_settings(conn, settings)
  assert conn.pending_settings == [settings]
}

// Local settings should not change until ack received
pub fn send_settings_does_not_change_local_settings_test() {
  let conn = new_connection(Client)
  let original_settings = conn.local_settings
  let settings = [h2_frame.MaxConcurrentStreams(100)]
  let assert Ok(#(conn, _events, _to_send)) = send_settings(conn, settings)
  assert conn.local_settings == original_settings
}

pub fn send_settings_multiple_values_test() {
  let conn = new_connection(Client)
  let settings = [
    h2_frame.MaxConcurrentStreams(100),
    h2_frame.InitialWindowSize(32_768),
    h2_frame.MaxFrameSize(32_768),
  ]
  let assert Ok(#(_conn, events, to_send)) = send_settings(conn, settings)
  assert events == []
  let assert Ok(expected) =
    h2_frame.encode_settings(ack: False, settings: settings)
  assert to_send == expected
}

// Sending two SETTINGS frames queues both in pending
pub fn send_settings_twice_queues_both_test() {
  let conn = new_connection(Client)
  let first = [h2_frame.MaxConcurrentStreams(100)]
  let second = [h2_frame.InitialWindowSize(32_768)]
  let assert Ok(#(conn, _events, _to_send)) = send_settings(conn, first)
  let assert Ok(#(conn, _events, _to_send)) = send_settings(conn, second)
  assert conn.pending_settings == [first, second]
}

// RFC 9113 Section 6.5.3 - Spurious SETTINGS ack is a PROTOCOL_ERROR
pub fn receive_settings_ack_with_nothing_pending_test() {
  let conn = new_connection(Client)
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(conn, settings_ack)
}

// RFC 9113 Section 6.5 - Receiving SETTINGS from peer
pub fn receive_settings_updates_remote_settings_test() {
  let conn = new_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(100),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert conn.remote_settings.max_concurrent_streams == Some(100)
}

pub fn receive_settings_sends_ack_test() {
  let conn = new_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(100),
    ])
  let assert Ok(#(_conn, _events, to_send)) = receive_data(conn, settings_frame)
  let assert Ok(expected) = h2_frame.encode_settings(ack: True, settings: [])
  assert to_send == expected
}

pub fn receive_settings_emits_event_test() {
  let conn = new_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(100),
    ])
  let assert Ok(#(conn, events, _to_send)) = receive_data(conn, settings_frame)
  assert events == [RemoteSettingsChanged(conn.remote_settings)]
}

pub fn receive_settings_does_not_change_local_settings_test() {
  let conn = new_connection(Client)
  let original = conn.local_settings
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(100),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert conn.local_settings == original
}

// RFC 9113 Section 6.5.3 - Receiving SETTINGS ack applies local pending settings
pub fn receive_settings_ack_applies_pending_test() {
  let conn = new_connection(Client)
  let settings = [h2_frame.MaxConcurrentStreams(200)]
  let assert Ok(#(conn, _events, _to_send)) = send_settings(conn, settings)
  assert conn.local_settings.max_concurrent_streams == None
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Ok(#(conn, events, _to_send)) = receive_data(conn, settings_ack)
  assert conn.local_settings.max_concurrent_streams == Some(200)
  assert events == [SettingsAcknowledged(conn.local_settings)]
}

pub fn receive_settings_ack_removes_from_pending_test() {
  let conn = new_connection(Client)
  let first = [h2_frame.MaxConcurrentStreams(100)]
  let second = [h2_frame.InitialWindowSize(32_768)]
  let assert Ok(#(conn, _events, _to_send)) = send_settings(conn, first)
  let assert Ok(#(conn, _events, _to_send)) = send_settings(conn, second)
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_ack)
  assert conn.pending_settings == [second]
}

// RFC 9113 Section 6.5.2 - ENABLE_PUSH invalid value is PROTOCOL_ERROR
pub fn receive_settings_enable_push_invalid_value_test() {
  let conn = new_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.EnablePush(2),
    ])
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(conn, settings_frame)
}

// RFC 9113 Section 6.5.2 - INITIAL_WINDOW_SIZE above 2^31-1 is FLOW_CONTROL_ERROR
pub fn receive_settings_initial_window_size_too_large_test() {
  let conn = new_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      // 2^31 = 2_147_483_648, one above max
      h2_frame.InitialWindowSize(2_147_483_648),
    ])
  let assert Error(ConnectionError(h2_frame.FlowControlError)) =
    receive_data(conn, settings_frame)
}

// RFC 9113 Section 6.5.2 - MAX_FRAME_SIZE below 16384 is PROTOCOL_ERROR
pub fn receive_settings_max_frame_size_too_small_test() {
  let conn = new_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxFrameSize(16_383),
    ])
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(conn, settings_frame)
}

// RFC 9113 Section 6.5.2 - MAX_FRAME_SIZE above 2^24-1 is PROTOCOL_ERROR
pub fn receive_settings_max_frame_size_too_large_test() {
  let conn = new_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      // 2^24 = 16_777_216, one above max
      h2_frame.MaxFrameSize(16_777_216),
    ])
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(conn, settings_frame)
}

// RFC 9113 Section 6.5.2 - Valid ENABLE_PUSH values
pub fn receive_settings_enable_push_zero_test() {
  let conn = new_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.EnablePush(0),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert conn.remote_settings.enable_push == False
}

// ENABLE_PUSH=1 from a client (received by server) is valid
pub fn receive_settings_enable_push_one_test() {
  let conn = new_connection(Server)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.EnablePush(1),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert conn.remote_settings.enable_push == True
}

// RFC 9113 Section 6.5.2 - Valid boundary values
pub fn receive_settings_initial_window_size_max_valid_test() {
  let conn = new_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      // 2^31-1 = 2_147_483_647, exactly the max
      h2_frame.InitialWindowSize(2_147_483_647),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert conn.remote_settings.initial_window_size == 2_147_483_647
}

pub fn receive_settings_max_frame_size_boundary_valid_test() {
  let conn = new_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      // 2^24-1 = 16_777_215, exactly the max
      h2_frame.MaxFrameSize(16_777_215),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert conn.remote_settings.max_frame_size == 16_777_215
}

// RFC 9113 Section 6.5.2 - Client MUST treat ENABLE_PUSH=1 from server as PROTOCOL_ERROR
pub fn receive_settings_server_sends_enable_push_1_test() {
  let conn = new_connection(Client)
  // Simulate receiving settings from a server with ENABLE_PUSH=1
  // A client must reject this
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.EnablePush(1),
    ])
  // For this to work, h2_core needs to know the settings came from a server.
  // Since we're a Client, remote is a server, so ENABLE_PUSH=1 from remote is an error.
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(conn, settings_frame)
}

// RFC 9113 Section 6.5 - Settings processed in order, last value wins
pub fn receive_settings_last_value_wins_test() {
  let conn = new_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(100),
      h2_frame.MaxConcurrentStreams(200),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert conn.remote_settings.max_concurrent_streams == Some(200)
}

// RFC 9113 Section 6.5.2 - Unknown settings MUST be ignored
pub fn receive_settings_unknown_setting_ignored_test() {
  let conn = new_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.UnknownSetting(id: 0xFF, value: 42),
      h2_frame.MaxConcurrentStreams(100),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert conn.remote_settings.max_concurrent_streams == Some(100)
}

// RFC 9113 Section 6.9.2 - "When the value of
// SETTINGS_INITIAL_WINDOW_SIZE changes, a receiver MUST adjust the
// size of all stream flow-control windows that it maintains by the
// difference between the new value and the old value."
//
// When the peer sends a new INITIAL_WINDOW_SIZE, existing streams'
// send windows must be adjusted by the delta.
pub fn receive_settings_initial_window_size_adjusts_existing_streams_test() {
  let server = new_connection(Server)
  let client = new_connection(Client)

  // Open stream 1 on the server
  let assert Ok(#(_client, _events, headers)) =
    send_headers(client, [Header(":method", "GET", WithIndexing)], False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.send_window_size == 65_535

  // Peer (client) sends SETTINGS with INITIAL_WINDOW_SIZE=32768
  // Delta = 32768 - 65535 = -32767
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.InitialWindowSize(32_768),
    ])
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, settings_frame)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.send_window_size == 32_768
}

// RFC 9113 Section 6.9.2 - "An endpoint MUST treat a change to
// SETTINGS_INITIAL_WINDOW_SIZE that causes any flow-control window
// to exceed the maximum size as a connection error (Section 5.4.1)
// of type FLOW_CONTROL_ERROR."
pub fn receive_settings_initial_window_size_overflow_is_flow_control_error_test() {
  let server = new_connection(Server)
  let client = new_connection(Client)

  // Open stream 1 on the server (default send_window_size = 65535)
  let assert Ok(#(_client, _events, headers)) =
    send_headers(client, [Header(":method", "GET", WithIndexing)], False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  // Peer sends SETTINGS with INITIAL_WINDOW_SIZE = 2^31-1
  // Delta = 2_147_483_647 - 65_535 = 2_147_418_112
  // New stream window = 65_535 + 2_147_418_112 = 2_147_483_647 (exactly max, OK)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.InitialWindowSize(2_147_483_647),
    ])
  let assert Ok(#(_server, _events, _to_send)) =
    receive_data(server, settings_frame)

  // Start fresh with a known state
  let server = new_connection(Server)
  let assert Ok(#(_client2, _events, headers2)) =
    send_headers(
      new_connection(Client),
      [Header(":method", "GET", WithIndexing)],
      False,
    )
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers2)

  // Receive a large WINDOW_UPDATE on stream 1 to push window close to max
  let assert Ok(wu) =
    h2_frame.encode_window_update(
      stream_id: 1,
      window_size_increment: 2_147_418_112,
    )
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, wu)
  let assert Ok(stream) = dict.get(server.streams, 1)
  // Stream send window is now 65_535 + 2_147_418_112 = 2_147_483_647
  assert stream.send_window_size == 2_147_483_647

  // Now peer sends SETTINGS with INITIAL_WINDOW_SIZE=65536 (delta = +1)
  // This would push the stream window to 2_147_483_648 which exceeds 2^31-1
  let assert Ok(overflow_settings) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.InitialWindowSize(65_536),
    ])
  let assert Error(ConnectionError(h2_frame.FlowControlError)) =
    receive_data(server, overflow_settings)
}

// RFC 9113 Section 6.5 - "The stream identifier for a SETTINGS
// frame MUST be zero (0x00). If an endpoint receives a SETTINGS
// frame whose Stream Identifier field is anything other than 0x00,
// the endpoint MUST respond with a connection error (Section 5.4.1)
// of type PROTOCOL_ERROR."
pub fn receive_settings_nonzero_stream_id_is_protocol_error_test() {
  let conn = new_connection(Client)
  // Manually craft a SETTINGS frame on stream 1
  // Length=0, Type=0x04, Flags=0, Stream ID=1
  let bad_settings = <<
    0:size(24),
    0x04:size(8),
    0:size(8),
    0:size(1),
    1:size(31),
  >>
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(conn, bad_settings)
}

// RFC 9113 Section 6.5 - "When this bit [ACK] is set, the frame
// payload of the SETTINGS frame MUST be empty. Receipt of a SETTINGS
// frame with the ACK flag set and a length field value other than 0
// MUST be treated as a connection error (Section 5.4.1) of type
// FRAME_SIZE_ERROR."
pub fn receive_settings_ack_with_nonzero_length_is_frame_size_error_test() {
  let conn = new_connection(Client)
  // First send a SETTINGS so there's something pending to ack
  let assert Ok(#(conn, _events, _to_send)) =
    send_settings(conn, [h2_frame.MaxConcurrentStreams(100)])

  // Manually craft a SETTINGS ACK with a non-empty payload
  // Length=6, Type=0x04, Flags=0x01 (ACK), Stream ID=0
  // Payload: one setting entry (6 bytes)
  let bad_ack = <<
    6:size(24),
    0x04:size(8),
    0x01:size(8),
    0:size(1),
    0:size(31),
    0x03:size(16),
    100:size(32),
  >>
  let assert Error(ConnectionError(h2_frame.FrameSizeError)) =
    receive_data(conn, bad_ack)
}
