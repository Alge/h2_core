import gleam/option.{None, Some}
import h2_core.{
  Client, CompressionError, ConnectionError, EnablePush, FlowControlError,
  FrameSizeError, HeaderTableSize, InitialWindowSize, MaxConcurrentStreams,
  MaxFrameSize, ProtocolError, RemoteSettingsChanged, Server,
  SettingsAcknowledged, get_local_settings, get_pending_settings,
  get_remote_settings, open_stream, receive_data, send_data, send_headers,
  send_settings,
}
import h2_frame
import helper

// RFC 9113 Section 6.5 - Sending SETTINGS
pub fn send_settings_returns_encoded_frame_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(#(_conn, to_send)) =
    send_settings(conn, [MaxConcurrentStreams(100)])
  let assert Ok(expected) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(100),
    ])
  assert to_send == expected
}

// RFC 9113 Section 6.5.3 - Sent settings are pending until acked
pub fn send_settings_adds_to_pending_test() {
  let conn = helper.connected_connection(Client)
  let settings = [MaxConcurrentStreams(100)]
  let assert Ok(#(conn, _to_send)) = send_settings(conn, settings)
  assert get_pending_settings(conn) == [settings]
}

// Local settings should not change until ack received
pub fn send_settings_does_not_change_local_settings_test() {
  let conn = helper.connected_connection(Client)
  let original_settings = get_local_settings(conn)
  let assert Ok(#(conn, _to_send)) =
    send_settings(conn, [MaxConcurrentStreams(100)])
  assert get_local_settings(conn) == original_settings
}

pub fn send_settings_multiple_values_test() {
  let conn = helper.connected_connection(Client)
  let settings = [
    MaxConcurrentStreams(100),
    InitialWindowSize(32_768),
    MaxFrameSize(32_768),
  ]
  let assert Ok(#(_conn, to_send)) = send_settings(conn, settings)
  let assert Ok(expected) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(100),
      h2_frame.InitialWindowSize(32_768),
      h2_frame.MaxFrameSize(32_768),
    ])
  assert to_send == expected
}

// EnablePush(False) encodes as 0 on the wire
pub fn send_settings_enable_push_false_encodes_as_zero_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(#(_conn, to_send)) = send_settings(conn, [EnablePush(False)])
  let assert Ok(expected) =
    h2_frame.encode_settings(ack: False, settings: [h2_frame.EnablePush(0)])
  assert to_send == expected
}

// EnablePush(True) encodes as 1 on the wire
pub fn send_settings_enable_push_true_encodes_as_one_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(#(_conn, to_send)) = send_settings(conn, [EnablePush(True)])
  let assert Ok(expected) =
    h2_frame.encode_settings(ack: False, settings: [h2_frame.EnablePush(1)])
  assert to_send == expected
}

// Sending two SETTINGS frames queues both in pending
pub fn send_settings_twice_queues_both_test() {
  let conn = helper.connected_connection(Client)
  let first = [MaxConcurrentStreams(100)]
  let second = [InitialWindowSize(32_768)]
  let assert Ok(#(conn, _to_send)) = send_settings(conn, first)
  let assert Ok(#(conn, _to_send)) = send_settings(conn, second)
  assert get_pending_settings(conn) == [first, second]
}

// RFC 9113 Section 6.5.3 - Spurious SETTINGS ack is a PROTOCOL_ERROR
pub fn receive_settings_ack_with_nothing_pending_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(conn, settings_ack)
}

// RFC 9113 Section 6.5 - Receiving SETTINGS from peer
pub fn receive_settings_updates_remote_settings_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(100),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert get_remote_settings(conn).max_concurrent_streams == Some(100)
}

pub fn receive_settings_sends_ack_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(100),
    ])
  let assert Ok(#(_conn, _events, to_send)) = receive_data(conn, settings_frame)
  let assert Ok(expected) = h2_frame.encode_settings(ack: True, settings: [])
  assert to_send == expected
}

pub fn receive_settings_emits_event_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(100),
    ])
  let assert Ok(#(conn, events, _to_send)) = receive_data(conn, settings_frame)
  assert events == [RemoteSettingsChanged(get_remote_settings(conn))]
}

pub fn receive_settings_does_not_change_local_settings_test() {
  let conn = helper.connected_connection(Client)
  let original = get_local_settings(conn)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(100),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert get_local_settings(conn) == original
}

// RFC 9113 Section 6.5.3 - Receiving SETTINGS ack applies local pending settings
pub fn receive_settings_ack_applies_pending_test() {
  let conn = helper.connected_connection(Client)
  let settings = [MaxConcurrentStreams(200)]
  let assert Ok(#(conn, _to_send)) = send_settings(conn, settings)
  assert get_local_settings(conn).max_concurrent_streams == None
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Ok(#(conn, events, _to_send)) = receive_data(conn, settings_ack)
  assert get_local_settings(conn).max_concurrent_streams == Some(200)
  assert events == [SettingsAcknowledged(get_local_settings(conn))]
}

pub fn receive_settings_ack_removes_from_pending_test() {
  let conn = helper.connected_connection(Client)
  let first = [MaxConcurrentStreams(100)]
  let second = [InitialWindowSize(32_768)]
  let assert Ok(#(conn, _to_send)) = send_settings(conn, first)
  let assert Ok(#(conn, _to_send)) = send_settings(conn, second)
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_ack)
  assert get_pending_settings(conn) == [second]
}

// RFC 9113 Section 6.5.2 - ENABLE_PUSH invalid value is PROTOCOL_ERROR
pub fn receive_settings_enable_push_invalid_value_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.EnablePush(2),
    ])
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(conn, settings_frame)
}

// RFC 9113 Section 6.5.2 - INITIAL_WINDOW_SIZE above 2^31-1 is FLOW_CONTROL_ERROR
pub fn receive_settings_initial_window_size_too_large_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      // 2^31 = 2_147_483_648, one above max
      h2_frame.InitialWindowSize(2_147_483_648),
    ])
  let assert Error(ConnectionError(FlowControlError)) =
    receive_data(conn, settings_frame)
}

// RFC 9113 Section 6.5.2 - MAX_FRAME_SIZE below 16384 is PROTOCOL_ERROR
pub fn receive_settings_max_frame_size_too_small_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxFrameSize(16_383),
    ])
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(conn, settings_frame)
}

// RFC 9113 Section 6.5.2 - MAX_FRAME_SIZE above 2^24-1 is PROTOCOL_ERROR
pub fn receive_settings_max_frame_size_too_large_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      // 2^24 = 16_777_216, one above max
      h2_frame.MaxFrameSize(16_777_216),
    ])
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(conn, settings_frame)
}

// RFC 9113 Section 6.5.2 - Valid ENABLE_PUSH values
pub fn receive_settings_enable_push_zero_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.EnablePush(0),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert get_remote_settings(conn).enable_push == False
}

// ENABLE_PUSH=1 from a client (received by server) is valid
pub fn receive_settings_enable_push_one_test() {
  let conn = helper.connected_connection(Server)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.EnablePush(1),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert get_remote_settings(conn).enable_push == True
}

// RFC 9113 Section 6.5.2 - Valid boundary values
pub fn receive_settings_initial_window_size_max_valid_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      // 2^31-1 = 2_147_483_647, exactly the max
      h2_frame.InitialWindowSize(2_147_483_647),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert get_remote_settings(conn).initial_window_size == 2_147_483_647
}

pub fn receive_settings_max_frame_size_boundary_valid_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      // 2^24-1 = 16_777_215, exactly the max
      h2_frame.MaxFrameSize(16_777_215),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert get_remote_settings(conn).max_frame_size == 16_777_215
}

// RFC 9113 Section 6.5.2 - Client MUST treat ENABLE_PUSH=1 from server as PROTOCOL_ERROR
pub fn receive_settings_server_sends_enable_push_1_test() {
  let conn = helper.connected_connection(Client)
  // Simulate receiving settings from a server with ENABLE_PUSH=1
  // A client must reject this
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.EnablePush(1),
    ])
  // For this to work, h2_core needs to know the settings came from a server.
  // Since we're a Client, remote is a server, so ENABLE_PUSH=1 from remote is an error.
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(conn, settings_frame)
}

// RFC 9113 Section 6.5 - Settings processed in order, last value wins
pub fn receive_settings_last_value_wins_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(100),
      h2_frame.MaxConcurrentStreams(200),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert get_remote_settings(conn).max_concurrent_streams == Some(200)
}

// RFC 9113 Section 6.5.2 - Unknown settings MUST be ignored
pub fn receive_settings_unknown_setting_ignored_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.UnknownSetting(id: 0xFF, value: 42),
      h2_frame.MaxConcurrentStreams(100),
    ])
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, settings_frame)
  assert get_remote_settings(conn).max_concurrent_streams == Some(100)
}

// RFC 9113 Section 6.9.2 - "When the value of
// SETTINGS_INITIAL_WINDOW_SIZE changes, a receiver MUST adjust the
// size of all stream flow-control windows that it maintains by the
// difference between the new value and the old value."
//
// When the peer sends a new INITIAL_WINDOW_SIZE, existing streams'
// send windows must be adjusted by the delta.
pub fn receive_settings_initial_window_size_adjusts_existing_streams_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)

  // Open stream 1 on the server
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  let assert Ok(65_535) = h2_core.get_stream_send_window_size(server, 1)

  // Peer (client) sends SETTINGS with INITIAL_WINDOW_SIZE=32768
  // Delta = 32768 - 65535 = -32767
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.InitialWindowSize(32_768),
    ])
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, settings_frame)
  let assert Ok(32_768) = h2_core.get_stream_send_window_size(server, 1)
}

// RFC 9113 Section 6.9.2 - "An endpoint MUST treat a change to
// SETTINGS_INITIAL_WINDOW_SIZE that causes any flow-control window
// to exceed the maximum size as a connection error (Section 5.4.1)
// of type FLOW_CONTROL_ERROR."
pub fn receive_settings_initial_window_size_overflow_is_flow_control_error_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)

  // Open stream 1 on the server (default send_window_size = 65535)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
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
  let server = helper.connected_connection(Server)
  let assert Ok(#(_client2, headers2, _stream_id)) =
    open_stream(
      helper.connected_connection(Client),
      helper.request_headers(),
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
  // Stream send window is now 65_535 + 2_147_418_112 = 2_147_483_647
  let assert Ok(2_147_483_647) = h2_core.get_stream_send_window_size(server, 1)

  // Now peer sends SETTINGS with INITIAL_WINDOW_SIZE=65536 (delta = +1)
  // This would push the stream window to 2_147_483_648 which exceeds 2^31-1
  let assert Ok(overflow_settings) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.InitialWindowSize(65_536),
    ])
  let assert Error(ConnectionError(FlowControlError)) =
    receive_data(server, overflow_settings)
}

// RFC 9113 Section 6.5 - "The stream identifier for a SETTINGS
// frame MUST be zero (0x00). If an endpoint receives a SETTINGS
// frame whose Stream Identifier field is anything other than 0x00,
// the endpoint MUST respond with a connection error (Section 5.4.1)
// of type PROTOCOL_ERROR."
pub fn receive_settings_nonzero_stream_id_is_protocol_error_test() {
  let conn = helper.connected_connection(Client)
  // Manually craft a SETTINGS frame on stream 1
  // Length=0, Type=0x04, Flags=0, Stream ID=1
  let bad_settings = <<
    0:size(24),
    0x04:size(8),
    0:size(8),
    0:size(1),
    1:size(31),
  >>
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(conn, bad_settings)
}

// RFC 9113 Section 6.5 - "When this bit [ACK] is set, the frame
// payload of the SETTINGS frame MUST be empty. Receipt of a SETTINGS
// frame with the ACK flag set and a length field value other than 0
// MUST be treated as a connection error (Section 5.4.1) of type
// FRAME_SIZE_ERROR."
pub fn receive_settings_ack_with_nonzero_length_is_frame_size_error_test() {
  let conn = helper.connected_connection(Client)
  // First send a SETTINGS so there's something pending to ack
  let assert Ok(#(conn, _to_send)) =
    send_settings(conn, [MaxConcurrentStreams(100)])

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
  let assert Error(ConnectionError(FrameSizeError)) =
    receive_data(conn, bad_ack)
}

// RFC 9113 Section 6.9.2 - "When the value of SETTINGS_INITIAL_WINDOW_SIZE
// changes, a receiver MUST adjust the size of all stream flow-control windows
// that it maintains by the difference between the new value and the old value."
//
// When our own SETTINGS with INITIAL_WINDOW_SIZE is acknowledged, we must
// adjust recv_window_size on existing streams by the delta.
pub fn settings_ack_initial_window_size_adjusts_recv_window_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)

  // Client opens stream 1
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  let assert Ok(65_535) = h2_core.get_stream_recv_window_size(server, 1)

  // Server sends SETTINGS with INITIAL_WINDOW_SIZE=32768
  let assert Ok(#(server, _to_send)) =
    send_settings(server, [InitialWindowSize(32_768)])

  // Server receives ACK for its settings
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, settings_ack)
  // Delta = 32768 - 65535 = -32767, so recv_window_size = 65535 - 32767 = 32768
  let assert Ok(32_768) = h2_core.get_stream_recv_window_size(server, 1)
}

// RFC 9113 Section 6.9.2 - "A change to SETTINGS_INITIAL_WINDOW_SIZE can
// cause the available space in a flow-control window to become negative.
// A sender MUST track the negative flow-control window and MUST NOT send
// new flow-controlled frames until it receives WINDOW_UPDATE frames that
// cause the flow-control window to become positive."
//
// When the peer reduces INITIAL_WINDOW_SIZE, a stream's send_window_size
// can go negative. This must not cause an error.
pub fn receive_settings_initial_window_size_can_go_negative_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)

  // Client opens stream 1 (default send_window_size = 65535)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  // Peer sends SETTINGS with INITIAL_WINDOW_SIZE=0
  // Delta = 0 - 65535 = -65535
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.InitialWindowSize(0),
    ])
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, settings_frame)
  // Window should be negative: 65535 + (-65535) = 0
  let assert Ok(0) = h2_core.get_stream_send_window_size(server, 1)
}

// RFC 9113 Section 6.9.2 - "A change to SETTINGS_INITIAL_WINDOW_SIZE can
// cause the available space in a flow-control window to become negative.
// A sender MUST track the negative flow-control window and MUST NOT send
// new flow-controlled frames until it receives WINDOW_UPDATE frames that
// cause the flow-control window to become positive."
//
// Simulate a stream that has partially consumed its window (e.g. by sending
// data), then the peer reduces INITIAL_WINDOW_SIZE enough to push the
// remaining window negative. This must not error.
pub fn receive_settings_initial_window_size_negative_window_tracked_test() {
  let #(server, _client) = helper.server_with_open_stream()

  // Server sends response headers (non-END_STREAM) so it can send data
  let assert Ok(#(server, _to_send)) =
    send_headers(server, 1, helper.response_headers(), False)

  // Server sends 100 bytes of data, consuming window:
  // send_window_size = 65535 - 100 = 65435
  let assert Ok(#(server, _to_send)) =
    send_data(server, 1, <<0:size(800)>>, False, None)
  let assert Ok(v) = h2_core.get_stream_send_window_size(server, 1)
  assert v == 65_435

  // Client (peer) reduces INITIAL_WINDOW_SIZE to 0. Delta = 0 - 65535 = -65535
  // send_window_size = 65435 + (-65535) = -100
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.InitialWindowSize(0),
    ])
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, settings_frame)
  let assert Ok(v) = h2_core.get_stream_send_window_size(server, 1)
  assert v == -100
}

// RFC 9113 Section 6.5 - "A SETTINGS frame with a length other than a
// multiple of 6 octets MUST be treated as a connection error (Section 5.4.1)
// of type FRAME_SIZE_ERROR."
pub fn receive_settings_non_multiple_of_six_length_is_frame_size_error_test() {
  let conn = helper.connected_connection(Client)
  // Manually craft a SETTINGS frame with 7 bytes payload (not a multiple of 6)
  // Length=7, Type=0x04, Flags=0, Stream ID=0
  let bad_settings = <<
    7:size(24),
    0x04:size(8),
    0:size(8),
    0:size(1),
    0:size(31),
    // 7 bytes of payload (invalid)
    0:size(56),
  >>
  let assert Error(ConnectionError(FrameSizeError)) =
    receive_data(conn, bad_settings)
}

// RFC 9113 Section 4.3.1 - "Once an endpoint acknowledges a change to
// SETTINGS_HEADER_TABLE_SIZE that reduces the maximum below the current
// size of the dynamic table, its HPACK encoder MUST start the next
// field block with a Dynamic Table Size Update instruction."
//
// When the remote peer reduces our header table size via SETTINGS,
// our encoder must call resize_dynamic so the next encode includes
// the size update instruction.
pub fn receive_settings_header_table_size_reduction_affects_encoder_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  // Server sends SETTINGS reducing header table size to 0
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.HeaderTableSize(0),
    ])
  // Client receives the settings - this applies the new table size
  // and resizes the encoder automatically
  let assert Ok(#(client, _events, _to_send)) =
    receive_data(client, settings_frame)

  // Client sends headers on a new stream - the encoded block must
  // start with a Dynamic Table Size Update instruction. The server
  // should be able to decode it without COMPRESSION_ERROR.
  let assert Ok(#(_client, encoded, _stream_id)) =
    open_stream(client, helper.request_headers(), False)

  // Server receives the new headers - if the encoder didn't emit
  // the size update, the server's decoder will be out of sync
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded)
  let assert [h2_core.HeadersReceived(stream_id: 3, ..)] = events
}

// RFC 9113 Section 4.3.1 - "An endpoint MUST treat a field block that
// follows an acknowledgment of the reduction to the maximum dynamic
// table size as a connection error (Section 5.4.1) of type
// COMPRESSION_ERROR if it does not start with a conformant Dynamic
// Table Size Update instruction."
//
// When we reduce our header table size via SETTINGS and the peer
// acknowledges, the peer's next field block must include a size update.
// If it doesn't, we must return COMPRESSION_ERROR.
pub fn receive_headers_without_required_size_update_is_compression_error_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)

  // Client sends SETTINGS reducing header table size to 0
  let assert Ok(#(client, settings_frame)) =
    send_settings(client, [HeaderTableSize(0)])
  // Server receives and processes settings
  let assert Ok(#(_server, _events, _to_send)) =
    receive_data(server, settings_frame)

  // Server encodes headers WITHOUT calling resize_dynamic - the encoded
  // block won't have a size update instruction. Client should reject
  // with COMPRESSION_ERROR.
  // To simulate this, we use a raw HPACK block that doesn't start
  // with a Dynamic Table Size Update.
  let bad_hpack = <<0x88>>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 2,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Error(ConnectionError(CompressionError)) =
    receive_data(client, headers_frame)
}
