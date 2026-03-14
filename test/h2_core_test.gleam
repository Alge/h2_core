import gleam/dict
import gleam/option.{None}
import gleeunit
import h2_core.{
  Client, ConnectionError, Idle, Open, PingAcknowledged, Server, Stream,
  new_connection, receive_data, send_headers,
}
import h2_frame

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn new_client_connection_test() {
  let conn = new_connection(Client)
  assert conn.role == Client
}

pub fn new_server_connection_test() {
  let conn = new_connection(Server)
  assert conn.role == Server
}

// RFC 9113 Section 6.5.2 - Default settings values
pub fn new_connection_default_settings_test() {
  let conn = new_connection(Client)
  let settings = conn.local_settings

  assert settings.header_table_size == 4096
  assert settings.enable_push == True
  assert settings.max_concurrent_streams == None
  assert settings.initial_window_size == 65_535
  assert settings.max_frame_size == 16_384
  assert settings.max_header_list_size == None
}

pub fn new_connection_remote_settings_match_defaults_test() {
  let conn = new_connection(Client)
  assert conn.local_settings == conn.remote_settings
}

// RFC 9113 Section 5.1 - Stream States
pub fn new_connection_has_no_streams_test() {
  let conn = new_connection(Client)
  assert conn.streams == dict.new()
}

pub fn stream_initial_state_is_idle_test() {
  let stream = Stream(state: Idle)
  assert stream.state == Idle
}

// RFC 9113 Section 5.1.1 - Stream identifiers
pub fn client_next_stream_id_starts_at_1_test() {
  let conn = new_connection(Client)
  assert conn.next_stream_id == 1
}

pub fn server_next_stream_id_starts_at_2_test() {
  let conn = new_connection(Server)
  assert conn.next_stream_id == 2
}

// RFC 9113 Section 5.1 - send HEADERS transitions idle -> open
pub fn send_headers_opens_stream_test() {
  let conn = new_connection(Client)
  let assert Ok(#(conn, _events)) = send_headers(conn, [], False)
  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == Open
}

pub fn send_headers_increments_stream_id_test() {
  let conn = new_connection(Client)
  let assert Ok(#(conn, _events)) = send_headers(conn, [], False)
  assert conn.next_stream_id == 3
}

pub fn send_headers_returns_no_events_test() {
  let conn = new_connection(Client)
  let assert Ok(#(_conn, events)) = send_headers(conn, [], False)
  assert events == []
}

// Connection recv_buffer
pub fn new_connection_has_empty_recv_buffer_test() {
  let conn = new_connection(Client)
  assert conn.recv_buffer == <<>>
}

// receive_data
pub fn receive_empty_data_test() {
  let conn = new_connection(Client)
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, <<>>)
  assert events == []
  assert to_send == <<>>
}

pub fn receive_partial_frame_buffers_data_test() {
  let conn = new_connection(Client)
  // A few bytes that can't form a complete frame
  let assert Ok(#(conn, events, to_send)) = receive_data(conn, <<1, 2, 3>>)
  assert events == []
  assert to_send == <<>>
  assert conn.recv_buffer == <<1, 2, 3>>
}

pub fn receive_partial_frame_appends_to_buffer_test() {
  let conn = new_connection(Client)
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, <<1, 2, 3>>)
  let assert Ok(#(conn, events, to_send)) = receive_data(conn, <<4, 5, 6>>)
  assert events == []
  assert to_send == <<>>
  assert conn.recv_buffer == <<1, 2, 3, 4, 5, 6>>
}

// RFC 9113 Section 6.7 - PING
pub fn receive_ping_sends_ack_test() {
  let conn = new_connection(Client)
  let ping_data = <<1, 2, 3, 4, 5, 6, 7, 8>>
  let assert Ok(ping_frame) = h2_frame.encode_ping(ack: False, data: ping_data)
  let assert Ok(#(conn, events, to_send)) = receive_data(conn, ping_frame)
  assert events == []
  assert conn.recv_buffer == <<>>
  let assert Ok(expected) = h2_frame.encode_ping(ack: True, data: ping_data)
  assert to_send == expected
}

pub fn receive_ping_ack_emits_event_test() {
  let conn = new_connection(Client)
  let ping_data = <<1, 2, 3, 4, 5, 6, 7, 8>>
  let assert Ok(ping_ack) = h2_frame.encode_ping(ack: True, data: ping_data)
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, ping_ack)
  assert events == [PingAcknowledged(ping_data)]
  assert to_send == <<>>
}

// RFC 9113 Section 6.7 - PING with wrong length is FRAME_SIZE_ERROR
pub fn receive_ping_wrong_length_test() {
  let conn = new_connection(Client)
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
  let assert Error(ConnectionError(h2_frame.FrameSizeError)) =
    receive_data(conn, bad_ping)
}

// RFC 9113 Section 6.7 - PING on non-zero stream is PROTOCOL_ERROR
pub fn receive_ping_nonzero_stream_test() {
  let conn = new_connection(Client)
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
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(conn, bad_ping)
}
