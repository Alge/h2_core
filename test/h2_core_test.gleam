import gleam/dict
import gleam/option.{None}
import gleeunit
import h2_core.{
  Client, Idle, Open, Server, Stream, new_connection, receive_data, send_headers,
}

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
  let assert Ok(#(conn, _events, _to_send)) = send_headers(conn, [], False)
  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == Open
}

pub fn send_headers_increments_stream_id_test() {
  let conn = new_connection(Client)
  let assert Ok(#(conn, _events, _to_send)) = send_headers(conn, [], False)
  assert conn.next_stream_id == 3
}

pub fn send_headers_returns_no_events_test() {
  let conn = new_connection(Client)
  let assert Ok(#(_conn, events, _to_send)) = send_headers(conn, [], False)
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
