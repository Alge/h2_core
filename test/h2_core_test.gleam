import gleam/dict
import gleam/option.{None}
import gleeunit
import h2_core.{
  Client, ConnectionError, Header, Idle, Open, Server, Stream, WithIndexing,
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
  let stream =
    Stream(state: Idle, send_window_size: 65_535, recv_window_size: 65_535)
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

// --- General frame processing (Section 4) ---

// RFC 9113 Section 4.1 - "Implementations MUST ignore and discard
// frames of unknown types."
//
// An unknown frame type should not cause an error; the connection
// should continue processing subsequent frames normally.
pub fn receive_unknown_frame_type_is_ignored_test() {
  let conn = new_connection(Client)
  // Craft a frame with unknown type 0xFF
  // Length=5, Type=0xFF, Flags=0, Stream ID=0, Payload=5 bytes
  let unknown_frame = <<
    5:size(24),
    0xFF:size(8),
    0:size(8),
    0:size(1),
    0:size(31),
    1,
    2,
    3,
    4,
    5,
  >>
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, unknown_frame)
  assert events == []
  assert to_send == <<>>
}

// Unknown frame types on a stream should also be ignored.
pub fn receive_unknown_frame_type_on_stream_is_ignored_test() {
  let server = new_connection(Server)
  let client = new_connection(Client)
  let assert Ok(#(_client, _events, headers)) =
    send_headers(client, [Header(":method", "GET", WithIndexing)], False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  // Unknown frame type 0xFE on stream 1
  let unknown_frame = <<
    3:size(24),
    0xFE:size(8),
    0:size(8),
    0:size(1),
    1:size(31),
    10,
    20,
    30,
  >>
  let assert Ok(#(server, events, to_send)) =
    receive_data(server, unknown_frame)
  assert events == []
  assert to_send == <<>>
  // Stream 1 should still be open
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Open
}

// RFC 9113 Section 4.1 - "Reserved: A reserved 1-bit field. The
// semantics of this bit are undefined, and the bit MUST remain unset
// (0x00) when sending and MUST be ignored when receiving."
//
// A frame with the reserved bit set should be processed normally.
pub fn receive_frame_with_reserved_bit_set_is_accepted_test() {
  let conn = new_connection(Client)
  // Craft a valid PING frame but with the reserved bit set to 1
  // Length=8, Type=0x06, Flags=0, Reserved=1, Stream ID=0
  let ping_with_reserved_bit = <<
    8:size(24),
    0x06:size(8),
    0:size(8),
    1:size(1),
    0:size(31),
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
  >>
  let assert Ok(#(_conn, _events, to_send)) =
    receive_data(conn, ping_with_reserved_bit)
  // Should have responded with PING ACK
  assert to_send != <<>>
}

// RFC 9113 Section 4.2 - "An endpoint MUST send an error code of
// FRAME_SIZE_ERROR if a frame exceeds the size defined in
// SETTINGS_MAX_FRAME_SIZE, exceeds any limit defined for the frame
// type, or is too small to contain mandatory frame data. A frame size
// error in a frame that could alter the state of the entire
// connection MUST be treated as a connection error (Section 5.4.1)."
//
// A DATA frame exceeding SETTINGS_MAX_FRAME_SIZE (default 16384) on
// a stream should be a connection error of type FRAME_SIZE_ERROR since
// a frame that exceeds the limit is always a connection error.
pub fn receive_frame_exceeding_max_frame_size_test() {
  let server = new_connection(Server)
  let client = new_connection(Client)
  let assert Ok(#(_client, _events, headers)) =
    send_headers(client, [Header(":method", "GET", WithIndexing)], False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)

  // Craft a DATA frame (type 0x00) with payload larger than 16384 bytes
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
  let assert Error(ConnectionError(h2_frame.FrameSizeError)) =
    receive_data(server, oversized_frame)
}
