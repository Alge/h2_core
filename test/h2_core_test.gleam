import gleam/dict
import gleeunit
import h2_core.{
  Client, Connected, Header, Idle, Open, Server, Stream, WithIndexing,
  default_settings, new_connection, open_stream, receive_data,
}
import helper

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn new_client_connection_test() {
  let assert Ok(#(conn, _)) = new_connection(Client, default_settings())
  assert conn.role == Client
}

pub fn new_server_connection_test() {
  let assert Ok(#(conn, _)) = new_connection(Server, default_settings())
  assert conn.role == Server
}

pub fn new_connection_remote_settings_match_defaults_test() {
  let assert Ok(#(conn, _)) = new_connection(Client, default_settings())
  assert conn.local_settings == conn.remote_settings
}

// RFC 9113 Section 5.1 - Stream States
pub fn new_connection_has_no_streams_test() {
  let assert Ok(#(conn, _)) = new_connection(Client, default_settings())
  assert conn.streams == dict.new()
}

pub fn stream_initial_state_is_idle_test() {
  let stream =
    Stream(state: Idle, send_window_size: 65_535, recv_window_size: 65_535)
  assert stream.state == Idle
}

// RFC 9113 Section 5.1.1 - Stream identifiers
pub fn client_next_stream_id_starts_at_1_test() {
  let assert Ok(#(conn, _)) = new_connection(Client, default_settings())
  assert conn.next_stream_id == 1
}

pub fn server_next_stream_id_starts_at_2_test() {
  let assert Ok(#(conn, _)) = new_connection(Server, default_settings())
  assert conn.next_stream_id == 2
}

// RFC 9113 Section 5.1 - send HEADERS transitions idle -> open
pub fn open_stream_opens_stream_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Ok(#(conn, _events, _to_send)) = open_stream(conn, [], False)
  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == Open
}

pub fn open_stream_increments_stream_id_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Ok(#(conn, _events, _to_send)) = open_stream(conn, [], False)
  assert conn.next_stream_id == 3
}

pub fn open_stream_returns_no_events_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Ok(#(_conn, events, _to_send)) = open_stream(conn, [], False)
  assert events == []
}

// Connection recv_buffer
pub fn new_connection_has_empty_recv_buffer_test() {
  let assert Ok(#(conn, _)) = new_connection(Client, default_settings())
  assert conn.recv_buffer == <<>>
}

// receive_data
pub fn receive_empty_data_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, <<>>)
  assert events == []
  assert to_send == <<>>
}

pub fn receive_partial_frame_buffers_data_test() {
  let conn = helper.new_connection(Client, Connected)
  // A partial frame header: length=5, then 2 bytes of the remaining 6 header bytes.
  // Not enough for a complete 9-byte header, so this should buffer.
  let partial = <<0, 0, 5, 0x06, 0>>
  let assert Ok(#(conn, events, to_send)) = receive_data(conn, partial)
  assert events == []
  assert to_send == <<>>
  assert conn.recv_buffer == partial
}

pub fn receive_partial_frame_appends_to_buffer_test() {
  let conn = helper.new_connection(Client, Connected)
  // First chunk: length=5, partial header
  let chunk1 = <<0, 0, 5, 0x06>>
  let assert Ok(#(conn, _events, _to_send)) = receive_data(conn, chunk1)
  // Second chunk: more header bytes but still incomplete frame
  let chunk2 = <<0, 0, 0, 0, 0>>
  let assert Ok(#(conn, events, to_send)) = receive_data(conn, chunk2)
  assert events == []
  assert to_send == <<>>
  assert conn.recv_buffer == <<chunk1:bits, chunk2:bits>>
}

// --- General frame processing (Section 4) ---

// RFC 9113 Section 4.1 - "Implementations MUST ignore and discard
// frames of unknown types."
//
// An unknown frame type should not cause an error; the connection
// should continue processing subsequent frames normally.
pub fn receive_unknown_frame_type_is_ignored_test() {
  let conn = helper.new_connection(Client, Connected)
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
  let server = helper.new_connection(Server, Connected)
  let client = helper.new_connection(Client, Connected)
  let assert Ok(#(_client, _events, headers)) =
    open_stream(client, [Header(":method", "GET", WithIndexing)], False)
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
  let conn = helper.new_connection(Client, Connected)
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
