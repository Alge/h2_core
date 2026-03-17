import gleam/option
import h2_core.{
  Client, ConnectionError, RemoteSettingsChanged, Server, new_connection,
  receive_data,
}
import h2_frame

// The client connection preface magic string (24 bytes)
const client_preface_magic = <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n":utf8>>

// ---------------------------------------------------------------------------
// RFC 9113 Section 3.4 - HTTP/2 Connection Preface
//
// "The client connection preface starts with a sequence of 24 octets,
//  which in hex notation is:
//    0x505249202a20485454502f322e300d0a0d0a534d0d0a0d0a
//  That is, the connection preface starts with the string
//  PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n). This sequence MUST be followed
//  by a SETTINGS frame (Section 6.5), which MAY be empty."
// ---------------------------------------------------------------------------

// A server receiving the correct client preface (magic + SETTINGS) should
// succeed and emit a RemoteSettingsChanged event for the peer's SETTINGS.
pub fn server_receives_valid_client_preface_test() {
  let conn = new_connection(Server)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [])
  let data = <<client_preface_magic:bits, settings_frame:bits>>
  let assert Ok(#(_conn, events, _to_send)) = receive_data(conn, data)
  // The server should process the SETTINGS frame and emit RemoteSettingsChanged
  let assert [RemoteSettingsChanged(_)] = events
}

// A server receiving the correct preface with non-empty SETTINGS should work.
pub fn server_receives_valid_preface_with_settings_test() {
  let conn = new_connection(Server)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(100),
    ])
  let data = <<client_preface_magic:bits, settings_frame:bits>>
  let assert Ok(#(conn, events, _to_send)) = receive_data(conn, data)
  let assert [RemoteSettingsChanged(_)] = events
  let assert option.Some(100) = conn.remote_settings.max_concurrent_streams
}

// ---------------------------------------------------------------------------
// RFC 9113 Section 3.4:
// "Clients and servers MUST treat an invalid connection preface as a
//  connection error (Section 5.4.1) of type PROTOCOL_ERROR."
// ---------------------------------------------------------------------------

// A server receiving garbage bytes instead of the client preface magic
// MUST result in a connection error of type PROTOCOL_ERROR.
pub fn server_receives_invalid_preface_bytes_test() {
  let conn = new_connection(Server)
  let garbage = <<"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n":utf8>>
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(conn, garbage)
}

// A server receiving partially correct magic followed by wrong bytes
// MUST result in PROTOCOL_ERROR.
pub fn server_receives_corrupted_preface_magic_test() {
  let conn = new_connection(Server)
  // First 10 bytes correct, then garbage
  let corrupted = <<"PRI * HTTP/XXXXXXXXXXXXXX":utf8>>
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(conn, corrupted)
}

// ---------------------------------------------------------------------------
// RFC 9113 Section 3.4:
// "This sequence MUST be followed by a SETTINGS frame"
//
// If the server receives the 24-byte magic but the next frame is NOT a
// SETTINGS frame, that is an invalid connection preface -> PROTOCOL_ERROR.
// ---------------------------------------------------------------------------

// Client preface magic followed by a non-SETTINGS frame (e.g. PING)
// MUST be a PROTOCOL_ERROR.
pub fn server_receives_preface_magic_followed_by_non_settings_test() {
  let conn = new_connection(Server)
  let assert Ok(ping_frame) =
    h2_frame.encode_ping(ack: False, data: <<1, 2, 3, 4, 5, 6, 7, 8>>)
  let data = <<client_preface_magic:bits, ping_frame:bits>>
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(conn, data)
}

// Client preface magic followed by a SETTINGS ACK (not a non-ack SETTINGS)
// MUST be a PROTOCOL_ERROR because the first frame must be a SETTINGS
// (non-ack) frame.
pub fn server_receives_preface_magic_followed_by_settings_ack_test() {
  let conn = new_connection(Server)
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let data = <<client_preface_magic:bits, settings_ack:bits>>
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(conn, data)
}

// ---------------------------------------------------------------------------
// Partial preface - incomplete data should not error, the connection should
// buffer and wait for more data.
// ---------------------------------------------------------------------------

// Server receives only the 24-byte magic without a SETTINGS frame yet.
// This is incomplete, not an error -- the server should wait for more data.
pub fn server_receives_only_preface_magic_waits_for_settings_test() {
  let conn = new_connection(Server)
  let assert Ok(#(conn, events, to_send)) =
    receive_data(conn, client_preface_magic)
  // No events yet since SETTINGS hasn't arrived
  assert events == []
  assert to_send == <<>>
  // Now send the SETTINGS frame
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [])
  let assert Ok(#(_conn, events, _to_send)) = receive_data(conn, settings_frame)
  let assert [RemoteSettingsChanged(_)] = events
}

// Server receives the magic in two separate chunks (split mid-magic).
// Both chunks are individually incomplete -- the connection should buffer.
pub fn server_receives_preface_magic_in_chunks_test() {
  let conn = new_connection(Server)
  // Split the 24-byte magic into two parts
  let assert <<part1:bytes-size(12), part2:bytes>> = client_preface_magic
  let assert Ok(#(conn, events, to_send)) = receive_data(conn, part1)
  assert events == []
  assert to_send == <<>>
  // Send the rest of the magic + SETTINGS
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [])
  let data = <<part2:bits, settings_frame:bits>>
  let assert Ok(#(_conn, events, _to_send)) = receive_data(conn, data)
  let assert [RemoteSettingsChanged(_)] = events
}

// ---------------------------------------------------------------------------
// RFC 9113 Section 3.4:
// "The server connection preface consists of a potentially empty SETTINGS
//  frame (Section 6.5) that MUST be the first frame the server sends in
//  the HTTP/2 connection."
//
// From the client's perspective, the first frame received from the server
// MUST be a SETTINGS frame.
// ---------------------------------------------------------------------------

// A client receiving a non-SETTINGS frame as the first frame from the server
// MUST treat it as PROTOCOL_ERROR.
pub fn client_receives_non_settings_as_first_frame_test() {
  let conn = new_connection(Client)
  let assert Ok(ping_frame) =
    h2_frame.encode_ping(ack: False, data: <<1, 2, 3, 4, 5, 6, 7, 8>>)
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(conn, ping_frame)
}

// A client receiving a SETTINGS ACK as the first frame is PROTOCOL_ERROR
// because the server must send a non-ack SETTINGS first.
pub fn client_receives_settings_ack_as_first_frame_test() {
  let conn = new_connection(Client)
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(conn, settings_ack)
}

// A client receiving a valid SETTINGS frame as the first frame should succeed.
pub fn client_receives_valid_server_preface_test() {
  let conn = new_connection(Client)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(128),
    ])
  let assert Ok(#(conn, events, _to_send)) = receive_data(conn, settings_frame)
  let assert [RemoteSettingsChanged(_)] = events
  let assert option.Some(128) = conn.remote_settings.max_concurrent_streams
}

// ---------------------------------------------------------------------------
// A client MUST NOT expect to strip the 24-byte magic from the server.
// The server does NOT send the magic string. Only the client sends it.
// Verify that a client receiving the magic bytes treats it as an error
// (it would be unparseable as a frame -> connection error).
// ---------------------------------------------------------------------------
pub fn client_receiving_magic_bytes_is_error_test() {
  let conn = new_connection(Client)
  let assert Error(_) = receive_data(conn, client_preface_magic)
}

// ---------------------------------------------------------------------------
// RFC 9113 Section 3.4:
// "The server connection preface consists of a potentially empty SETTINGS
//  frame"
//
// After the preface is done, normal frame processing should continue.
// ---------------------------------------------------------------------------

// After a valid client preface, the server should process subsequent frames
// normally (e.g. a PING after the initial SETTINGS).
pub fn server_processes_frames_after_valid_preface_test() {
  let conn = new_connection(Server)
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [])
  let assert Ok(ping_frame) =
    h2_frame.encode_ping(ack: False, data: <<1, 2, 3, 4, 5, 6, 7, 8>>)
  let data = <<
    client_preface_magic:bits,
    settings_frame:bits,
    ping_frame:bits,
  >>
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, data)
  // Should have RemoteSettingsChanged from the SETTINGS frame
  // The PING is responded to with an ACK (no event emitted for received pings)
  let assert [RemoteSettingsChanged(_)] = events
  // to_send should contain: SETTINGS ACK + PING ACK
  let assert Ok(expected_settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Ok(expected_ping_ack) =
    h2_frame.encode_ping(ack: True, data: <<1, 2, 3, 4, 5, 6, 7, 8>>)
  assert to_send == <<expected_settings_ack:bits, expected_ping_ack:bits>>
}

// ---------------------------------------------------------------------------
// Edge case: empty data should not cause an error.
// ---------------------------------------------------------------------------
pub fn server_receives_empty_data_test() {
  let conn = new_connection(Server)
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, <<>>)
  assert events == []
  assert to_send == <<>>
}

pub fn client_receives_empty_data_test() {
  let conn = new_connection(Client)
  let assert Ok(#(_conn, events, to_send)) = receive_data(conn, <<>>)
  assert events == []
  assert to_send == <<>>
}
