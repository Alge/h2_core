import gleam/list
import gleam/option
import h2_core.{
  Client, ConnectionError, FrameSizeError, InvalidSettings, ProtocolError,
  RemoteSettingsChanged, Server, SettingsAcknowledged, default_settings,
  get_connection_state, get_remote_settings, new_connection, receive_data,
}
import h2_core/internal.{AwaitingPreface, AwaitingSettings, Connected}
import h2_frame
import helper

// The client connection preface magic string (24 bytes)
const client_preface_magic = <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n":utf8>>

// =============================================================================
// Connection initial state — RFC 9113 Section 3.4
// =============================================================================

// RFC 9113 Section 3.4:
// "The client connection preface starts with a sequence of 24 octets [...]
//  This sequence MUST be followed by a SETTINGS frame"
//
// A server must first receive the 24-byte magic, so it starts in
// AwaitingPreface state.
pub fn server_starts_in_awaiting_preface_state_test() {
  let assert Ok(#(conn, _)) = new_connection(Server, default_settings())
  assert get_connection_state(conn) == AwaitingPreface
}

// RFC 9113 Section 3.4:
// "The server connection preface consists of a potentially empty SETTINGS
//  frame (Section 6.5) that MUST be the first frame the server sends in
//  the HTTP/2 connection."
//
// A client does not receive the 24-byte magic from the server — only a
// SETTINGS frame. So the client starts in AwaitingSettings state.
pub fn client_starts_in_awaiting_settings_state_test() {
  let assert Ok(#(conn, _)) = new_connection(Client, default_settings())
  assert get_connection_state(conn) == AwaitingSettings
}

// =============================================================================
// new_connection preface bytes — RFC 9113 Section 3.4
//
// "The client connection preface starts with a sequence of 24 octets [...]
//  This sequence MUST be followed by a SETTINGS frame"
// "The server connection preface consists of a potentially empty SETTINGS
//  frame (Section 6.5) that MUST be the first frame the server sends"
//
// new_connection/2 returns the preface bytes alongside the connection so
// the caller always has them without a separate call.
// =============================================================================

// Client preface bytes must start with the 24-byte magic string.
pub fn new_client_preface_starts_with_magic_test() {
  let assert Ok(#(_conn, preface)) = new_connection(Client, default_settings())
  let assert <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n":utf8, _rest:bits>> = preface
}

// The magic must be followed by a valid non-ack SETTINGS frame and nothing else.
pub fn new_client_preface_magic_followed_by_settings_test() {
  let assert Ok(#(_conn, preface)) = new_connection(Client, default_settings())
  let assert <<_magic:bytes-size(24), rest:bits>> = preface
  let assert Ok(#(frame_data, <<>>)) = h2_frame.extract_frame(rest, 16_384)
  let assert Ok(h2_frame.Settings(ack: False, settings: _)) =
    h2_frame.decode_frame(frame_data)
}

// Server preface bytes must be exactly one valid non-ack SETTINGS frame.
// The server does NOT send the client magic string.
pub fn new_server_preface_is_settings_frame_test() {
  let assert Ok(#(_conn, preface)) = new_connection(Server, default_settings())
  let assert Ok(#(frame_data, <<>>)) = h2_frame.extract_frame(preface, 16_384)
  let assert Ok(h2_frame.Settings(ack: False, settings: _)) =
    h2_frame.decode_frame(frame_data)
}

// Custom settings are encoded into the preface bytes.
// local_settings stays at defaults until the peer acknowledges with SETTINGS ACK.
pub fn new_connection_custom_settings_test() {
  let settings =
    h2_core.Settings(
      ..default_settings(),
      max_concurrent_streams: option.Some(42),
    )
  let assert Ok(#(_conn, preface)) = new_connection(Client, settings)
  // The preface SETTINGS frame must contain the custom value
  let assert <<_magic:bytes-size(24), rest:bits>> = preface
  let assert Ok(#(frame_data, <<>>)) = h2_frame.extract_frame(rest, 16_384)
  let assert Ok(h2_frame.Settings(ack: False, settings: params)) =
    h2_frame.decode_frame(frame_data)
  assert list.contains(params, h2_frame.MaxConcurrentStreams(42))
}

// Invalid settings (e.g. MaxFrameSize below the RFC minimum of 16384)
// must return an error.
pub fn new_connection_invalid_settings_returns_error_test() {
  let settings = h2_core.Settings(..default_settings(), max_frame_size: 1000)
  let assert Error(InvalidSettings) = new_connection(Client, settings)
}

// Round-trip: a server can receive_data the bytes from new_connection(Client).
pub fn server_receives_new_client_preface_test() {
  let assert Ok(#(server, _)) = new_connection(Server, default_settings())
  let assert Ok(#(_client, client_preface)) =
    new_connection(Client, default_settings())
  let assert Ok(#(server, [RemoteSettingsChanged(_)], _to_send)) =
    receive_data(server, client_preface)
  assert get_connection_state(server) == Connected
}

// Round-trip: a client can receive_data the bytes from new_connection(Server).
pub fn client_receives_new_server_preface_test() {
  let assert Ok(#(client, _)) = new_connection(Client, default_settings())
  let assert Ok(#(_server, server_preface)) =
    new_connection(Server, default_settings())
  let assert Ok(#(client, [RemoteSettingsChanged(_)], _to_send)) =
    receive_data(client, server_preface)
  assert get_connection_state(client) == Connected
}

// RFC 9113 Section 6.5.2:
// "A client MUST treat receipt of a SETTINGS frame with
//  SETTINGS_ENABLE_PUSH set to 1 as a connection error"
//
// This restriction applies to SETTINGS frames *received from the server*,
// not to the client's own pending settings being acknowledged via
// SETTINGS ACK. The client's default settings include ENABLE_PUSH=1
// (Section 6.5.2: "The initial value of SETTINGS_ENABLE_PUSH is 1"),
// and acknowledging those must not trigger a PROTOCOL_ERROR.
pub fn client_receives_settings_ack_after_preface_test() {
  let assert Ok(#(client, _)) = new_connection(Client, default_settings())
  let assert Ok(#(_server, server_preface)) =
    new_connection(Server, default_settings())
  let assert Ok(#(client, _events, _to_send)) =
    receive_data(client, server_preface)
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Ok(#(_client, [SettingsAcknowledged(_)], _to_send)) =
    receive_data(client, settings_ack)
}

// Same for server side — receiving a SETTINGS ACK after preface should work.
pub fn server_receives_settings_ack_after_preface_test() {
  let assert Ok(#(server, _)) = new_connection(Server, default_settings())
  let assert Ok(#(_client, client_preface)) =
    new_connection(Client, default_settings())
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, client_preface)
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Ok(#(_server, [SettingsAcknowledged(_)], _to_send)) =
    receive_data(server, settings_ack)
}

// =============================================================================
// Server: client preface magic — RFC 9113 Section 3.4
//
// "The client connection preface starts with a sequence of 24 octets,
//  which in hex notation is:
//    0x505249202a20485454502f322e300d0a0d0a534d0d0a0d0a
//  That is, the connection preface starts with the string
//  PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n). This sequence MUST be followed
//  by a SETTINGS frame (Section 6.5), which MAY be empty."
// =============================================================================

// A server receiving the correct client preface (magic + SETTINGS) should
// succeed and emit a RemoteSettingsChanged event for the peer's SETTINGS.
pub fn server_receives_valid_client_preface_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Server, default_settings())
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [])
  let data = <<client_preface_magic:bits, settings_frame:bits>>
  let assert Ok(#(conn, events, _to_send)) = receive_data(conn, data)
  let assert [RemoteSettingsChanged(_)] = events
  assert get_connection_state(conn) == Connected
}

// A server receiving the correct preface with non-empty SETTINGS should work.
pub fn server_receives_valid_preface_with_settings_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Server, default_settings())
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(100),
    ])
  let data = <<client_preface_magic:bits, settings_frame:bits>>
  let assert Ok(#(conn, events, _to_send)) = receive_data(conn, data)
  let assert [RemoteSettingsChanged(_)] = events
  let assert option.Some(100) = get_remote_settings(conn).max_concurrent_streams
  assert get_connection_state(conn) == Connected
}

// After receiving the magic but before receiving SETTINGS, the server
// should be in AwaitingSettings state.
pub fn server_transitions_to_awaiting_settings_after_magic_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Server, default_settings())
  let assert Ok(#(conn, events, to_send)) =
    receive_data(conn, client_preface_magic)
  assert events == []
  assert to_send == <<>>
  assert get_connection_state(conn) == AwaitingSettings
}

// =============================================================================
// Server: invalid preface — RFC 9113 Section 3.4
//
// "Clients and servers MUST treat an invalid connection preface as a
//  connection error (Section 5.4.1) of type PROTOCOL_ERROR."
// =============================================================================

// A server receiving garbage bytes instead of the client preface magic
// MUST result in a connection error of type PROTOCOL_ERROR.
pub fn server_receives_invalid_preface_bytes_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Server, default_settings())
  let garbage = <<"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n":utf8>>
  let assert Error(ConnectionError(ProtocolError)) = receive_data(conn, garbage)
}

// A server receiving partially correct magic followed by wrong bytes
// MUST result in PROTOCOL_ERROR.
pub fn server_receives_corrupted_preface_magic_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Server, default_settings())
  // First 10 bytes correct, then garbage
  let corrupted = <<"PRI * HTTP/XXXXXXXXXXXXXX":utf8>>
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(conn, corrupted)
}

// =============================================================================
// Server: first frame after magic MUST be SETTINGS — RFC 9113 Section 3.4
//
// "This sequence MUST be followed by a SETTINGS frame"
// =============================================================================

// Client preface magic followed by a non-SETTINGS frame (e.g. PING)
// MUST be a PROTOCOL_ERROR.
pub fn server_receives_preface_magic_followed_by_non_settings_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Server, default_settings())
  let assert Ok(ping_frame) =
    h2_frame.encode_ping(ack: False, data: <<1, 2, 3, 4, 5, 6, 7, 8>>)
  let data = <<client_preface_magic:bits, ping_frame:bits>>
  let assert Error(ConnectionError(ProtocolError)) = receive_data(conn, data)
}

// Client preface magic followed by a SETTINGS ACK (not a non-ack SETTINGS)
// MUST be a PROTOCOL_ERROR because the first frame must be a SETTINGS
// (non-ack) frame.
pub fn server_receives_preface_magic_followed_by_settings_ack_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Server, default_settings())
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let data = <<client_preface_magic:bits, settings_ack:bits>>
  let assert Error(ConnectionError(ProtocolError)) = receive_data(conn, data)
}

// Client preface magic followed by HEADERS is PROTOCOL_ERROR.
pub fn server_receives_preface_magic_followed_by_headers_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Server, default_settings())
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers_frame, _stream_id)) =
    h2_core.open_stream(client, helper.request_headers(), False)
  let data = <<client_preface_magic:bits, headers_frame:bits>>
  let assert Error(ConnectionError(ProtocolError)) = receive_data(conn, data)
}

// =============================================================================
// Server: partial preface delivery
//
// Incomplete data should not error — the connection should buffer and
// wait for more data.
// =============================================================================

// Server receives only the 24-byte magic without a SETTINGS frame yet.
// This is incomplete, not an error — the server should wait for more data.
pub fn server_receives_only_preface_magic_waits_for_settings_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Server, default_settings())
  let assert Ok(#(conn, events, to_send)) =
    receive_data(conn, client_preface_magic)
  assert events == []
  assert to_send == <<>>
  assert get_connection_state(conn) == AwaitingSettings
  // Now send the SETTINGS frame
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [])
  let assert Ok(#(conn, events, _to_send)) = receive_data(conn, settings_frame)
  let assert [RemoteSettingsChanged(_)] = events
  assert get_connection_state(conn) == Connected
}

// Server receives the magic in two separate chunks (split mid-magic).
// Both chunks are individually incomplete — the connection should buffer.
pub fn server_receives_preface_magic_in_chunks_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Server, default_settings())
  // Split the 24-byte magic into two parts
  let assert <<part1:bytes-size(12), part2:bytes>> = client_preface_magic
  let assert Ok(#(conn, events, to_send)) = receive_data(conn, part1)
  assert events == []
  assert to_send == <<>>
  assert get_connection_state(conn) == AwaitingPreface
  // Send the rest of the magic + SETTINGS
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [])
  let data = <<part2:bits, settings_frame:bits>>
  let assert Ok(#(conn, events, _to_send)) = receive_data(conn, data)
  let assert [RemoteSettingsChanged(_)] = events
  assert get_connection_state(conn) == Connected
}

// Server receives magic byte-by-byte, then SETTINGS in a final chunk.
pub fn server_receives_preface_magic_byte_by_byte_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Server, default_settings())
  let assert <<
    b1,
    b2,
    b3,
    b4,
    b5,
    b6,
    b7,
    b8,
    b9,
    b10,
    b11,
    b12,
    b13,
    b14,
    b15,
    b16,
    b17,
    b18,
    b19,
    b20,
    b21,
    b22,
    b23,
    b24,
  >> = client_preface_magic
  let bytes = [
    b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17,
    b18, b19, b20, b21, b22, b23, b24,
  ]
  let conn = feed_bytes(conn, bytes)
  assert get_connection_state(conn) == AwaitingSettings
  // Now send SETTINGS
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [])
  let assert Ok(#(conn, events, _to_send)) = receive_data(conn, settings_frame)
  let assert [RemoteSettingsChanged(_)] = events
  assert get_connection_state(conn) == Connected
}

fn feed_bytes(conn, bytes) {
  case bytes {
    [] -> conn
    [byte, ..rest] -> {
      let assert Ok(#(conn, [], <<>>)) = receive_data(conn, <<byte>>)
      feed_bytes(conn, rest)
    }
  }
}

// =============================================================================
// Client: first frame MUST be SETTINGS — RFC 9113 Section 3.4
//
// "The server connection preface consists of a potentially empty SETTINGS
//  frame (Section 6.5) that MUST be the first frame the server sends in
//  the HTTP/2 connection."
//
// From the client's perspective, the first frame received from the server
// MUST be a non-ack SETTINGS frame.
// =============================================================================

// A client receiving a valid SETTINGS frame as the first frame should succeed
// and transition to Open state.
pub fn client_receives_valid_server_preface_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Client, default_settings())
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [
      h2_frame.MaxConcurrentStreams(128),
    ])
  let assert Ok(#(conn, events, _to_send)) = receive_data(conn, settings_frame)
  let assert [RemoteSettingsChanged(_)] = events
  let assert option.Some(128) = get_remote_settings(conn).max_concurrent_streams
  assert get_connection_state(conn) == Connected
}

// A client receiving a non-SETTINGS frame as the first frame from the server
// MUST treat it as PROTOCOL_ERROR.
pub fn client_receives_non_settings_as_first_frame_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Client, default_settings())
  let assert Ok(ping_frame) =
    h2_frame.encode_ping(ack: False, data: <<1, 2, 3, 4, 5, 6, 7, 8>>)
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(conn, ping_frame)
}

// A client receiving a SETTINGS ACK as the first frame is PROTOCOL_ERROR
// because the server must send a non-ack SETTINGS first.
pub fn client_receives_settings_ack_as_first_frame_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Client, default_settings())
  let assert Ok(settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(conn, settings_ack)
}

// A client receiving GOAWAY as the first frame is PROTOCOL_ERROR.
pub fn client_receives_goaway_as_first_frame_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Client, default_settings())
  let goaway_frame =
    h2_frame.encode_goaway(
      last_stream_id: 0,
      error_code: h2_frame.NoError,
      debug_data: <<>>,
    )
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(conn, goaway_frame)
}

// A client receiving WINDOW_UPDATE as the first frame is PROTOCOL_ERROR.
pub fn client_receives_window_update_as_first_frame_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Client, default_settings())
  let assert Ok(wu_frame) =
    h2_frame.encode_window_update(stream_id: 0, window_size_increment: 1024)
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(conn, wu_frame)
}

// =============================================================================
// Client: magic bytes from server
//
// The server does NOT send the 24-byte magic string — only the client does.
// A client receiving magic bytes treats it as a connection error (it would
// be unparseable as a valid frame).
// =============================================================================

pub fn client_receiving_magic_bytes_is_error_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Client, default_settings())
  let assert Error(ConnectionError(FrameSizeError)) =
    receive_data(conn, client_preface_magic)
}

// =============================================================================
// Post-preface normal processing — RFC 9113 Section 3.4
//
// After the preface is complete, normal frame processing should continue.
// =============================================================================

// After a valid client preface, the server should process subsequent frames
// normally (e.g. a PING after the initial SETTINGS).
pub fn server_processes_frames_after_valid_preface_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Server, default_settings())
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [])
  let assert Ok(ping_frame) =
    h2_frame.encode_ping(ack: False, data: <<1, 2, 3, 4, 5, 6, 7, 8>>)
  let data = <<
    client_preface_magic:bits,
    settings_frame:bits,
    ping_frame:bits,
  >>
  let assert Ok(#(conn, events, to_send)) = receive_data(conn, data)
  // Should have RemoteSettingsChanged from the SETTINGS frame
  let assert [RemoteSettingsChanged(_)] = events
  assert get_connection_state(conn) == Connected
  // to_send should contain: SETTINGS ACK + PING ACK
  let assert Ok(expected_settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Ok(expected_ping_ack) =
    h2_frame.encode_ping(ack: True, data: <<1, 2, 3, 4, 5, 6, 7, 8>>)
  assert to_send == <<expected_settings_ack:bits, expected_ping_ack:bits>>
}

// After a valid server preface, the client should process subsequent frames
// normally.
pub fn client_processes_frames_after_valid_preface_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Client, default_settings())
  let assert Ok(settings_frame) =
    h2_frame.encode_settings(ack: False, settings: [])
  let assert Ok(ping_frame) =
    h2_frame.encode_ping(ack: False, data: <<1, 2, 3, 4, 5, 6, 7, 8>>)
  let data = <<settings_frame:bits, ping_frame:bits>>
  let assert Ok(#(conn, events, to_send)) = receive_data(conn, data)
  let assert [RemoteSettingsChanged(_)] = events
  assert get_connection_state(conn) == Connected
  let assert Ok(expected_settings_ack) =
    h2_frame.encode_settings(ack: True, settings: [])
  let assert Ok(expected_ping_ack) =
    h2_frame.encode_ping(ack: True, data: <<1, 2, 3, 4, 5, 6, 7, 8>>)
  assert to_send == <<expected_settings_ack:bits, expected_ping_ack:bits>>
}

// =============================================================================
// Edge cases
// =============================================================================

// Empty data should not cause an error regardless of connection state.
pub fn server_receives_empty_data_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Server, default_settings())
  let assert Ok(#(conn, events, to_send)) = receive_data(conn, <<>>)
  assert events == []
  assert to_send == <<>>
  assert get_connection_state(conn) == AwaitingPreface
}

pub fn client_receives_empty_data_test() {
  let assert Ok(#(conn, _preface)) = new_connection(Client, default_settings())
  let assert Ok(#(conn, events, to_send)) = receive_data(conn, <<>>)
  assert events == []
  assert to_send == <<>>
  assert get_connection_state(conn) == AwaitingSettings
}
