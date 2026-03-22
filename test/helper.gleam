import gleam/dict
import gleam/list
import gleam/option
import h2_core.{
  type Connection, type Header, Client, Connection, Header, Server, WithIndexing,
  open_stream, receive_data,
}
import h2_core/internal/stream.{type Stream, type StreamState, Stream}
import h2_frame

pub fn request_headers() -> List(Header) {
  [
    Header(":method", "GET", WithIndexing),
    Header(":scheme", "https", WithIndexing),
    Header(":path", "/", WithIndexing),
  ]
}

pub fn response_headers() -> List(Header) {
  [Header(":status", "200", WithIndexing)]
}

/// Create a connected client+server pair by completing the full preface exchange.
pub fn connected_pair() -> #(Connection, Connection) {
  let assert Ok(#(server, server_preface)) =
    h2_core.new_connection(Server, h2_core.default_settings())
  let assert Ok(#(client, client_preface)) =
    h2_core.new_connection(Client, h2_core.default_settings())

  // Each side receives the other's preface
  let assert Ok(#(server, _events, server_to_send)) =
    receive_data(server, client_preface)
  let assert Ok(#(client, _events, client_to_send)) =
    receive_data(client, server_preface)

  // Each side receives the SETTINGS ACK from the other
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, client_to_send)
  let assert Ok(#(client, _events, _to_send)) =
    receive_data(client, server_to_send)

  #(server, client)
}

/// Create a connected connection for a given role.
pub fn connected_connection(role: h2_core.Role) -> Connection {
  let #(server, client) = connected_pair()
  case role {
    Server -> server
    Client -> client
  }
}

/// Create a server and client with an open client-initiated stream 1.
pub fn server_with_open_stream() -> #(Connection, Connection) {
  let #(server, client) = connected_pair()
  let assert Ok(#(client, headers, _stream_id)) =
    open_stream(client, request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  #(server, client)
}

/// Create a server and client where stream 1 is half-closed (remote)
/// (client sent END_STREAM with headers).
pub fn server_with_half_closed_remote_stream() -> #(Connection, Connection) {
  let #(server, client) = connected_pair()
  let assert Ok(#(client, headers, _stream_id)) =
    open_stream(client, request_headers(), True)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, headers)
  #(server, client)
}

// TODO: Remove these once Connection is opaque (Step 6)
pub fn new_stream(state: StreamState) -> Stream {
  Stream(
    state: state,
    send_window_size: 65_535,
    recv_window_size: 65_535,
    headers_sent: False,
    final_response_received: False,
    expected_content_length: option.None,
    received_content_length: 0,
  )
}

pub fn set_stream_state(
  conn: Connection,
  stream_id: Int,
  state: StreamState,
) -> Connection {
  Connection(
    ..conn,
    streams: dict.insert(conn.streams, stream_id, new_stream(state)),
  )
}

/// Patch a single frame's stream ID without changing anything else.
pub fn patch_stream_id(frame_bytes: BitArray, new_id: Int) -> BitArray {
  let assert <<
    header:bits-size(40),
    _reserved:size(1),
    _old:size(31),
    payload:bits,
  >> = frame_bytes
  <<header:bits, 0:size(1), new_id:size(31), payload:bits>>
}

/// Patch the stream ID on every frame in a concatenated sequence.
pub fn patch_all_frames_stream_id(data: BitArray, new_id: Int) -> BitArray {
  patch_all_frames_loop(data, new_id, <<>>)
}

fn patch_all_frames_loop(data: BitArray, new_id: Int, acc: BitArray) -> BitArray {
  case h2_frame.extract_frame(data, 16_384) {
    Ok(#(frame_data, rest)) -> {
      let patched = patch_stream_id(frame_data, new_id)
      patch_all_frames_loop(rest, new_id, <<acc:bits, patched:bits>>)
    }
    Error(_) -> acc
  }
}

/// Parse all frames from a concatenated BitArray.
pub fn parse_all_frames(
  data: BitArray,
  acc: List(h2_frame.Frame),
) -> List(h2_frame.Frame) {
  case h2_frame.extract_frame(data, 16_384) {
    Ok(#(frame_data, rest)) -> {
      let assert Ok(frame) = h2_frame.decode_frame(frame_data)
      parse_all_frames(rest, list.append(acc, [frame]))
    }
    Error(_) -> acc
  }
}
