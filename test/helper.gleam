import gleam/list
import h2_core.{
  type Connection, type Header, Client, Header, Server, WithIndexing,
  open_stream, receive_data,
}
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

/// Server with stream 1 half-closed(local): server has sent END_STREAM.
pub fn server_with_half_closed_local_stream() -> #(Connection, Connection) {
  let #(server, client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    h2_core.send_headers(server, 1, response_headers(), True)
  #(server, client)
}

/// Server with stream 1 closed via RST_STREAM.
pub fn server_with_closed_stream() -> #(Connection, Connection) {
  let #(server, client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    h2_core.send_rst_stream(server, 1, h2_core.NoError)
  #(server, client)
}

/// Server with a ReservedLocal stream (via PUSH_PROMISE).
/// Returns #(server, client, promised_stream_id).
pub fn server_with_reserved_local_stream() -> #(Connection, Connection, Int) {
  let #(server, client) = server_with_open_stream()
  let push_headers = [
    Header(":method", "GET", WithIndexing),
    Header(":scheme", "https", WithIndexing),
    Header(":path", "/pushed", WithIndexing),
  ]
  let assert Ok(#(server, _to_send, promised_id)) =
    h2_core.send_push_promise(server, 1, push_headers)
  #(server, client, promised_id)
}

/// Client with a ReservedRemote stream (received PUSH_PROMISE from server).
/// Returns #(server, client, promised_stream_id).
pub fn client_with_reserved_remote_stream() -> #(Connection, Connection, Int) {
  let #(server, client) = server_with_open_stream()
  let push_headers = [
    Header(":method", "GET", WithIndexing),
    Header(":scheme", "https", WithIndexing),
    Header(":path", "/pushed", WithIndexing),
  ]
  let assert Ok(#(server, push_bytes, promised_id)) =
    h2_core.send_push_promise(server, 1, push_headers)
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, push_bytes)
  #(server, client, promised_id)
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
