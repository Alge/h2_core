import alpacki
import gleam/bool
import gleam/bytes_tree
import gleam/dict
import gleam/list
import gleam/option
import gleam/result
import h2_frame.{type ErrorCode}

pub type Role {
  Client
  Server
}

pub type Settings {
  Settings(
    header_table_size: Int,
    enable_push: Bool,
    max_concurrent_streams: option.Option(Int),
    initial_window_size: Int,
    max_frame_size: Int,
    max_header_list_size: option.Option(Int),
  )
}

fn apply_settings(
  role: Role,
  settings: Settings,
  new: List(h2_frame.Setting),
) -> Result(Settings, H2Error) {
  case new {
    [] -> Ok(settings)
    [h2_frame.HeaderTableSize(value), ..rest] ->
      apply_settings(role, Settings(..settings, header_table_size: value), rest)
    [h2_frame.EnablePush(value), ..rest] -> {
      case value {
        0 ->
          apply_settings(role, Settings(..settings, enable_push: False), rest)
        1 -> {
          case role {
            Server ->
              apply_settings(
                role,
                Settings(..settings, enable_push: True),
                rest,
              )
            // Client should never receive enable_push == 1
            Client -> Error(ConnectionError(h2_frame.ProtocolError))
          }
        }
        _ -> Error(ConnectionError(h2_frame.ProtocolError))
      }
    }
    [h2_frame.MaxConcurrentStreams(value), ..rest] ->
      apply_settings(
        role,
        Settings(..settings, max_concurrent_streams: option.Some(value)),
        rest,
      )
    [h2_frame.InitialWindowSize(value), ..rest] -> {
      use <- bool.guard(
        value > 2_147_483_647,
        Error(ConnectionError(h2_frame.FlowControlError)),
      )
      apply_settings(
        role,
        Settings(..settings, initial_window_size: value),
        rest,
      )
    }
    [h2_frame.MaxFrameSize(value), ..rest] -> {
      use <- bool.guard(
        value < 16_384,
        Error(ConnectionError(h2_frame.ProtocolError)),
      )
      use <- bool.guard(
        value > 16_777_215,
        Error(ConnectionError(h2_frame.ProtocolError)),
      )
      apply_settings(role, Settings(..settings, max_frame_size: value), rest)
    }
    [h2_frame.MaxHeaderListSize(value), ..rest] ->
      apply_settings(
        role,
        Settings(..settings, max_header_list_size: option.Some(value)),
        rest,
      )

    // Ignore unknown settings
    [h2_frame.UnknownSetting(_, _), ..rest] ->
      apply_settings(role, settings, rest)
  }
}

fn default_settings() -> Settings {
  Settings(
    header_table_size: 4096,
    enable_push: True,
    max_concurrent_streams: option.None,
    initial_window_size: 65_535,
    max_frame_size: 16_384,
    max_header_list_size: option.None,
  )
}

pub type Event {
  HeadersReceived(stream_id: Int, headers: List(Header), end_stream: Bool)
  DataReceived(stream_id: Int, data: BitArray, end_stream: Bool)
  StreamReset(stream_id: Int, error_code: ErrorCode)
  StreamEnded(stream_id: Int)
  PushPromiseReceived(
    stream_id: Int,
    promised_stream_id: Int,
    headers: List(Header),
  )
  SettingsAcknowledged(settings: Settings)
  RemoteSettingsChanged(settings: Settings)
  GoawayReceived(
    last_stream_id: Int,
    error_code: ErrorCode,
    debug_data: BitArray,
  )
  PingAcknowledged(data: BitArray)
}

pub opaque type HpackContext {
  HpackContext(table: alpacki.DynamicTable)
}

pub type Connection {
  Connection(
    role: Role,
    local_settings: Settings,
    pending_settings: List(List(h2_frame.Setting)),
    remote_settings: Settings,
    streams: dict.Dict(Int, Stream),
    last_remote_stream_id: Int,
    next_stream_id: Int,
    recv_buffer: BitArray,
    send_window_size: Int,
    recv_window_size: Int,
    hpack_encoder: HpackContext,
    hpack_decoder: HpackContext,
  )
}

pub fn new_connection(role: Role) -> Connection {
  let next_stream_id = case role {
    Client -> 1
    Server -> 2
  }

  Connection(
    role: role,
    local_settings: default_settings(),
    pending_settings: [],
    remote_settings: default_settings(),
    streams: dict.new(),
    last_remote_stream_id: 0,
    next_stream_id: next_stream_id,
    recv_buffer: <<>>,
    send_window_size: 65_535,
    recv_window_size: 65_535,
    hpack_encoder: HpackContext(alpacki.new_dynamic(4096)),
    hpack_decoder: HpackContext(alpacki.new_dynamic(4096)),
  )
}

pub type StreamState {
  Idle
  ReservedLocal
  ReservedRemote
  Open
  HalfClosedLocal
  HalfClosedRemote
  Closed
}

pub type Stream {
  Stream(state: StreamState)
}

fn new_stream() -> Stream {
  Stream(state: Idle)
}

fn add_stream(conn: Connection, stream: Stream) -> #(Connection, Int) {
  #(
    Connection(
      ..conn,
      next_stream_id: conn.next_stream_id + 2,
      streams: dict.insert(conn.streams, conn.next_stream_id, stream),
    ),
    conn.next_stream_id,
  )
}

pub type StreamEvent {
  SendHeaders
  RecvHeaders
  SendEndStream
  RecvEndStream
  SendRstStream
  RecvRstStream
  SendPushPromise
  RecvPushPromise
}

fn transition(_stream: Stream, _event: StreamEvent) -> Result(Stream, H2Error) {
  Ok(Stream(state: Open))
  //Ok(Stream(..stream, state: Open))
}

pub type Indexing {
  WithIndexing
  WithoutIndexing
  NeverIndexed
}

pub type Header {
  Header(name: String, value: String, indexing: Indexing)
}

fn to_alpacki_header(header: Header) -> alpacki.HeaderField {
  let alpacki_indexing = case header.indexing {
    WithIndexing -> alpacki.WithIndexing
    WithoutIndexing -> alpacki.WithoutIndexing
    NeverIndexed -> alpacki.NeverIndexed
  }
  alpacki.HeaderField(
    name: header.name,
    value: header.value,
    indexing: alpacki_indexing,
  )
}

fn from_alpacki_header(header: alpacki.HeaderField) -> Header {
  let indexing = case header.indexing {
    alpacki.WithIndexing -> WithIndexing
    alpacki.WithoutIndexing -> WithoutIndexing
    alpacki.NeverIndexed -> NeverIndexed
  }
  Header(name: header.name, value: header.value, indexing: indexing)
}

pub type H2Error {
  ConnectionError(error_code: h2_frame.ErrorCode)
  StreamError(stream_id: Int, error_code: h2_frame.ErrorCode)
}

fn map_frame_error(error: h2_frame.FrameError) -> H2Error {
  case error {
    h2_frame.ConnectionError(code) -> ConnectionError(code)
    h2_frame.StreamError(id, code) -> StreamError(id, code)
    h2_frame.InvalidPadding -> ConnectionError(h2_frame.ProtocolError)
    h2_frame.Incomplete -> ConnectionError(h2_frame.InternalError)
  }
}

pub fn send_headers(
  conn: Connection,
  headers: List(Header),
  end_stream: Bool,
) -> Result(#(Connection, List(StreamEvent), BitArray), H2Error) {
  let stream = case end_stream {
    True -> Stream(..new_stream(), state: HalfClosedLocal)
    False -> Stream(..new_stream(), state: Open)
  }

  let #(conn, stream_id) = add_stream(conn, stream)

  // Map headers to alpacki headers for encoding
  let headers = list.map(headers, to_alpacki_header)

  let #(encoded_headers, new_table) =
    alpacki.encode_header_block(
      headers,
      conn.hpack_encoder.table,
      huffman: True,
    )

  // Add the new table to the conn
  let conn = Connection(..conn, hpack_encoder: HpackContext(table: new_table))

  case
    h2_frame.encode_headers(
      stream_id: stream_id,
      end_stream: end_stream,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bytes_tree.to_bit_array(encoded_headers),
      padding: option.None,
    )
  {
    Ok(encoded_frame) -> Ok(#(conn, [], encoded_frame))
    Error(error) -> Error(map_frame_error(error))
  }
}

pub fn send_settings(
  conn: Connection,
  settings: List(h2_frame.Setting),
) -> Result(#(Connection, List(StreamEvent), BitArray), H2Error) {
  let conn =
    Connection(
      ..conn,
      pending_settings: list.append(conn.pending_settings, [settings]),
    )
  case h2_frame.encode_settings(ack: False, settings: settings) {
    Ok(encoded) -> {
      Ok(#(conn, [], encoded))
    }
    Error(error) -> Error(map_frame_error(error))
  }
}

pub fn send_ping(
  conn: Connection,
  data: BitArray,
) -> Result(#(Connection, List(StreamEvent), BitArray), H2Error) {
  case h2_frame.encode_ping(ack: False, data: data) {
    Ok(encoded) -> {
      Ok(#(conn, [], encoded))
    }
    Error(error) -> Error(map_frame_error(error))
  }
}

pub fn send_goaway(
  conn: Connection,
  error_code: h2_frame.ErrorCode,
  debug_data: BitArray,
) -> Result(#(Connection, List(StreamEvent), BitArray), H2Error) {
  let encoded_frame =
    h2_frame.encode_goaway(
      last_stream_id: conn.last_remote_stream_id,
      error_code: error_code,
      debug_data: debug_data,
    )
  Ok(#(conn, [], encoded_frame))
}

pub fn send_window_update(
  conn: Connection,
  stream_id: Int,
  window_size_increment: Int,
) -> Result(#(Connection, List(StreamEvent), BitArray), H2Error) {
  // Update the connection

  let conn = case stream_id {
    0 ->
      Connection(
        ..conn,
        recv_window_size: conn.recv_window_size + window_size_increment,
      )
    // TODO: Handle updating window size on stream
    _ -> conn
  }

  use <- bool.guard(
    stream_id == 0 && conn.recv_window_size > 2_147_483_647,
    Error(ConnectionError(h2_frame.FlowControlError)),
  )

  case
    h2_frame.encode_window_update(
      stream_id: stream_id,
      window_size_increment: window_size_increment,
    )
  {
    Ok(encoded_frame) -> Ok(#(conn, [], encoded_frame))
    Error(error) -> Error(map_frame_error(error))
  }
}

pub fn send_rst_stream(
  conn: Connection,
  stream_id: Int,
  error_code: h2_frame.ErrorCode,
) -> Result(#(Connection, List(StreamEvent), BitArray), H2Error) {
  // Must be sent on a existing stream
  use stream <- result.try(
    dict.get(conn.streams, stream_id)
    |> result.replace_error(ConnectionError(h2_frame.ProtocolError)),
  )

  // Must not be sent on a idle stream
  use <- bool.guard(
    stream.state == Idle,
    Error(ConnectionError(h2_frame.ProtocolError)),
  )

  case
    h2_frame.encode_rst_stream(stream_id: stream_id, error_code: error_code)
  {
    Ok(encoded_frame) -> Ok(#(conn, [], encoded_frame))
    Error(error) -> Error(map_frame_error(error))
  }
}

fn parse_loop(
  conn: Connection,
  events: List(Event),
  to_send: BitArray,
) -> Result(#(Connection, List(Event), BitArray), H2Error) {
  case h2_frame.parse(conn.recv_buffer) {
    Ok(#(frame, rest)) -> {
      let conn = Connection(..conn, recv_buffer: rest)

      case frame {
        // Pings
        h2_frame.Ping(ack: False, data: data) -> {
          // Generate the Ping ack and put it on the to_send buffer
          case h2_frame.encode_ping(ack: True, data: data) {
            Ok(response) ->
              parse_loop(conn, events, <<to_send:bits, response:bits>>)
            Error(error) -> Error(map_frame_error(error))
          }
        }
        h2_frame.Ping(ack: True, data: data) -> {
          // Do nothing, this was the ack
          parse_loop(conn, [PingAcknowledged(data: data), ..events], to_send)
        }

        // Settings
        h2_frame.Settings(ack: False, settings: settings) -> {
          // Apply these settings to the remote settings

          case apply_settings(conn.role, conn.remote_settings, settings) {
            Ok(new_settings) -> {
              let conn = Connection(..conn, remote_settings: new_settings)

              // Reply with an ACK
              case h2_frame.encode_settings(ack: True, settings: []) {
                Ok(response) ->
                  parse_loop(
                    conn,
                    [RemoteSettingsChanged(conn.remote_settings), ..events],
                    <<to_send:bits, response:bits>>,
                  )
                Error(error) -> Error(map_frame_error(error))
              }
            }

            Error(error) -> Error(error)
          }
        }

        h2_frame.Settings(ack: True, settings: _) -> {
          case conn.pending_settings {
            [settings, ..rest] -> {
              case apply_settings(conn.role, conn.local_settings, settings) {
                // Apply settings
                Ok(new_settings) -> {
                  let conn =
                    Connection(
                      ..conn,
                      local_settings: new_settings,
                      pending_settings: rest,
                    )
                  parse_loop(
                    conn,
                    [SettingsAcknowledged(conn.local_settings), ..events],
                    to_send,
                  )
                }
                Error(error) -> Error(error)
              }
            }
            [] -> {
              Error(ConnectionError(h2_frame.ProtocolError))
            }
          }
        }

        // Goaway
        h2_frame.Goaway(last_stream_id, error_code, debug_data) -> {
          parse_loop(
            conn,
            [
              GoawayReceived(
                last_stream_id: last_stream_id,
                error_code: error_code,
                debug_data: debug_data,
              ),
              ..events
            ],
            to_send,
          )
        }

        // WINDOW_UPDATE
        h2_frame.WindowUpdate(stream_id, window_size_increment) -> {
          case stream_id {
            0 -> {
              let conn =
                Connection(
                  ..conn,
                  send_window_size: conn.send_window_size
                    + window_size_increment,
                )
              use <- bool.guard(
                conn.send_window_size > 2_147_483_647,
                Error(ConnectionError(h2_frame.FlowControlError)),
              )
              parse_loop(conn, events, to_send)
            }
            stream_id -> {
              // Handle WindowUpdate on specific stream
              todo
            }
          }
        }
        // RST_STREAM
        h2_frame.RstStream(stream_id, error_code) -> {
          use stream <- result.try(
            dict.get(conn.streams, stream_id)
            |> result.replace_error(ConnectionError(h2_frame.ProtocolError)),
          )

          let stream = Stream(..stream, state: Closed)

          let conn =
            Connection(
              ..conn,
              streams: dict.insert(conn.streams, stream_id, stream),
            )

          Ok(#(
            conn,
            [
              StreamReset(stream_id: stream_id, error_code: error_code),
              ..events
            ],
            to_send,
          ))
        }
        h2_frame.Headers(
          stream_id,
          end_stream,
          _end_headers,
          _priority,
          field_block_fragment,
        ) -> {
          use <- bool.guard(
            stream_id <= conn.last_remote_stream_id,
            Error(ConnectionError(h2_frame.ProtocolError)),
          )

          use #(decoded_headers, new_table) <- result.try(
            alpacki.decode_header_block(
              field_block_fragment,
              conn.hpack_decoder.table,
            )
            |> result.replace_error(ConnectionError(h2_frame.CompressionError)),
          )

          let decoded_headers = list.map(decoded_headers, from_alpacki_header)
          let conn =
            Connection(..conn, hpack_decoder: HpackContext(table: new_table))

          // Create new stream
          let stream = case end_stream {
            False -> Stream(..new_stream(), state: Open)
            True -> Stream(..new_stream(), state: HalfClosedRemote)
          }

          let conn =
            Connection(
              ..conn,
              last_remote_stream_id: stream_id,
              streams: dict.insert(conn.streams, stream_id, stream),
            )

          parse_loop(
            conn,
            [
              HeadersReceived(
                stream_id: stream_id,
                headers: decoded_headers,
                end_stream: end_stream,
              ),
              ..events
            ],
            to_send,
          )
        }
        _ -> todo
      }
    }
    Error(h2_frame.Incomplete) -> Ok(#(conn, list.reverse(events), to_send))
    Error(error) -> Error(map_frame_error(error))
  }
}

pub fn receive_data(
  conn: Connection,
  data: BitArray,
) -> Result(#(Connection, List(Event), BitArray), H2Error) {
  let conn =
    Connection(..conn, recv_buffer: <<conn.recv_buffer:bits, data:bits>>)

  case parse_loop(conn, [], <<>>) {
    Ok(#(conn, events, to_send)) -> {
      Ok(#(conn, events, to_send))
    }

    Error(error) -> Error(error)
  }
}
