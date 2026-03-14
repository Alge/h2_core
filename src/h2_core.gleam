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
  Setting(
    header_table_size: Int,
    enable_push: Bool,
    max_concurrent_streams: option.Option(Int),
    initial_window_size: Int,
    max_frame_size: Int,
    max_header_list_size: option.Option(Int),
  )
}

fn default_settings() -> Settings {
  Setting(
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
  SettingsChanged(settings: Settings)
  GoawayReceived(
    last_stream_id: Int,
    error_code: ErrorCode,
    debug_data: BitArray,
  )
  PingAcknowledged(data: BitArray)
}

pub type Connection {
  Connection(
    role: Role,
    local_settings: Settings,
    remote_settings: Settings,
    streams: dict.Dict(Int, Stream),
    next_stream_id: Int,
    recv_buffer: BitArray,
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
    remote_settings: default_settings(),
    streams: dict.new(),
    next_stream_id: next_stream_id,
    recv_buffer: <<>>,
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

fn add_stream(conn: Connection, stream: Stream) -> Connection {
  Connection(
    ..conn,
    next_stream_id: conn.next_stream_id + 2,
    streams: dict.insert(conn.streams, conn.next_stream_id, stream),
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

fn transition(stream: Stream, event: StreamEvent) -> Result(Stream, H2Error) {
  Ok(Stream(..stream, state: Open))
}

pub type Header {
  Header(name: String, value: String)
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
  connection: Connection,
  headers: List(Header),
  end_stream: Bool,
) -> Result(#(Connection, List(StreamEvent)), H2Error) {
  use stream <- result.try(transition(new_stream(), SendHeaders))

  Ok(#(add_stream(connection, stream), []))
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
