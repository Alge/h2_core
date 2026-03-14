import gleam/dict
import gleam/option
import gleam/result
import h2_frame

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

pub type Connection {
  Connection(
    role: Role,
    local_settings: Settings,
    remote_settings: Settings,
    streams: dict.Dict(Int, Stream),
    next_stream_id: Int,
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

pub fn send_headers(
  connection: Connection,
  headers: List(Header),
  end_stream: Bool,
) -> Result(#(Connection), H2Error) {
  use stream <- result.try(transition(Stream(state: Idle), SendHeaders))

  Ok(#(add_stream(connection, stream)))
}
