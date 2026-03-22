import alpacki
import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/dict
import gleam/int
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import h2_core/internal/stream.{
  type Stream, Closed, HalfClosedLocal, HalfClosedRemote, Idle, Open,
  ReservedLocal, ReservedRemote, Stream, new_stream,
}
import h2_frame

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

fn validate_settings(settings: Settings) -> Result(Nil, H2Error) {
  use <- bool.guard(
    settings.initial_window_size < 0
      || settings.initial_window_size > 2_147_483_647,
    Error(ConnectionError(FlowControlError)),
  )
  use <- bool.guard(
    settings.max_frame_size < 16_384 || settings.max_frame_size > 16_777_215,
    Error(ConnectionError(ProtocolError)),
  )

  use <- bool.guard(
    settings.header_table_size < 0,
    Error(ConnectionError(ProtocolError)),
  )

  use <- bool.guard(
    settings.max_concurrent_streams
      |> option.map(fn(v) { v < 0 })
      |> option.unwrap(False),
    Error(ConnectionError(ProtocolError)),
  )
  use <- bool.guard(
    settings.max_header_list_size
      |> option.map(fn(v) { v < 0 })
      |> option.unwrap(False),
    Error(ConnectionError(ProtocolError)),
  )
  Ok(Nil)
}

/// Helper function to convert a settings object to a list of h2_frame.Setting
/// Useful when sending the current settings as a frame
fn to_settings_list(
  settings settings: Settings,
  role role: Role,
) -> List(Setting) {
  let settings_list = [
    HeaderTableSize(settings.header_table_size),
    InitialWindowSize(settings.initial_window_size),
    MaxFrameSize(settings.max_frame_size),
  ]
  let settings_list = case role {
    Client -> [
      EnablePush(case settings.enable_push {
        True -> 1
        False -> 0
      }),
      ..settings_list
    ]
    Server -> settings_list
  }

  let settings_list = case settings.max_concurrent_streams {
    option.Some(value) -> {
      [MaxConcurrentStreams(value), ..settings_list]
    }
    option.None -> settings_list
  }

  let settings_list = case settings.max_header_list_size {
    option.Some(value) -> {
      [MaxHeaderListSize(value), ..settings_list]
    }
    option.None -> settings_list
  }

  settings_list
}

fn apply_send_new_window_size(
  conn: Connection,
  delta: Int,
) -> Result(Connection, H2Error) {
  // Early return if delta is 0
  use <- bool.guard(delta == 0, Ok(conn))

  use streams <- result.try(
    list.try_map(dict.to_list(conn.streams), fn(pair) {
      let #(stream_id, stream) = pair
      let stream =
        Stream(..stream, send_window_size: stream.send_window_size + delta)
      use <- bool.guard(
        stream.send_window_size > 2_147_483_647,
        Error(ConnectionError(FlowControlError)),
      )
      Ok(#(stream_id, stream))
    }),
  )
  Ok(Connection(..conn, streams: dict.from_list(streams)))
}

fn apply_recv_new_window_size(
  conn: Connection,
  delta: Int,
) -> Result(Connection, H2Error) {
  // Early return if delta is 0
  use <- bool.guard(delta == 0, Ok(conn))

  use streams <- result.try(
    list.try_map(dict.to_list(conn.streams), fn(pair) {
      let #(stream_id, stream) = pair
      let stream =
        Stream(..stream, recv_window_size: stream.recv_window_size + delta)
      use <- bool.guard(
        stream.recv_window_size > 2_147_483_647,
        Error(ConnectionError(FlowControlError)),
      )
      Ok(#(stream_id, stream))
    }),
  )
  Ok(Connection(..conn, streams: dict.from_list(streams)))
}

fn from_frame_setting(setting: h2_frame.Setting) -> Result(Setting, Nil) {
  case setting {
    h2_frame.HeaderTableSize(v) -> Ok(HeaderTableSize(v))
    h2_frame.EnablePush(v) -> Ok(EnablePush(v))
    h2_frame.MaxConcurrentStreams(v) -> Ok(MaxConcurrentStreams(v))
    h2_frame.InitialWindowSize(v) -> Ok(InitialWindowSize(v))
    h2_frame.MaxFrameSize(v) -> Ok(MaxFrameSize(v))
    h2_frame.MaxHeaderListSize(v) -> Ok(MaxHeaderListSize(v))
    h2_frame.UnknownSetting(_, _) -> Error(Nil)
  }
}

fn from_frame_settings(settings: List(h2_frame.Setting)) -> List(Setting) {
  list.filter_map(settings, from_frame_setting)
}

fn apply_settings(
  role: Role,
  settings: Settings,
  new: List(Setting),
  remote remote: Bool,
) -> Result(Settings, H2Error) {
  case new {
    [] -> Ok(settings)
    [HeaderTableSize(value), ..rest] ->
      apply_settings(
        role,
        Settings(..settings, header_table_size: value),
        rest,
        remote:,
      )
    [EnablePush(value), ..rest] -> {
      case value {
        0 ->
          apply_settings(
            role,
            Settings(..settings, enable_push: False),
            rest,
            remote:,
          )
        1 -> {
          // RFC 9113 Section 6.5.2: "A client MUST treat receipt of a
          // SETTINGS frame with SETTINGS_ENABLE_PUSH set to 1 as a
          // connection error of type PROTOCOL_ERROR."
          // This only applies to received (remote) settings.
          case remote && role == Client {
            True -> Error(ConnectionError(ProtocolError))
            False ->
              apply_settings(
                role,
                Settings(..settings, enable_push: True),
                rest,
                remote:,
              )
          }
        }
        _ -> Error(ConnectionError(ProtocolError))
      }
    }
    [MaxConcurrentStreams(value), ..rest] ->
      apply_settings(
        role,
        Settings(..settings, max_concurrent_streams: option.Some(value)),
        rest,
        remote:,
      )
    [InitialWindowSize(value), ..rest] -> {
      use <- bool.guard(
        value > 2_147_483_647,
        Error(ConnectionError(FlowControlError)),
      )
      apply_settings(
        role,
        Settings(..settings, initial_window_size: value),
        rest,
        remote:,
      )
    }
    [MaxFrameSize(value), ..rest] -> {
      use <- bool.guard(value < 16_384, Error(ConnectionError(ProtocolError)))
      use <- bool.guard(
        value > 16_777_215,
        Error(ConnectionError(ProtocolError)),
      )
      apply_settings(
        role,
        Settings(..settings, max_frame_size: value),
        rest,
        remote:,
      )
    }
    [MaxHeaderListSize(value), ..rest] ->
      apply_settings(
        role,
        Settings(..settings, max_header_list_size: option.Some(value)),
        rest,
        remote:,
      )
  }
}

pub fn default_settings() -> Settings {
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
  DataReceived(
    stream_id: Int,
    data: BitArray,
    end_stream: Bool,
    flow_controlled_length: Int,
  )
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

pub type ConnectionState {
  AwaitingPreface
  AwaitingSettings
  Connected
  Draining
}

pub type PendingHeaderBlock {
  PendingHeaders(stream_id: Int, end_stream: Bool, fragment: BitArray)
  PendingPushPromise(
    stream_id: Int,
    promised_stream_id: Int,
    fragment: BitArray,
  )
}

pub type Connection {
  Connection(
    state: ConnectionState,
    role: Role,
    local_settings: Settings,
    pending_settings: List(List(Setting)),
    remote_settings: Settings,
    streams: dict.Dict(Int, Stream),
    last_remote_stream_id: Int,
    next_stream_id: Int,
    recv_buffer: BitArray,
    send_window_size: Int,
    recv_window_size: Int,
    hpack_encoder: HpackContext,
    hpack_decoder: HpackContext,
    pending_header_blocks: option.Option(PendingHeaderBlock),
  )
}

/// Creates a new HTTP/2 connection for the given role and initial settings.
///
/// Returns the connection and the preface bytes that MUST be sent to the peer
/// before any other data. The provided settings are advertised to the peer in
/// the preface SETTINGS frame but do not take effect locally until a
/// `SettingsAcknowledged` event is received.
pub fn new_connection(
  role role: Role,
  settings settings: Settings,
) -> Result(#(Connection, BitArray), H2Error) {
  let next_stream_id = case role {
    Client -> 1
    Server -> 2
  }

  let state = case role {
    Client -> AwaitingSettings
    Server -> AwaitingPreface
  }

  let conn =
    Connection(
      state: state,
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
      pending_header_blocks: option.None,
    )

  use _ <- result.try(validate_settings(settings))

  use #(conn, encoded_settings) <- result.try(send_settings(
    conn,
    to_settings_list(settings, role),
  ))

  let initial_bytes = case role {
    Client -> <<client_preface_magic:bits, encoded_settings:bits>>
    Server -> encoded_settings
  }

  Ok(#(conn, initial_bytes))
}

fn count_inbound_streams(conn: Connection) -> Int {
  dict.fold(conn.streams, 0, fn(count, stream_id, stream) {
    let is_peer_stream = case conn.role {
      Server -> stream_id % 2 == 1
      Client -> stream_id % 2 == 0
    }

    case is_peer_stream, stream.state {
      True, Open | True, HalfClosedLocal | True, HalfClosedRemote -> count + 1
      _, _ -> count
    }
  })
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

pub type Indexing {
  WithIndexing
  WithoutIndexing
  NeverIndexed
}

pub type Header {
  Header(name: String, value: String, indexing: Indexing)
}

fn extract_status_code(headers: List(Header)) -> Result(Int, Nil) {
  case headers {
    [] -> Error(Nil)
    [header, ..rest] -> {
      case header.name {
        ":status" -> {
          case int.parse(header.value) {
            Ok(status_code) -> Ok(status_code)
            Error(_) -> Error(Nil)
          }
        }
        _ -> extract_status_code(rest)
      }
    }
  }
}

fn extract_content_length(
  headers: List(Header),
) -> Result(option.Option(Int), Nil) {
  case headers {
    [] -> Ok(option.None)
    [header, ..rest] -> {
      case header.name {
        "content-length" -> {
          case int.parse(header.value) {
            Ok(content_length) -> Ok(option.Some(content_length))
            Error(_) -> Error(Nil)
          }
        }
        _ -> extract_content_length(rest)
      }
    }
  }
}

fn verify_mandatory_pseudoheaders(
  role role: Role,
  headers headers: List(Header),
) -> Result(Nil, Nil) {
  let pseudo_names =
    list.filter_map(headers, fn(h) {
      case string.starts_with(h.name, ":") {
        True -> Ok(h.name)
        False -> Error(Nil)
      }
    })
  case role {
    Server -> {
      use method <- result.try(
        list.find(headers, fn(h) { h.name == ":method" }),
      )
      case method.value {
        "CONNECT" -> {
          case list.contains(pseudo_names, ":authority") {
            True -> {
              case
                list.contains(pseudo_names, ":scheme")
                || list.contains(pseudo_names, ":path")
              {
                True -> Error(Nil)
                False -> Ok(Nil)
              }
            }
            False -> Error(Nil)
          }
        }

        _ -> {
          use path <- result.try(
            list.find(headers, fn(h) { h.name == ":path" }),
          )
          use <- bool.guard(path.value == "", Error(Nil))

          case
            list.contains(pseudo_names, ":method")
            && list.contains(pseudo_names, ":scheme")
            && list.contains(pseudo_names, ":path")
          {
            True -> Ok(Nil)
            False -> Error(Nil)
          }
        }
      }
    }
    Client ->
      case list.contains(pseudo_names, ":status") {
        True -> Ok(Nil)
        False -> Error(Nil)
      }
  }
}

fn validate_headers(
  role role: Role,
  headers headers: List(Header),
  is_trailer is_trailer: Bool,
) -> Result(Nil, Nil) {
  use _ <- result.try(do_validate_headers(role, headers, is_trailer, False, []))
  case is_trailer {
    True -> Ok(Nil)
    False -> verify_mandatory_pseudoheaders(role, headers)
  }
}

fn validate_header_value(header: Header) -> Result(Nil, Nil) {
  use _ <- result.try(check_header_value_bytes(<<header.value:utf8>>))

  // header values must not start or end with a space character  
  use <- bool.guard(
    string.starts_with(header.value, " ") || string.ends_with(header.value, " "),
    Error(Nil),
  )

  // header values must not start or end with a tab character
  use <- bool.guard(
    string.starts_with(header.value, "\t")
      || string.ends_with(header.value, "\t"),
    Error(Nil),
  )

  Ok(Nil)
}

fn check_header_value_bytes(bytes: BitArray) -> Result(Nil, Nil) {
  case bytes {
    <<>> -> Ok(Nil)
    <<byte, rest:bits>> -> {
      case byte {
        // NUL
        0x00 -> Error(Nil)
        // LF
        0x0a -> Error(Nil)
        // CR
        0x0d -> Error(Nil)
        _ -> check_header_value_bytes(rest)
      }
    }
    _ -> Error(Nil)
  }
}

fn do_validate_headers(
  role role: Role,
  headers headers: List(Header),
  is_trailer is_trailer: Bool,
  seen_regular seen_regular: Bool,
  seen_pseudos seen_pseudos: List(String),
) -> Result(Nil, Nil) {
  case headers {
    [] -> Ok(Nil)

    [header, ..rest] -> {
      // Pseudo headers cannot arrive after regular headers
      use <- bool.guard(
        string.starts_with(header.name, ":") && seen_regular,
        Error(Nil),
      )

      let is_pseudo = string.starts_with(header.name, ":")

      use <- bool.guard(is_pseudo && seen_regular, Error(Nil))
      use <- bool.guard(
        is_pseudo && list.contains(seen_pseudos, header.name),
        Error(Nil),
      )

      use <- bool.guard(is_pseudo && is_trailer, Error(Nil))

      use _ <- result.try(validate_header_value(header))

      // TODO: Enable this when non-valid headers are allowed through alpacki
      // use <- bool.guard(
      //   is_pseudo
      //     && !list.contains(
      //     [":method", ":scheme", ":path", ":authority", ":status", ":protocol"],
      //     header.name,
      //   ),
      //   Error(Nil),
      // )

      use <- bool.guard(
        !is_pseudo
          && list.contains(
          [
            "connection",
            "transfer-encoding",
            "keep-alive",
            "proxy-connection",
            "upgrade",
          ],
          header.name,
        ),
        Error(Nil),
      )

      use <- bool.guard(
        !is_pseudo && header.name == "te" && header.value != "trailers",
        Error(Nil),
      )

      let #(seen_pseudos, seen_regular) = case is_pseudo {
        True -> #([header.name, ..seen_pseudos], seen_regular)
        False -> #(seen_pseudos, True)
      }

      do_validate_headers(role, rest, is_trailer, seen_regular, seen_pseudos)
    }
  }
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

pub type Setting {
  HeaderTableSize(Int)
  EnablePush(Int)
  MaxConcurrentStreams(Int)
  InitialWindowSize(Int)
  MaxFrameSize(Int)
  MaxHeaderListSize(Int)
}

fn to_frame_setting(setting: Setting) -> h2_frame.Setting {
  case setting {
    HeaderTableSize(v) -> h2_frame.HeaderTableSize(v)
    EnablePush(v) -> h2_frame.EnablePush(v)
    MaxConcurrentStreams(v) -> h2_frame.MaxConcurrentStreams(v)
    InitialWindowSize(v) -> h2_frame.InitialWindowSize(v)
    MaxFrameSize(v) -> h2_frame.MaxFrameSize(v)
    MaxHeaderListSize(v) -> h2_frame.MaxHeaderListSize(v)
  }
}

fn to_frame_settings(settings: List(Setting)) -> List(h2_frame.Setting) {
  list.map(settings, to_frame_setting)
}

pub type ErrorCode {
  NoError
  ProtocolError
  InternalError
  FlowControlError
  SettingsTimeout
  StreamClosed
  FrameSizeError
  RefusedStream
  Cancel
  CompressionError
  ConnectError
  EnhanceYourCalm
  InadequateSecurity
  Http11Required
  UnknownErrorCode(Int)
}

fn from_frame_error_code(code: h2_frame.ErrorCode) -> ErrorCode {
  case code {
    h2_frame.NoError -> NoError
    h2_frame.ProtocolError -> ProtocolError
    h2_frame.InternalError -> InternalError
    h2_frame.FlowControlError -> FlowControlError
    h2_frame.SettingsTimeout -> SettingsTimeout
    h2_frame.StreamClosed -> StreamClosed
    h2_frame.FrameSizeError -> FrameSizeError
    h2_frame.RefusedStream -> RefusedStream
    h2_frame.Cancel -> Cancel
    h2_frame.CompressionError -> CompressionError
    h2_frame.ConnectError -> ConnectError
    h2_frame.EnhanceYourCalm -> EnhanceYourCalm
    h2_frame.InadequateSecurity -> InadequateSecurity
    h2_frame.Http11Required -> Http11Required
    h2_frame.UnknownErrorCode(code) -> UnknownErrorCode(code)
  }
}

fn to_frame_error_code(code: ErrorCode) -> h2_frame.ErrorCode {
  case code {
    NoError -> h2_frame.NoError
    ProtocolError -> h2_frame.ProtocolError
    InternalError -> h2_frame.InternalError
    FlowControlError -> h2_frame.FlowControlError
    SettingsTimeout -> h2_frame.SettingsTimeout
    StreamClosed -> h2_frame.StreamClosed
    FrameSizeError -> h2_frame.FrameSizeError
    RefusedStream -> h2_frame.RefusedStream
    Cancel -> h2_frame.Cancel
    CompressionError -> h2_frame.CompressionError
    ConnectError -> h2_frame.ConnectError
    EnhanceYourCalm -> h2_frame.EnhanceYourCalm
    InadequateSecurity -> h2_frame.InadequateSecurity
    Http11Required -> h2_frame.Http11Required
    UnknownErrorCode(code) -> h2_frame.UnknownErrorCode(code)
  }
}

pub type H2Error {
  ConnectionError(error_code: ErrorCode)
  StreamError(stream_id: Int, error_code: ErrorCode)
}

fn map_frame_error(error: h2_frame.FrameError) -> H2Error {
  case error {
    h2_frame.ConnectionError(code) ->
      ConnectionError(from_frame_error_code(code))
    h2_frame.StreamError(id, code) ->
      StreamError(id, from_frame_error_code(code))
    h2_frame.InvalidPadding -> ConnectionError(ProtocolError)
    h2_frame.NeedMoreData -> ConnectionError(InternalError)
    h2_frame.MalformedFrame -> ConnectionError(InternalError)
  }
}

fn chunk_bytes(
  bytes: BitArray,
  chunk_size: Int,
  chunks: List(BitArray),
) -> List(BitArray) {
  case bytes {
    <<>> -> list.reverse(chunks)
    <<b:bytes-size(chunk_size), rest:bits>> ->
      chunk_bytes(rest, chunk_size, [b, ..chunks])
    <<b:bits>> -> chunk_bytes(<<>>, chunk_size, [b, ..chunks])
  }
}

fn encode_header_continuations(
  chunks: List(BitArray),
  stream_id: Int,
  processed: BitArray,
) -> Result(BitArray, H2Error) {
  case chunks {
    [] -> Ok(processed)
    [chunk] -> {
      case
        h2_frame.encode_continuation(
          stream_id: stream_id,
          end_headers: True,
          field_block_fragment: chunk,
        )
      {
        Ok(encoded_frame) -> {
          Ok(<<processed:bits, encoded_frame:bits>>)
        }
        Error(error) -> Error(map_frame_error(error))
      }
    }
    [chunk, ..rest] -> {
      case
        h2_frame.encode_continuation(
          stream_id: stream_id,
          end_headers: False,
          field_block_fragment: chunk,
        )
      {
        Ok(encoded_frame) -> {
          encode_header_continuations(rest, stream_id, <<
            processed:bits,
            encoded_frame:bits,
          >>)
        }
        Error(error) -> Error(map_frame_error(error))
      }
    }
  }
}

/// Decodes headers using the conns HPACK table, returning the headers
/// and a Connection with updated tables
pub fn decode_headers(
  conn conn: Connection,
  encoded_headers encoded_headers: BitArray,
) -> Result(#(Connection, List(Header)), H2Error) {
  use #(decoded_headers, new_table) <- result.try(
    alpacki.decode_header_block(encoded_headers, conn.hpack_decoder.table)
    |> result.replace_error(ConnectionError(CompressionError)),
  )

  let decoded_headers = list.map(decoded_headers, from_alpacki_header)
  let conn = Connection(..conn, hpack_decoder: HpackContext(table: new_table))

  Ok(#(conn, decoded_headers))
}

pub fn encode_headers(
  conn conn: Connection,
  headers headers: List(Header),
) -> Result(#(Connection, BitArray), H2Error) {
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

  Ok(#(conn, bytes_tree.to_bit_array(encoded_headers)))
}

fn handle_decoded_push_promise(
  conn: Connection,
  stream_id: Int,
  promised_stream_id: Int,
  decoded_headers: List(Header),
  events: List(Event),
  to_send: BitArray,
) -> Result(#(Connection, List(Event), BitArray), H2Error) {
  use <- bool.guard(
    validate_headers(Server, decoded_headers, False) == Error(Nil),
    handle_rst_stream(
      conn:,
      stream_id:,
      error_code: ProtocolError,
      flow_controlled_length: 0,
      events:,
      to_send:,
    ),
  )

  let promised_stream = Stream(..new_stream(), state: ReservedRemote)

  let conn =
    Connection(
      ..conn,
      streams: dict.insert(conn.streams, promised_stream_id, promised_stream),
      last_remote_stream_id: promised_stream_id,
    )

  Ok(#(
    conn,
    [
      PushPromiseReceived(
        stream_id: stream_id,
        promised_stream_id: promised_stream_id,
        headers: decoded_headers,
      ),
      ..events
    ],
    to_send,
  ))
}

fn handle_decoded_headers(
  conn: Connection,
  stream_id,
  end_stream,
  decoded_headers,
  events,
  to_send,
) -> Result(#(Connection, List(Event), BitArray), H2Error) {
  let is_existing =
    stream_id <= conn.last_remote_stream_id
    || dict.has_key(conn.streams, stream_id)
  let is_trailer = end_stream && is_existing

  use <- bool.guard(
    validate_headers(conn.role, decoded_headers, is_trailer) == Error(Nil),
    handle_rst_stream(
      conn:,
      stream_id:,
      error_code: ProtocolError,
      flow_controlled_length: 0,
      events:,
      to_send:,
    ),
  )

  case is_existing {
    True ->
      handle_headers_on_existing_stream(
        conn,
        stream_id,
        end_stream,
        decoded_headers,
        events,
        to_send,
      )
    False ->
      handle_headers_on_new_stream(
        conn,
        stream_id,
        end_stream,
        decoded_headers,
        events,
        to_send,
      )
  }
}

fn handle_headers_on_existing_stream(
  conn: Connection,
  stream_id,
  end_stream,
  decoded_headers,
  events,
  to_send,
) -> Result(#(Connection, List(Event), BitArray), H2Error) {
  case dict.get(conn.streams, stream_id) {
    Ok(existing_stream) -> {
      case existing_stream.state {
        Open | HalfClosedLocal -> {
          use <- bool.guard(
            conn.role == Client
              && existing_stream.final_response_received
              && !end_stream,
            handle_rst_stream(
              conn:,
              stream_id:,
              error_code: ProtocolError,
              flow_controlled_length: 0,
              events:,
              to_send:,
            ),
          )

          use status_code <- result.try(
            {
              case conn.role {
                Client -> extract_status_code(decoded_headers)
                Server -> Ok(0)
              }
            }
            |> result.replace_error(ConnectionError(ProtocolError)),
          )

          // Set final_response_received if this is a non-1xx response
          let existing_stream = case conn.role == Client && status_code >= 200 {
            True -> Stream(..existing_stream, final_response_received: True)
            False -> existing_stream
          }

          // If end_stream is set, update the stream state
          let new_state = case end_stream {
            False -> existing_stream.state
            True ->
              case existing_stream.state {
                Open -> HalfClosedRemote
                HalfClosedLocal -> Closed
                _ -> existing_stream.state
              }
          }

          let conn =
            Connection(
              ..conn,
              streams: dict.insert(
                conn.streams,
                stream_id,
                Stream(..existing_stream, state: new_state),
              ),
            )

          Ok(#(
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
          ))
        }

        // RFC 9113 Section 5.1 (half-closed remote)
        HalfClosedRemote ->
          handle_rst_stream(
            conn:,
            stream_id:,
            error_code: StreamClosed,
            flow_controlled_length: 0,
            events:,
            to_send:,
          )

        // RFC 9113 Section 5.1 (closed state) - minimally process and discard
        Closed -> Ok(#(conn, events, to_send))

        // RFC 9113 Section 5.1 (reserved remote) - HEADERS transitions to
        // half-closed (local)
        ReservedRemote -> {
          let new_state = case end_stream {
            True -> Closed
            False -> HalfClosedLocal
          }
          let conn =
            Connection(
              ..conn,
              streams: dict.insert(
                conn.streams,
                stream_id,
                Stream(..existing_stream, state: new_state),
              ),
            )
          Ok(#(
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
          ))
        }

        Idle | ReservedLocal -> Error(ConnectionError(ProtocolError))
      }
    }
    Error(Nil) -> Error(ConnectionError(ProtocolError))
  }
}

fn handle_headers_on_new_stream(
  conn: Connection,
  stream_id,
  end_stream,
  decoded_headers,
  events,
  to_send,
) -> Result(#(Connection, List(Event), BitArray), H2Error) {
  // RFC 9113 Section 5.1.1 - Client streams are odd, server streams are even.
  use <- bool.guard(
    case conn.role {
      Server -> stream_id % 2 == 0
      Client -> stream_id % 2 != 0
    },
    Error(ConnectionError(ProtocolError)),
  )

  // Check MAX_CONCURRENT_STREAMS
  use <- bool.guard(
    case conn.local_settings.max_concurrent_streams {
      option.Some(max) -> count_inbound_streams(conn) >= max
      option.None -> False
    },
    handle_rst_stream(
      conn:,
      stream_id:,
      error_code: RefusedStream,
      flow_controlled_length: 0,
      events:,
      to_send:,
    ),
  )

  // Ignore new streams if state is Draining, we are shutting down
  use <- bool.guard(conn.state == Draining, Ok(#(conn, events, to_send)))

  // Make sure content length is valid if present
  let content_length_result = extract_content_length(decoded_headers)

  use <- bool.guard(
    content_length_result == Error(Nil),
    handle_rst_stream(
      conn:,
      stream_id:,
      error_code: ProtocolError,
      flow_controlled_length: 0,
      events:,
      to_send:,
    ),
  )

  // This can't actually error due to our guard before
  use content_length <- result.try(
    content_length_result
    |> result.replace_error(StreamError(stream_id, ProtocolError)),
  )

  let stream = case end_stream {
    False ->
      Stream(
        ..new_stream(),
        state: Open,
        expected_content_length: content_length,
      )
    True ->
      Stream(
        ..new_stream(),
        state: HalfClosedRemote,
        expected_content_length: content_length,
      )
  }

  let conn =
    Connection(
      ..conn,
      last_remote_stream_id: stream_id,
      streams: dict.insert(conn.streams, stream_id, stream),
    )

  Ok(#(
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
  ))
}

pub fn open_stream(
  conn conn: Connection,
  headers headers: List(Header),
  end_stream end_stream: Bool,
) -> Result(#(Connection, BitArray, Int), H2Error) {
  // Not allowed to open streams while in Draining state (we have received a GOAWAY)
  use <- bool.guard(
    conn.state == Draining,
    Error(ConnectionError(ProtocolError)),
  )

  let stream = new_stream()
  let #(conn, stream_id) = add_stream(conn, stream)

  // Send initial headers
  case send_headers(conn, stream_id, headers, end_stream) {
    Ok(#(conn, bytes)) -> Ok(#(conn, bytes, stream_id))
    Error(e) -> Error(e)
  }
}

pub fn send_headers(
  conn conn: Connection,
  stream_id stream_id: Int,
  headers headers: List(Header),
  end_stream end_stream: Bool,
) -> Result(#(Connection, BitArray), H2Error) {
  // headers cannot be sent on the connection level, it must be on a stream
  use <- bool.guard(stream_id == 0, Error(ConnectionError(ProtocolError)))

  // headers must be sent on a existing stream
  use stream <- result.try(
    dict.get(conn.streams, stream_id)
    |> result.replace_error(StreamError(stream_id, StreamClosed)),
  )

  // headers cannot be sent on a ReservedRemote stream, those are initiated by the other party!
  use <- bool.guard(
    stream.state == ReservedRemote,
    Error(ConnectionError(ProtocolError)),
  )

  // Headers must not be sent on a stream we have initiated closing of or a closed stream
  use <- bool.guard(
    stream.state == HalfClosedLocal || stream.state == Closed,
    Error(StreamError(stream_id, StreamClosed)),
  )

  // Validate outbound headers — clients send requests (validate with
  // Server rules), servers send responses (validate with Client rules).
  // RFC 9113 Section 8.3.1 / 8.3.2.
  let outbound_role = case conn.role {
    Server -> Client
    Client -> Server
  }
  use <- bool.guard(
    validate_headers(outbound_role, headers, stream.headers_sent) == Error(Nil),
    Error(StreamError(stream_id, ProtocolError)),
  )

  use #(conn, encoded_headers) <- result.try(encode_headers(conn, headers))

  let new_state = case stream.state {
    HalfClosedRemote -> {
      case end_stream {
        True -> Closed
        False -> stream.state
      }
    }
    ReservedLocal -> HalfClosedRemote
    _ ->
      case end_stream {
        True -> HalfClosedLocal
        False -> Open
      }
  }

  let stream = Stream(..stream, state: new_state, headers_sent: True)

  let conn =
    Connection(..conn, streams: dict.insert(conn.streams, stream_id, stream))

  case chunk_bytes(encoded_headers, conn.remote_settings.max_frame_size, []) {
    [] -> {
      use frame <- result.try(
        h2_frame.encode_headers(
          stream_id: stream_id,
          end_stream: end_stream,
          end_headers: True,
          priority: option.None,
          field_block_fragment: <<>>,
          padding: option.None,
        )
        |> result.map_error(map_frame_error),
      )
      Ok(#(conn, frame))
    }

    [header_chunk, ..rest] -> {
      use headers_frame <- result.try(
        h2_frame.encode_headers(
          stream_id: stream_id,
          end_stream: end_stream,
          end_headers: rest == [],
          priority: option.None,
          field_block_fragment: header_chunk,
          padding: option.None,
        )
        |> result.map_error(map_frame_error),
      )
      use continuation_frames <- result.try(
        encode_header_continuations(rest, stream_id, <<>>),
      )
      Ok(#(conn, <<headers_frame:bits, continuation_frames:bits>>))
    }
  }
}

pub fn send_push_promise(
  conn conn: Connection,
  stream_id stream_id: Int,
  headers headers: List(Header),
) -> Result(#(Connection, BitArray, Int), H2Error) {
  // Not allowed to open streams while in Draining state (we have received a GOAWAY)
  use <- bool.guard(
    conn.state == Draining,
    Error(ConnectionError(ProtocolError)),
  )

  // Must only be sent by server
  use <- bool.guard(conn.role == Client, Error(ConnectionError(ProtocolError)))

  // Must not be sent on stream 0 (the connection level)
  use <- bool.guard(stream_id == 0, Error(ConnectionError(ProtocolError)))

  // Stream must exist
  use stream <- result.try(
    dict.get(conn.streams, stream_id)
    |> result.replace_error(ConnectionError(ProtocolError)),
  )

  use <- bool.guard(
    stream.state != Open && stream.state != HalfClosedRemote,
    Error(ConnectionError(ProtocolError)),
  )

  use <- bool.guard(
    !conn.remote_settings.enable_push,
    Error(ConnectionError(ProtocolError)),
  )

  let #(conn, promised_stream_id) =
    add_stream(conn, Stream(..new_stream(), state: ReservedLocal))

  use #(conn, encoded_headers) <- result.try(encode_headers(
    conn: conn,
    headers: headers,
  ))

  case chunk_bytes(encoded_headers, conn.remote_settings.max_frame_size, []) {
    [] -> {
      use push_promise_frame <- result.try(
        h2_frame.encode_push_promise(
          stream_id: stream_id,
          promised_stream_id: promised_stream_id,
          end_headers: True,
          field_block_fragment: <<>>,
          padding: option.None,
        )
        |> result.map_error(map_frame_error),
      )
      Ok(#(conn, push_promise_frame, promised_stream_id))
    }

    [header_chunk, ..rest] -> {
      use push_promise_frame <- result.try(
        h2_frame.encode_push_promise(
          stream_id: stream_id,
          promised_stream_id: promised_stream_id,
          end_headers: True,
          field_block_fragment: header_chunk,
          padding: option.None,
        )
        |> result.map_error(map_frame_error),
      )
      use continuation_frames <- result.try(
        encode_header_continuations(rest, stream_id, <<>>),
      )
      Ok(#(
        conn,
        <<push_promise_frame:bits, continuation_frames:bits>>,
        promised_stream_id,
      ))
    }
  }
}

pub fn send_data(
  conn conn: Connection,
  stream_id stream_id: Int,
  data data: BitArray,
  end_stream end_stream: Bool,
  padding padding: option.Option(Int),
) -> Result(#(Connection, BitArray), H2Error) {
  use <- bool.guard(stream_id == 0, Error(ConnectionError(ProtocolError)))

  use stream <- result.try(case dict.get(conn.streams, stream_id) {
    Ok(stream) -> {
      use <- bool.guard(
        stream.state == HalfClosedLocal || stream.state == Closed,
        Error(StreamError(stream_id, StreamClosed)),
      )
      use <- bool.guard(
        stream.state == ReservedLocal || stream.state == ReservedRemote,
        Error(ConnectionError(ProtocolError)),
      )
      Ok(stream)
    }
    Error(Nil) ->
      Error(StreamError(stream_id: stream_id, error_code: StreamClosed))
  })

  use max_allowed_window_size <- result.try(
    get_send_window_size(conn: conn, stream_id: stream_id)
    |> result.replace_error(StreamError(
      stream_id: stream_id,
      error_code: StreamClosed,
    )),
  )

  let padding_length = case padding {
    // We need to count both the actual padding bytes and
    // the extra pad length byte that gets added
    option.Some(value) -> value + 1
    option.None -> 0
  }

  let payload_length = bit_array.byte_size(data) + padding_length

  use <- bool.guard(
    payload_length > conn.remote_settings.max_frame_size,
    Error(ConnectionError(FrameSizeError)),
  )

  use <- bool.guard(
    payload_length > max_allowed_window_size,
    Error(ConnectionError(FlowControlError)),
  )

  use encoded_frame <- result.try(
    h2_frame.encode_data(
      stream_id: stream_id,
      end_stream: end_stream,
      data: data,
      padding: padding,
    )
    |> result.map_error(map_frame_error),
  )

  let new_stream_state = case end_stream {
    True ->
      case stream.state {
        HalfClosedRemote -> Closed
        _ -> HalfClosedLocal
      }
    False -> stream.state
  }

  // Update the window size of the stream, and set the new state
  let stream =
    Stream(
      ..stream,
      send_window_size: stream.send_window_size - payload_length,
      state: new_stream_state,
    )

  // Update the window sizes of the connection, and add the updated stream
  let conn =
    Connection(
      ..conn,
      send_window_size: conn.send_window_size - payload_length,
      streams: dict.insert(conn.streams, stream_id, stream),
    )

  Ok(#(conn, encoded_frame))
}

pub fn get_send_window_size(
  conn conn: Connection,
  stream_id stream_id: Int,
) -> Result(Int, Nil) {
  case dict.get(conn.streams, stream_id) {
    Ok(stream) ->
      Ok(int.max(0, int.min(stream.send_window_size, conn.send_window_size)))
    Error(Nil) -> Error(Nil)
  }
}

pub fn send_settings(
  conn conn: Connection,
  settings settings: List(Setting),
) -> Result(#(Connection, BitArray), H2Error) {
  let conn =
    Connection(
      ..conn,
      pending_settings: list.append(conn.pending_settings, [settings]),
    )
  case
    h2_frame.encode_settings(ack: False, settings: to_frame_settings(settings))
  {
    Ok(encoded) -> {
      Ok(#(conn, encoded))
    }
    Error(error) -> Error(map_frame_error(error))
  }
}

pub fn send_ping(
  conn conn: Connection,
  data data: BitArray,
) -> Result(#(Connection, BitArray), H2Error) {
  case h2_frame.encode_ping(ack: False, data: data) {
    Ok(encoded) -> {
      Ok(#(conn, encoded))
    }
    Error(error) -> Error(map_frame_error(error))
  }
}

pub fn send_goaway(
  conn conn: Connection,
  error_code error_code: ErrorCode,
  debug_data debug_data: BitArray,
) -> Result(#(Connection, BitArray), H2Error) {
  let encoded_frame =
    h2_frame.encode_goaway(
      last_stream_id: conn.last_remote_stream_id,
      error_code: to_frame_error_code(error_code),
      debug_data: debug_data,
    )
  Ok(#(Connection(..conn, state: Draining), encoded_frame))
}

pub fn send_window_update(
  conn conn: Connection,
  stream_id stream_id: Int,
  window_size_increment window_size_increment: Int,
) -> Result(#(Connection, BitArray), H2Error) {
  // Update the connection

  use conn <- result.try(case stream_id {
    // updating window size on the connection
    0 -> {
      let conn =
        Connection(
          ..conn,
          recv_window_size: conn.recv_window_size + window_size_increment,
        )
      use <- bool.guard(
        conn.recv_window_size > 2_147_483_647,
        Error(ConnectionError(FlowControlError)),
      )

      Ok(conn)
    }
    // updating window size on stream
    _ -> {
      case dict.get(conn.streams, stream_id) {
        Ok(stream) -> {
          let stream =
            Stream(
              ..stream,
              recv_window_size: stream.recv_window_size + window_size_increment,
            )

          use <- bool.guard(
            stream.state == Closed,
            Error(ConnectionError(ProtocolError)),
          )
          use <- bool.guard(
            stream.recv_window_size > 2_147_483_647,
            Error(ConnectionError(FlowControlError)),
          )
          Ok(
            Connection(
              ..conn,
              streams: dict.insert(conn.streams, stream_id, stream),
            ),
          )
        }
        // Stream doesn't exist
        Error(_) -> Error(ConnectionError(ProtocolError))
      }
    }
  })

  case
    h2_frame.encode_window_update(
      stream_id: stream_id,
      window_size_increment: window_size_increment,
    )
  {
    Ok(encoded_frame) -> Ok(#(conn, encoded_frame))
    Error(error) -> Error(map_frame_error(error))
  }
}

pub fn send_rst_stream(
  conn conn: Connection,
  stream_id stream_id: Int,
  error_code error_code: ErrorCode,
) -> Result(#(Connection, BitArray), H2Error) {
  // Must be sent on a existing stream
  use stream <- result.try(
    dict.get(conn.streams, stream_id)
    |> result.replace_error(ConnectionError(ProtocolError)),
  )

  // Must not be sent on a idle stream
  use <- bool.guard(stream.state == Idle, Error(ConnectionError(ProtocolError)))

  use <- bool.guard(
    stream.state == Closed,
    Error(ConnectionError(ProtocolError)),
  )

  // Close the stream
  let stream = Stream(..stream, state: Closed)

  let conn =
    Connection(..conn, streams: dict.insert(conn.streams, stream_id, stream))

  case
    h2_frame.encode_rst_stream(
      stream_id: stream_id,
      error_code: to_frame_error_code(error_code),
    )
  {
    Ok(encoded_frame) -> Ok(#(conn, encoded_frame))
    Error(error) -> Error(map_frame_error(error))
  }
}

fn handle_rst_stream(
  conn conn: Connection,
  stream_id stream_id: Int,
  error_code error_code: ErrorCode,
  flow_controlled_length flow_controlled_length: Int,
  events events: List(Event),
  to_send to_send: BitArray,
) -> Result(#(Connection, List(Event), BitArray), H2Error) {
  use encoded_rst_stream_frame <- result.try(
    h2_frame.encode_rst_stream(
      stream_id: stream_id,
      error_code: to_frame_error_code(error_code),
    )
    |> result.map_error(map_frame_error),
  )

  // If flow_controlled_length is not 0, add a WindowReset frame
  case flow_controlled_length {
    0 ->
      Ok(
        #(conn, [StreamReset(stream_id:, error_code:), ..events], <<
          to_send:bits,
          encoded_rst_stream_frame:bits,
        >>),
      )
    _ -> {
      use encoded_window_update <- result.try(
        h2_frame.encode_window_update(
          stream_id: 0,
          window_size_increment: flow_controlled_length,
        )
        |> result.map_error(map_frame_error),
      )
      Ok(
        #(conn, [StreamReset(stream_id:, error_code:), ..events], <<
          to_send:bits,
          encoded_rst_stream_frame:bits,
          encoded_window_update:bits,
        >>),
      )
    }
  }
}

fn parse_loop(
  conn: Connection,
  events: List(Event),
  to_send: BitArray,
) -> Result(#(Connection, List(Event), BitArray), H2Error) {
  case
    h2_frame.extract_frame(conn.recv_buffer, conn.local_settings.max_frame_size)
  {
    Ok(#(frame_data, rest)) -> {
      let conn = Connection(..conn, recv_buffer: rest)

      case h2_frame.decode_frame(frame_data) {
        Ok(frame) -> {
          // Make sure we are not receiving anything but settings in the AwaitingSettings state
          use <- bool.guard(
            conn.state == AwaitingSettings
              && case frame {
              h2_frame.Settings(ack: False, ..) -> False
              _ -> True
            },
            Error(ConnectionError(ProtocolError)),
          )

          case frame {
            // If we're in the middle of receiving a header block,
            // only CONTINUATION on the same stream is allowed
            _ if conn.pending_header_blocks != option.None -> {
              case frame {
                h2_frame.Continuation(
                  stream_id,
                  end_headers,
                  field_block_fragment,
                ) -> {
                  case conn.pending_header_blocks {
                    option.None -> Error(ConnectionError(ProtocolError))

                    option.Some(PendingHeaders(
                      pending_stream_id,
                      end_stream,
                      pending_block_fragment,
                    )) -> {
                      use <- bool.guard(
                        pending_stream_id != stream_id,
                        Error(ConnectionError(ProtocolError)),
                      )

                      let combined = <<
                        pending_block_fragment:bits,
                        field_block_fragment:bits,
                      >>

                      case end_headers {
                        // More continuation frames incoming
                        False -> {
                          let conn =
                            Connection(
                              ..conn,
                              pending_header_blocks: option.Some(PendingHeaders(
                                stream_id:,
                                end_stream:,
                                fragment: combined,
                              )),
                            )
                          parse_loop(conn, events, to_send)
                        }

                        // Last continuation block
                        True -> {
                          use #(conn, decoded_headers) <- result.try(
                            decode_headers(conn, combined),
                          )

                          let conn =
                            Connection(
                              ..conn,
                              pending_header_blocks: option.None,
                            )

                          use #(conn, events, to_send) <- result.try(
                            handle_decoded_headers(
                              conn,
                              stream_id,
                              end_stream,
                              decoded_headers,
                              events,
                              to_send,
                            ),
                          )
                          parse_loop(conn, events, to_send)
                        }
                      }
                    }

                    option.Some(PendingPushPromise(
                      pending_stream_id,
                      promised_stream_id,
                      pending_block_fragment,
                    )) -> {
                      use <- bool.guard(
                        pending_stream_id != stream_id,
                        Error(ConnectionError(ProtocolError)),
                      )

                      let combined = <<
                        pending_block_fragment:bits,
                        field_block_fragment:bits,
                      >>

                      case end_headers {
                        // More continuation frames incoming
                        False -> {
                          let conn =
                            Connection(
                              ..conn,
                              pending_header_blocks: option.Some(
                                PendingPushPromise(
                                  stream_id:,
                                  promised_stream_id:,
                                  fragment: combined,
                                ),
                              ),
                            )
                          parse_loop(conn, events, to_send)
                        }

                        // Last continuation block
                        True -> {
                          use #(conn, decoded_headers) <- result.try(
                            decode_headers(conn, combined),
                          )

                          let conn =
                            Connection(
                              ..conn,
                              pending_header_blocks: option.None,
                            )

                          use #(conn, events, to_send) <- result.try(
                            handle_decoded_push_promise(
                              conn,
                              stream_id,
                              promised_stream_id,
                              decoded_headers,
                              events,
                              to_send,
                            ),
                          )
                          parse_loop(conn, events, to_send)
                        }
                      }
                    }
                  }
                }
                _ -> Error(ConnectionError(ProtocolError))
              }
            }

            // Handle CONTINUATION if pending_header_blocks is None
            // This is always an error
            h2_frame.Continuation(_, _, _) ->
              Error(ConnectionError(ProtocolError))

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
              parse_loop(
                conn,
                [PingAcknowledged(data: data), ..events],
                to_send,
              )
            }

            // Settings
            h2_frame.Settings(ack: False, settings: frame_settings) -> {
              // Apply these settings to the remote settings
              let settings = from_frame_settings(frame_settings)
              case
                apply_settings(
                  conn.role,
                  conn.remote_settings,
                  settings,
                  remote: True,
                )
              {
                Ok(new_settings) -> {
                  let old_settings = conn.remote_settings

                  // Apply new settings and update the state
                  let conn =
                    Connection(
                      ..conn,
                      remote_settings: new_settings,
                      state: Connected,
                    )

                  use conn <- result.try(apply_send_new_window_size(
                    conn,
                    new_settings.initial_window_size
                      - old_settings.initial_window_size,
                  ))

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
                  case
                    apply_settings(
                      conn.role,
                      conn.local_settings,
                      settings,
                      remote: False,
                    )
                  {
                    // Apply settings
                    Ok(new_settings) -> {
                      let old_settings = conn.local_settings
                      let conn =
                        Connection(
                          ..conn,
                          local_settings: new_settings,
                          pending_settings: rest,
                        )

                      use conn <- result.try(apply_recv_new_window_size(
                        conn,
                        new_settings.initial_window_size
                          - old_settings.initial_window_size,
                      ))

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
                  Error(ConnectionError(ProtocolError))
                }
              }
            }

            // Goaway
            h2_frame.Goaway(last_stream_id, error_code, debug_data) -> {
              parse_loop(
                Connection(..conn, state: Draining),
                [
                  GoawayReceived(
                    last_stream_id: last_stream_id,
                    error_code: from_frame_error_code(error_code),
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
                    Error(ConnectionError(FlowControlError)),
                  )
                  parse_loop(conn, events, to_send)
                }
                // Handle WindowUpdate on specific stream
                stream_id -> {
                  case dict.get(conn.streams, stream_id) {
                    Ok(stream) -> {
                      let stream =
                        Stream(
                          ..stream,
                          send_window_size: stream.send_window_size
                            + window_size_increment,
                        )

                      use <- bool.guard(
                        stream.state == ReservedRemote,
                        Error(ConnectionError(ProtocolError)),
                      )

                      use <- bool.guard(
                        stream.send_window_size > 2_147_483_647,
                        {
                          // Send RST_STREAM and bubble up StreamReset
                          case
                            h2_frame.encode_rst_stream(
                              stream_id: stream_id,
                              error_code: to_frame_error_code(FlowControlError),
                            )
                          {
                            Ok(encoded_frame) -> {
                              parse_loop(
                                conn,
                                [
                                  StreamReset(
                                    stream_id: stream_id,
                                    error_code: FlowControlError,
                                  ),
                                  ..events
                                ],
                                <<to_send:bits, encoded_frame:bits>>,
                              )
                            }
                            Error(error) -> Error(map_frame_error(error))
                          }
                        },
                      )

                      let conn =
                        Connection(
                          ..conn,
                          streams: dict.insert(conn.streams, stream_id, stream),
                        )

                      parse_loop(conn, events, to_send)
                    }
                    Error(_) -> Error(ConnectionError(ProtocolError))
                  }
                }
              }
            }

            // RST_STREAM
            h2_frame.RstStream(stream_id, error_code) -> {
              use stream <- result.try(
                dict.get(conn.streams, stream_id)
                |> result.replace_error(ConnectionError(ProtocolError)),
              )

              use <- bool.guard(
                stream.state == Closed,
                parse_loop(conn, events, to_send),
              )

              let stream = Stream(..stream, state: Closed)

              let conn =
                Connection(
                  ..conn,
                  streams: dict.insert(conn.streams, stream_id, stream),
                )

              parse_loop(
                conn,
                [
                  StreamReset(
                    stream_id: stream_id,
                    error_code: from_frame_error_code(error_code),
                  ),
                  ..events
                ],
                to_send,
              )
            }

            // HEADERS
            h2_frame.Headers(
              stream_id,
              end_stream,
              end_headers,
              _priority,
              field_block_fragment,
            ) -> {
              case end_headers {
                False -> {
                  let conn =
                    Connection(
                      ..conn,
                      pending_header_blocks: option.Some(PendingHeaders(
                        stream_id:,
                        end_stream:,
                        fragment: field_block_fragment,
                      )),
                    )
                  parse_loop(conn, events, to_send)
                }
                True -> {
                  use #(conn, decoded_headers) <- result.try(decode_headers(
                    conn,
                    field_block_fragment,
                  ))

                  use #(conn, events, to_send) <- result.try(
                    handle_decoded_headers(
                      conn,
                      stream_id,
                      end_stream,
                      decoded_headers,
                      events,
                      to_send,
                    ),
                  )
                  parse_loop(conn, events, to_send)
                }
              }
            }

            // DATA
            h2_frame.Data(stream_id, end_stream, padding, data) -> {
              use stream <- result.try(case dict.get(conn.streams, stream_id) {
                Ok(stream) -> {
                  use <- bool.guard(
                    stream.state == ReservedLocal
                      || stream.state == ReservedRemote,
                    Error(ConnectionError(ProtocolError)),
                  )
                  Ok(stream)
                }
                Error(Nil) -> Error(ConnectionError(ProtocolError))
              })

              let new_stream_state = case end_stream {
                True -> {
                  case stream.state {
                    HalfClosedLocal -> Closed
                    _ -> HalfClosedRemote
                  }
                }
                False -> stream.state
              }

              let payload_length = case padding {
                option.Some(pad_length) ->
                  1 + bit_array.byte_size(data) + pad_length
                option.None -> bit_array.byte_size(data)
              }

              let new_conn_recv_window = conn.recv_window_size - payload_length

              // Make sure that the data does not exceed the connection recv window
              use <- bool.guard(
                new_conn_recv_window < 0,
                Error(ConnectionError(FlowControlError)),
              )

              let new_stream_recv_window =
                stream.recv_window_size - payload_length
              let conn =
                Connection(
                  ..conn,
                  streams: dict.insert(
                    conn.streams,
                    stream_id,
                    Stream(
                      ..stream,
                      state: new_stream_state,
                      recv_window_size: new_stream_recv_window,
                      received_content_length: stream.received_content_length
                        + bit_array.byte_size(data),
                    ),
                  ),
                  recv_window_size: new_conn_recv_window,
                )

              // Make sure we didn't receive more data than the expected content length
              use <- bool.guard(
                {
                  case stream.expected_content_length {
                    option.Some(expected_length) ->
                      stream.received_content_length + bit_array.byte_size(data)
                      > expected_length
                    option.None -> False
                  }
                },
                handle_rst_stream(
                  conn: conn,
                  stream_id: stream_id,
                  error_code: ProtocolError,
                  flow_controlled_length: payload_length,
                  events: events,
                  to_send: to_send,
                ),
              )

              // If this is the end of the stream, make sure we received the promised amount of data
              use <- bool.guard(
                end_stream
                  && case stream.expected_content_length {
                  option.Some(expected) ->
                    stream.received_content_length + bit_array.byte_size(data)
                    < expected
                  option.None -> False
                },
                handle_rst_stream(
                  conn: conn,
                  stream_id: stream_id,
                  error_code: ProtocolError,
                  flow_controlled_length: payload_length,
                  events: events,
                  to_send: to_send,
                ),
              )

              // Receiving data frames on a already HalfClosedRemote stream
              // triggers a RST_STREAM
              use <- bool.guard(
                stream.state == HalfClosedRemote,
                handle_rst_stream(
                  conn: conn,
                  stream_id: stream_id,
                  error_code: StreamClosed,
                  flow_controlled_length: payload_length,
                  events: events,
                  to_send: to_send,
                ),
              )

              use <- bool.guard(
                new_stream_recv_window < 0,
                handle_rst_stream(
                  conn: conn,
                  stream_id: stream_id,
                  error_code: FlowControlError,
                  flow_controlled_length: payload_length,
                  events: events,
                  to_send: to_send,
                ),
              )

              parse_loop(
                conn,
                [
                  DataReceived(
                    stream_id: stream_id,
                    data: data,
                    end_stream: end_stream,
                    flow_controlled_length: payload_length,
                  ),
                  ..events
                ],
                to_send,
              )
            }

            h2_frame.PushPromise(
              stream_id,
              end_headers,
              promised_stream_id,
              field_block_fragment,
            ) -> {
              // Can only be received by clients
              use <- bool.guard(
                conn.role == Server,
                Error(ConnectionError(ProtocolError)),
              )

              // Parent stream must exist
              use stream <- result.try(
                dict.get(conn.streams, stream_id)
                |> result.replace_error(ConnectionError(ProtocolError)),
              )

              // Stream state must be one of these
              use <- bool.guard(
                !list.contains([Open, HalfClosedLocal, Closed], stream.state),
                Error(ConnectionError(ProtocolError)),
              )

              // Our setting for enable push must be true
              use <- bool.guard(
                !conn.local_settings.enable_push,
                Error(ConnectionError(ProtocolError)),
              )

              // Promised stream ID must be even (it comes from a servere)
              use <- bool.guard(
                promised_stream_id % 2 == 1,
                Error(ConnectionError(ProtocolError)),
              )

              // Promised stream ID must be a new stream
              use <- bool.guard(
                promised_stream_id <= conn.last_remote_stream_id,
                Error(ConnectionError(ProtocolError)),
              )

              case end_headers {
                False -> {
                  let conn =
                    Connection(
                      ..conn,
                      pending_header_blocks: option.Some(PendingPushPromise(
                        stream_id:,
                        promised_stream_id:,
                        fragment: field_block_fragment,
                      )),
                    )
                  parse_loop(conn, events, to_send)
                }
                True -> {
                  use #(conn, decoded_headers) <- result.try(decode_headers(
                    conn,
                    field_block_fragment,
                  ))

                  use #(conn, events, to_send) <- result.try(
                    handle_decoded_push_promise(
                      conn,
                      stream_id,
                      promised_stream_id,
                      decoded_headers,
                      events,
                      to_send,
                    ),
                  )
                  parse_loop(conn, events, to_send)
                }
              }
            }

            // Ignore PRIORITY frames
            h2_frame.Priority(_, _, _, _) -> {
              parse_loop(conn, events, to_send)
            }

            // Ignore unknown frames
            h2_frame.Unknown(_, _, _, _) -> {
              parse_loop(conn, events, to_send)
            }
          }
        }
        Error(h2_frame.StreamError(stream_id, error_code)) -> {
          use encoded_frame <- result.try(
            h2_frame.encode_rst_stream(stream_id, error_code)
            |> result.map_error(map_frame_error),
          )

          parse_loop(
            conn,
            [
              StreamReset(
                stream_id: stream_id,
                error_code: from_frame_error_code(error_code),
              ),
              ..events
            ],
            <<to_send:bits, encoded_frame:bits>>,
          )
        }
        Error(h2_frame.MalformedFrame) -> Error(ConnectionError(ProtocolError))

        Error(error) -> Error(map_frame_error(error))
      }
    }

    Error(h2_frame.ConnectionError(error_code)) ->
      Error(ConnectionError(error_code: from_frame_error_code(error_code)))

    Error(h2_frame.NeedMoreData) -> Ok(#(conn, list.reverse(events), to_send))

    _ -> todo
  }
}

const client_preface_magic = <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n":utf8>>

pub fn receive_data(
  conn conn: Connection,
  data data: BitArray,
) -> Result(#(Connection, List(Event), BitArray), H2Error) {
  let conn =
    Connection(..conn, recv_buffer: <<conn.recv_buffer:bits, data:bits>>)

  case conn.state {
    // Make sure we receive the preface first!
    AwaitingPreface -> {
      let size = bit_array.byte_size(conn.recv_buffer)
      case size < 24 {
        True -> {
          case client_preface_magic {
            <<expected:bytes-size(size), _:bits>> ->
              case conn.recv_buffer == expected {
                True -> Ok(#(conn, [], <<>>))
                False -> Error(ConnectionError(ProtocolError))
              }
            _ -> Error(ConnectionError(ProtocolError))
          }
        }
        False -> {
          case conn.recv_buffer {
            <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n":utf8, rest:bits>> -> {
              let conn =
                Connection(..conn, state: AwaitingSettings, recv_buffer: rest)
              parse_loop(conn, [], <<>>)
            }
            _ -> Error(ConnectionError(ProtocolError))
          }
        }
      }
    }

    _ -> {
      case parse_loop(conn, [], <<>>) {
        Ok(#(conn, events, to_send)) -> {
          Ok(#(conn, events, to_send))
        }

        Error(error) -> Error(error)
      }
    }
  }
}
