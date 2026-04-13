import alpacki
import gleam/bit_array
import gleam/bool
import gleam/dict
import gleam/int
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import h2_core/internal.{
  type ConnectionState, AwaitingPreface, AwaitingSettings, Connected, Draining,
}
import h2_core/internal/stream.{
  type Stream, type StreamState, Closed, HalfClosedLocal, HalfClosedRemote, Idle,
  Open, ReservedLocal, ReservedRemote, Stream, new_stream,
}
import h2_frame

/// The role of an HTTP/2 endpoint, passed to `new_connection`.
/// Determines stream ID parity and which protocol rules apply.
pub type Role {
  Client
  Server
}

/// The full set of HTTP/2 connection settings
/// ([RFC 9113 Section 6.5.2](https://www.rfc-editor.org/rfc/rfc9113#section-6.5.2)).
/// Passed to `new_connection` to configure the initial local settings.
/// Use `default_settings` as a starting point.
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

fn validate_settings(settings: Settings) -> Result(Nil, SendError) {
  use _ <- result.try(
    validate_setting(HeaderTableSize(settings.header_table_size)),
  )
  use _ <- result.try(
    validate_setting(InitialWindowSize(settings.initial_window_size)),
  )
  use _ <- result.try(validate_setting(MaxFrameSize(settings.max_frame_size)))
  use _ <- result.try(case settings.max_concurrent_streams {
    option.Some(v) -> validate_setting(MaxConcurrentStreams(v))
    option.None -> Ok(Nil)
  })
  use _ <- result.try(case settings.max_header_list_size {
    option.Some(v) -> validate_setting(MaxHeaderListSize(v))
    option.None -> Ok(Nil)
  })
  Ok(Nil)
}

fn validate_setting(setting: Setting) -> Result(Nil, SendError) {
  case setting {
    InitialWindowSize(v) ->
      bool.guard(v < 0 || v > 2_147_483_647, Error(InvalidSettings), fn() {
        Ok(Nil)
      })
    MaxFrameSize(v) ->
      bool.guard(v < 16_384 || v > 16_777_215, Error(InvalidSettings), fn() {
        Ok(Nil)
      })
    HeaderTableSize(v) ->
      bool.guard(v < 0, Error(InvalidSettings), fn() { Ok(Nil) })
    MaxConcurrentStreams(v) ->
      bool.guard(v < 0, Error(InvalidSettings), fn() { Ok(Nil) })
    MaxHeaderListSize(v) ->
      bool.guard(v < 0, Error(InvalidSettings), fn() { Ok(Nil) })
    EnablePush(_) -> Ok(Nil)
  }
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
    Client -> [EnablePush(settings.enable_push), ..settings_list]
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

      // Don't update closed streams
      use <- bool.guard(
        stream.state != Open && stream.state != HalfClosedRemote,
        // return unchanged
        Ok(#(stream_id, stream)),
      )

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

      // Don't update closed streams
      use <- bool.guard(
        stream.state != Open && stream.state != HalfClosedLocal,
        // return unchanged
        Ok(#(stream_id, stream)),
      )

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

fn from_frame_setting(
  setting: h2_frame.Setting,
) -> Result(option.Option(Setting), Nil) {
  case setting {
    h2_frame.HeaderTableSize(v) -> Ok(option.Some(HeaderTableSize(v)))
    h2_frame.EnablePush(v) ->
      case v {
        0 -> Ok(option.Some(EnablePush(False)))
        1 -> Ok(option.Some(EnablePush(True)))
        _ -> Error(Nil)
      }
    h2_frame.MaxConcurrentStreams(v) -> Ok(option.Some(MaxConcurrentStreams(v)))
    h2_frame.InitialWindowSize(v) -> Ok(option.Some(InitialWindowSize(v)))
    h2_frame.MaxFrameSize(v) -> Ok(option.Some(MaxFrameSize(v)))
    h2_frame.MaxHeaderListSize(v) -> Ok(option.Some(MaxHeaderListSize(v)))
    h2_frame.UnknownSetting(_, _) -> Ok(option.None)
  }
}

fn from_frame_settings(
  settings: List(h2_frame.Setting),
) -> Result(List(Setting), Nil) {
  case list.try_map(settings, from_frame_setting) {
    Ok(settings_list) -> {
      // Filter out unknown settings, which is option.None
      Ok(list.filter_map(settings_list, option.to_result(_, Nil)))
    }
    Error(e) -> Error(e)
  }
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
        False ->
          apply_settings(
            role,
            Settings(..settings, enable_push: False),
            rest,
            remote:,
          )
        True -> {
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

/// Returns the default HTTP/2 settings as defined in
/// [RFC 9113 Section 6.5.2](https://www.rfc-editor.org/rfc/rfc9113#section-6.5.2).
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

/// Events returned by `receive_data` describing what was received from the peer.
/// Multiple events may be produced from a single `receive_data` call.
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

type PendingHeaderBlock {
  PendingHeaders(stream_id: Int, end_stream: Bool, fragment: BitArray)
  PendingPushPromise(
    stream_id: Int,
    promised_stream_id: Int,
    fragment: BitArray,
  )
}

/// An opaque HTTP/2 connection state machine. Created with `new_connection`
/// and threaded through the `send_*` and `receive_data` functions.
pub opaque type Connection {
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
    hpack_encoder: alpacki.DynamicTable,
    hpack_decoder: alpacki.DynamicTable,
    pending_header_blocks: option.Option(PendingHeaderBlock),
  )
}

/// Creates a new HTTP/2 connection for the given role and initial settings.
///
/// Returns the connection and the preface bytes that MUST be sent to the peer
/// before any other data. The provided settings are advertised to the peer in
/// the preface SETTINGS frame but do not take effect locally until a
/// `SettingsAcknowledged` event is received.
///
/// Errors:
/// - `InvalidSettings` - a settings value is outside the allowed range
/// - `FrameEncodingError` - frame encoding failed unexpectedly; if you encounter this, please open an issue
pub fn new_connection(
  role role: Role,
  settings settings: Settings,
) -> Result(#(Connection, BitArray), SendError) {
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
      hpack_encoder: alpacki.new_dynamic(4096),
      hpack_decoder: alpacki.new_dynamic(4096),
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

fn count_outbound_streams(conn: Connection) -> Int {
  dict.fold(conn.streams, 0, fn(count, stream_id, stream) {
    let is_outbound_stream = case conn.role {
      Server -> stream_id % 2 == 0
      Client -> stream_id % 2 == 1
    }

    case is_outbound_stream, stream.state {
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

/// Controls how a header field is represented in the HPACK dynamic table.
/// `WithIndexing` adds the field to the table, `WithoutIndexing` does not,
/// and `NeverIndexed` signals that the value is sensitive and must never be
/// indexed (e.g. authentication tokens).
pub type Indexing {
  WithIndexing
  WithoutIndexing
  NeverIndexed
}

/// An HTTP/2 header field, as defined by
/// [RFC 9113 Section 8.2](https://www.rfc-editor.org/rfc/rfc9113#section-8.2).
pub type Header {
  Header(
    /// The field name. Must be lowercase ASCII per RFC 9113 Section 8.2.
    name: String,
    /// The field value. Represented as a `BitArray` because RFC 9113 Section
    /// 8.2 permits any octet except NUL, CR, and LF - which may not be valid
    /// UTF-8.
    value: BitArray,
    indexing: Indexing,
  )
}

fn extract_status_code(headers: List(Header)) -> Result(Int, Nil) {
  case headers {
    [] -> Error(Nil)
    [header, ..rest] -> {
      case header.name {
        ":status" ->
          header.value
          |> bit_array.to_string
          |> result.try(int.parse)
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
        "content-length" ->
          header.value
          |> bit_array.to_string
          |> result.try(int.parse)
          |> result.map(option.Some)
        _ -> extract_content_length(rest)
      }
    }
  }
}

fn extract_method(headers: List(Header)) -> option.Option(String) {
  case headers {
    [] -> option.None
    [header, ..rest] -> {
      case header.name {
        ":method" ->
          header.value
          |> bit_array.to_string
          |> option.from_result
        _ -> extract_method(rest)
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
        <<"CONNECT":utf8>> -> {
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
          use <- bool.guard(path.value == <<>>, Error(Nil))

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

fn validate_header_name(name: BitArray) -> Result(Nil, Nil) {
  case name {
    // Name cannot be empty
    <<>> -> Error(Nil)
    // First char might be a ":", but that is not allowed after first byte
    <<":", rest:bits>> -> check_header_name_bytes(rest)
    _ -> check_header_name_bytes(name)
  }
}

fn check_header_name_bytes(name: BitArray) -> Result(Nil, Nil) {
  case name {
    <<>> -> Ok(Nil)
    <<byte, rest:bits>> -> {
      case byte {
        b if b >= 0x21 && b <= 0x39 -> check_header_name_bytes(rest)
        // '!' to '9', excluding nothing
        b if b >= 0x3b && b <= 0x40 -> check_header_name_bytes(rest)
        // ';' to '@', skipping ':'
        b if b >= 0x5b && b <= 0x7e -> check_header_name_bytes(rest)
        // '[' to '~', skipping uppercase
        _ -> Error(Nil)
      }
    }
    _ -> Error(Nil)
  }
}

fn validate_header_value(header: Header) -> Result(Nil, Nil) {
  use _ <- result.try(check_header_value_bytes(header.value))
  // RFC 9113 Section 8.2.1: a field value MUST NOT start or end with ASCII
  // whitespace (SP 0x20 or HTAB 0x09).
  use <- bool.guard(
    bit_array.starts_with(header.value, <<0x20>>)
      || bit_array.starts_with(header.value, <<0x09>>),
    Error(Nil),
  )
  let size = bit_array.byte_size(header.value)
  use <- bool.guard(
    size > 0
      && {
      let last = bit_array.slice(header.value, size - 1, 1)
      last == Ok(<<0x20>>) || last == Ok(<<0x09>>)
    },
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
      // Validate header name content
      use _ <- result.try(validate_header_name(<<header.name:utf8>>))

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

      use <- bool.guard(
        is_pseudo
          && !list.contains(
          case role {
            Server -> [":method", ":scheme", ":path", ":authority", ":protocol"]
            Client -> [":status"]
          },
          header.name,
        ),
        Error(Nil),
      )

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
        !is_pseudo && header.name == "te" && header.value != <<"trailers":utf8>>,
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
    name: <<header.name:utf8>>,
    value: header.value,
    indexing: alpacki_indexing,
  )
}

fn from_alpacki_header(header: alpacki.HeaderField) -> Result(Header, Nil) {
  let indexing = case header.indexing {
    alpacki.WithIndexing -> WithIndexing
    alpacki.WithoutIndexing -> WithoutIndexing
    alpacki.NeverIndexed -> NeverIndexed
  }
  use name <- result.try(bit_array.to_string(header.name))
  Ok(Header(name: name, value: header.value, indexing: indexing))
}

/// A single settings parameter to send to the peer via `send_settings`.
pub type Setting {
  HeaderTableSize(Int)
  EnablePush(Bool)
  MaxConcurrentStreams(Int)
  InitialWindowSize(Int)
  MaxFrameSize(Int)
  MaxHeaderListSize(Int)
}

fn to_frame_setting(setting: Setting) -> h2_frame.Setting {
  case setting {
    HeaderTableSize(v) -> h2_frame.HeaderTableSize(v)
    EnablePush(v) ->
      h2_frame.EnablePush(case v {
        True -> 1
        False -> 0
      })
    MaxConcurrentStreams(v) -> h2_frame.MaxConcurrentStreams(v)
    InitialWindowSize(v) -> h2_frame.InitialWindowSize(v)
    MaxFrameSize(v) -> h2_frame.MaxFrameSize(v)
    MaxHeaderListSize(v) -> h2_frame.MaxHeaderListSize(v)
  }
}

fn to_frame_settings(settings: List(Setting)) -> List(h2_frame.Setting) {
  list.map(settings, to_frame_setting)
}

/// HTTP/2 error codes as defined in
/// [RFC 9113 Section 7](https://www.rfc-editor.org/rfc/rfc9113#section-7).
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

/// Errors returned by the `send_*` and `receive_data` functions.
/// A `ConnectionError` affects the entire connection - the caller should
/// respond with `send_goaway` and close the connection.
/// A `StreamError` affects only the given stream.
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

pub type SendError {
  // send window exhausted - wait for WINDOW_UPDATE
  FlowControlBlocked
  // data exceeds max_frame_size - split the frame
  FrameTooLarge
  // GOAWAY received - cannot open new streams
  ConnectionDraining
  // stream is in wrong state for this operation
  InvalidStreamState
  // operation not valid for this connection's role
  InvalidRole
  // peer disabled push via SETTINGS
  PushDisabled
  // pseudoheader validation failed
  InvalidHeaders
  // stream does not exist (may have been closed by peer)
  UnknownStream
  // settings values out of allowed range
  InvalidSettings
  // ping payload must be exactly 8 bytes
  InvalidPingPayload
  // window increment must be > 0 and ≤ 2^31-1
  InvalidWindowIncrement
  // frame encoding failed unexpectedly; if you encounter this, please open an issue
  FrameEncodingError
  // the peer's SETTINGS_MAX_CONCURRENT_STREAMS limit has been reached
  StreamRefused
}

fn chunk_bytes(
  bytes: BitArray,
  first_chunk_size: Int,
  rest_chunk_size: Int,
  chunks: List(BitArray),
) -> List(BitArray) {
  case bytes {
    <<>> -> list.reverse(chunks)
    <<b:bytes-size(first_chunk_size), rest:bits>> ->
      chunk_bytes(rest, rest_chunk_size, rest_chunk_size, [b, ..chunks])
    <<b:bits>> ->
      chunk_bytes(<<>>, rest_chunk_size, rest_chunk_size, [b, ..chunks])
  }
}

fn encode_header_continuations(
  chunks: List(BitArray),
  stream_id: Int,
  processed: BitArray,
) -> Result(BitArray, SendError) {
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
        Error(_) -> Error(FrameEncodingError)
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
        Error(_) -> Error(FrameEncodingError)
      }
    }
  }
}

/// Decodes headers using the conns HPACK table, returning the headers
/// and a Connection with updated tables
fn decode_headers(
  conn conn: Connection,
  encoded_headers encoded_headers: BitArray,
) -> Result(#(Connection, List(alpacki.HeaderField)), H2Error) {
  use #(decoded_headers, new_table) <- result.try(
    alpacki.decode_header_block(encoded_headers, conn.hpack_decoder)
    |> result.replace_error(ConnectionError(CompressionError)),
  )

  let conn = Connection(..conn, hpack_decoder: new_table)

  Ok(#(conn, decoded_headers))
}

fn encode_headers(
  conn conn: Connection,
  headers headers: List(Header),
) -> #(Connection, BitArray) {
  // Map headers to alpacki headers for encoding
  let headers = list.map(headers, to_alpacki_header)

  let #(encoded_headers, new_table) =
    alpacki.encode_header_block(headers, conn.hpack_encoder, huffman: True)

  // Add the new table to the conn
  let conn = Connection(..conn, hpack_encoder: new_table)

  #(conn, encoded_headers)
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
      stream_id: promised_stream_id,
      error_code: ProtocolError,
      flow_controlled_length: 0,
      events:,
      to_send:,
    ),
  )

  let request_method = case
    list.find(decoded_headers, fn(h) { h.name == ":method" })
  {
    Ok(header) ->
      option.Some(bit_array.to_string(header.value) |> result.unwrap(""))
    Error(_) -> option.None
  }

  let promised_stream =
    Stream(
      ..new_stream(
        send_window_size: conn.remote_settings.initial_window_size,
        recv_window_size: conn.local_settings.initial_window_size,
      ),
      state: ReservedRemote,
      request_method:,
    )

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
  let is_trailer =
    end_stream
    && is_existing
    && case conn.role {
      Server -> True
      Client ->
        dict.get(conn.streams, stream_id)
        |> result.map(fn(s) { s.final_response_received })
        |> result.unwrap(False)
    }

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
              case
                conn.role,
                existing_stream.final_response_received,
                end_stream
              {
                // RFC 9113 Section 8.1: trailers carry no pseudo-header fields;
                // use 0 as a sentinel to skip the checks below
                Client, True, True -> Ok(0)
                // extract :status from response headers; fails if missing
                Client, _, _ -> {
                  extract_status_code(decoded_headers)
                }
                // servers receive requests, which have no :status
                Server, _, _ -> Ok(0)
              }
            }
            |> result.replace_error(ConnectionError(ProtocolError)),
          )

          // RFC 9113 Section 8.1: informational responses (1xx) with
          // END_STREAM are malformed
          use <- bool.guard(
            conn.role == Client
              && status_code >= 100
              && status_code < 200
              && end_stream,
            handle_rst_stream(
              conn:,
              stream_id:,
              error_code: ProtocolError,
              flow_controlled_length: 0,
              events:,
              to_send:,
            ),
          )

          // RFC 9113 Section 8.1.1 / [HTTP] Section 6.4.1: If content-length
          // is present and end_stream is set on the HEADERS frame, the body
          // length is 0 and must match content-length. A non-zero value is
          // malformed — except for responses defined as having no content:
          // 204, 304, and responses to HEAD requests (RFC 9110 Section 6.4.1).
          let response_content_length = extract_content_length(decoded_headers)
          use <- bool.guard(
            end_stream
              && conn.role == Client
              && status_code != 204
              && status_code != 304
              && existing_stream.request_method != option.Some("HEAD")
              && case response_content_length {
              Ok(option.Some(n)) -> n != 0
              _ -> False
            },
            handle_rst_stream(
              conn:,
              stream_id:,
              error_code: ProtocolError,
              flow_controlled_length: 0,
              events:,
              to_send:,
            ),
          )

          // Set final_response_received if this is a non-1xx response
          let existing_stream = case conn.role == Client && status_code >= 200 {
            True ->
              Stream(
                ..existing_stream,
                final_response_received: True,
                expected_content_length: case response_content_length {
                  Ok(cl) -> cl
                  _ -> option.None
                },
              )
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

          let new_events = [
            HeadersReceived(
              stream_id: stream_id,
              headers: decoded_headers,
              end_stream: end_stream,
            ),
          ]

          let new_events = case end_stream {
            True -> [StreamEnded(stream_id:), ..new_events]
            False -> new_events
          }

          Ok(#(conn, list.flatten([new_events, events]), to_send))
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
          let response_content_length = extract_content_length(decoded_headers)

          // END_STREAM + non-zero content-length = malformed
          // (exempt HEAD responses and 204/304)
          use <- bool.guard(
            end_stream
              && existing_stream.request_method != option.Some("HEAD")
              && case response_content_length {
              Ok(option.Some(n)) -> n != 0
              _ -> False
            },
            handle_rst_stream(
              conn:,
              stream_id:,
              error_code: ProtocolError,
              flow_controlled_length: 0,
              events:,
              to_send:,
            ),
          )

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
                Stream(
                  ..existing_stream,
                  state: new_state,
                  final_response_received: True,
                  expected_content_length: case response_content_length {
                    Ok(cl) -> cl
                    _ -> option.None
                  },
                ),
              ),
            )

          let new_events = [
            HeadersReceived(
              stream_id: stream_id,
              headers: decoded_headers,
              end_stream: end_stream,
            ),
          ]

          let new_events = case end_stream {
            True -> [StreamEnded(stream_id), ..new_events]
            False -> new_events
          }
          Ok(#(conn, list.flatten([new_events, events]), to_send))
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

  let request_method = extract_method(decoded_headers)

  let stream = case end_stream {
    False ->
      Stream(
        ..new_stream(
          send_window_size: conn.remote_settings.initial_window_size,
          recv_window_size: conn.local_settings.initial_window_size,
        ),
        state: Open,
        expected_content_length: content_length,
        request_method:,
      )
    True ->
      Stream(
        ..new_stream(
          send_window_size: conn.remote_settings.initial_window_size,
          recv_window_size: conn.local_settings.initial_window_size,
        ),
        state: HalfClosedRemote,
        expected_content_length: content_length,
        request_method:,
      )
  }

  let conn =
    Connection(
      ..conn,
      last_remote_stream_id: stream_id,
      streams: dict.insert(conn.streams, stream_id, stream),
    )

  // RFC 9113 Section 8.1.1: If content-length is present and end_stream
  // is set on the HEADERS frame (no DATA will follow), the received body
  // length is 0 and must equal content-length. A non-zero content-length
  // is therefore malformed.
  use <- bool.guard(
    end_stream
      && case content_length {
      option.Some(n) -> n != 0
      option.None -> False
    },
    handle_rst_stream(
      conn:,
      stream_id:,
      error_code: ProtocolError,
      flow_controlled_length: 0,
      events:,
      to_send:,
    ),
  )

  let new_events = [
    HeadersReceived(
      stream_id: stream_id,
      headers: decoded_headers,
      end_stream: end_stream,
    ),
  ]

  let new_events = case end_stream {
    True -> [StreamEnded(stream_id:), ..new_events]
    False -> new_events
  }

  Ok(#(conn, list.flatten([new_events, events]), to_send))
}

/// Opens a new stream by sending a HEADERS frame. Returns the updated
/// connection, the bytes to send to the peer, and the new stream ID.
/// Set `end_stream` to `True` to half-close the stream immediately.
///
/// Errors:
/// - `ConnectionDraining` - a GOAWAY has been received; no new streams may be opened
/// - `InvalidRole` - Only clients are allowed to open new streams, servers needs to use send_push_promise
/// - `InvalidHeaders` - the headers fail RFC 9113 validation
/// - `FrameEncodingError` - frame encoding failed unexpectedly; if you encounter this, please open an issue
pub fn open_stream(
  conn conn: Connection,
  headers headers: List(Header),
  end_stream end_stream: Bool,
) -> Result(#(Connection, BitArray, Int), SendError) {
  // Only clients are allowed to open streams
  use <- bool.guard(conn.role == Server, Error(InvalidRole))
  // Not allowed to open streams while in Draining state (we have received a GOAWAY)
  use <- bool.guard(conn.state == Draining, Error(ConnectionDraining))

  // Check MAX_CONCURRENT_STREAMS
  use <- bool.guard(
    case conn.remote_settings.max_concurrent_streams {
      option.Some(max) -> count_outbound_streams(conn) >= max
      option.None -> False
    },
    Error(StreamRefused),
  )

  let stream =
    Stream(
      ..new_stream(
        send_window_size: conn.remote_settings.initial_window_size,
        recv_window_size: conn.local_settings.initial_window_size,
      ),
      request_method: extract_method(headers),
    )
  let #(conn, stream_id) = add_stream(conn, stream)

  // Send initial headers
  case send_headers(conn, stream_id, headers, end_stream) {
    Ok(#(conn, bytes)) -> Ok(#(conn, bytes, stream_id))
    Error(e) -> Error(e)
  }
}

/// Sends a HEADERS frame (and CONTINUATION frames if needed) on an existing
/// stream. Use `open_stream` instead to create a new stream.
/// Set `end_stream` to `True` to half-close the local side of the stream.
///
/// Errors:
/// - `UnknownStream` - the stream does not exist
/// - `InvalidStreamState` - stream is in ReservedRemote, HalfClosedLocal, or Closed state
/// - `InvalidHeaders` - the headers fail RFC 9113 validation
/// - `FrameEncodingError` - frame encoding failed unexpectedly; if you encounter this, please open an issue
pub fn send_headers(
  conn conn: Connection,
  stream_id stream_id: Int,
  headers headers: List(Header),
  end_stream end_stream: Bool,
) -> Result(#(Connection, BitArray), SendError) {
  // headers cannot be sent on the connection level, it must be on a stream
  use <- bool.guard(stream_id == 0, Error(UnknownStream))

  // headers must be sent on a existing stream
  use stream <- result.try(
    dict.get(conn.streams, stream_id)
    |> result.replace_error(UnknownStream),
  )

  // headers cannot be sent on a ReservedRemote stream, those are initiated by the other party!
  use <- bool.guard(stream.state == ReservedRemote, Error(InvalidStreamState))

  // Headers must not be sent on a stream we have initiated closing of or a closed stream
  use <- bool.guard(
    stream.state == HalfClosedLocal || stream.state == Closed,
    Error(InvalidStreamState),
  )

  // Validate outbound headers - clients send requests (validate with
  // Server rules), servers send responses (validate with Client rules).
  // RFC 9113 Section 8.3.1 / 8.3.2.
  let outbound_role = case conn.role {
    Server -> Client
    Client -> Server
  }
  use <- bool.guard(
    validate_headers(outbound_role, headers, stream.headers_sent) == Error(Nil),
    Error(InvalidHeaders),
  )

  let #(conn, encoded_headers) = encode_headers(conn, headers)

  let new_state = case stream.state {
    HalfClosedRemote -> {
      case end_stream {
        True -> Closed
        False -> stream.state
      }
    }
    ReservedLocal ->
      case end_stream {
        True -> Closed
        False -> HalfClosedRemote
      }
    _ ->
      case end_stream {
        True -> HalfClosedLocal
        False -> Open
      }
  }

  let stream = Stream(..stream, state: new_state, headers_sent: True)

  let conn =
    Connection(..conn, streams: dict.insert(conn.streams, stream_id, stream))

  case
    chunk_bytes(
      encoded_headers,
      conn.remote_settings.max_frame_size,
      conn.remote_settings.max_frame_size,
      [],
    )
  {
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
        |> result.replace_error(FrameEncodingError),
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
        |> result.replace_error(FrameEncodingError),
      )
      use continuation_frames <- result.try(
        encode_header_continuations(rest, stream_id, <<>>),
      )
      Ok(#(conn, <<headers_frame:bits, continuation_frames:bits>>))
    }
  }
}

/// Sends a PUSH_PROMISE frame on an existing stream (server only).
/// Returns the updated connection, the bytes to send, and the promised
/// stream ID. The promised stream is created in the reserved (local) state.
///
/// Errors:
/// - `ConnectionDraining` - a GOAWAY has been received; no new streams may be opened
/// - `InvalidRole` - called on a client connection; only servers may send PUSH_PROMISE
/// - `UnknownStream` - the stream does not exist
/// - `InvalidStreamState` - stream is not in Open or HalfClosedRemote state
/// - `PushDisabled` - the peer has disabled push via SETTINGS
/// - `InvalidHeaders` - the headers fail RFC 9113 Section 8.4.1 validation (must be a valid request field block with `:method`, `:scheme`, and `:path`)
/// - `FrameEncodingError` - frame encoding failed unexpectedly; if you encounter this, please open an issue
pub fn send_push_promise(
  conn conn: Connection,
  stream_id stream_id: Int,
  headers headers: List(Header),
) -> Result(#(Connection, BitArray, Int), SendError) {
  // Not allowed to open streams while in Draining state (we have received a GOAWAY)
  use <- bool.guard(conn.state == Draining, Error(ConnectionDraining))

  // Must only be sent by server
  use <- bool.guard(conn.role == Client, Error(InvalidRole))

  // Must not be sent on stream 0 (the connection level)
  use <- bool.guard(stream_id == 0, Error(UnknownStream))

  // Stream must exist
  use stream <- result.try(
    dict.get(conn.streams, stream_id)
    |> result.replace_error(UnknownStream),
  )

  use <- bool.guard(
    stream.state != Open && stream.state != HalfClosedRemote,
    Error(InvalidStreamState),
  )

  use <- bool.guard(!conn.remote_settings.enable_push, Error(PushDisabled))

  use <- bool.guard(
    validate_headers(Server, headers, False) == Error(Nil),
    Error(InvalidHeaders),
  )

  let #(conn, promised_stream_id) =
    add_stream(
      conn,
      Stream(
        ..new_stream(
          send_window_size: conn.remote_settings.initial_window_size,
          recv_window_size: conn.local_settings.initial_window_size,
        ),
        state: ReservedLocal,
        request_method: extract_method(headers),
      ),
    )

  let #(conn, encoded_headers) = encode_headers(conn: conn, headers: headers)

  case
    chunk_bytes(
      encoded_headers,
      conn.remote_settings.max_frame_size - 4,
      conn.remote_settings.max_frame_size,
      [],
    )
  {
    [] -> {
      use push_promise_frame <- result.try(
        h2_frame.encode_push_promise(
          stream_id: stream_id,
          promised_stream_id: promised_stream_id,
          end_headers: True,
          field_block_fragment: <<>>,
          padding: option.None,
        )
        |> result.replace_error(FrameEncodingError),
      )
      Ok(#(conn, push_promise_frame, promised_stream_id))
    }

    [header_chunk, ..rest] -> {
      use push_promise_frame <- result.try(
        h2_frame.encode_push_promise(
          stream_id: stream_id,
          promised_stream_id: promised_stream_id,
          end_headers: rest == [],
          field_block_fragment: header_chunk,
          padding: option.None,
        )
        |> result.replace_error(FrameEncodingError),
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

/// Sends a DATA frame on an open stream. The data must fit within both the
/// stream and connection flow control windows - use `get_send_window_size`
/// to check available capacity before calling.
/// Set `end_stream` to `True` to half-close the local side of the stream.
///
/// Errors:
/// - `UnknownStream` - the stream does not exist
/// - `InvalidStreamState` - stream is in HalfClosedLocal, Closed, ReservedLocal, or ReservedRemote state
/// - `FrameTooLarge` - payload exceeds the peer's max_frame_size; split the data and retry
/// - `FlowControlBlocked` - payload exceeds the available send window; wait for a WINDOW_UPDATE
/// - `FrameEncodingError` - frame encoding failed unexpectedly; if you encounter this, please open an issue
pub fn send_data(
  conn conn: Connection,
  stream_id stream_id: Int,
  data data: BitArray,
  end_stream end_stream: Bool,
  padding padding: option.Option(Int),
) -> Result(#(Connection, BitArray), SendError) {
  use <- bool.guard(stream_id == 0, Error(UnknownStream))

  use stream <- result.try(case dict.get(conn.streams, stream_id) {
    Ok(stream) -> {
      use <- bool.guard(
        stream.state == HalfClosedLocal || stream.state == Closed,
        Error(InvalidStreamState),
      )
      use <- bool.guard(
        stream.state == ReservedLocal || stream.state == ReservedRemote,
        Error(InvalidStreamState),
      )
      Ok(stream)
    }
    Error(Nil) -> Error(UnknownStream)
  })

  use max_allowed_window_size <- result.try(
    get_send_window_size(conn: conn, stream_id: stream_id)
    |> result.replace_error(InvalidStreamState),
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
    Error(FrameTooLarge),
  )

  use <- bool.guard(
    payload_length > max_allowed_window_size,
    Error(FlowControlBlocked),
  )

  use encoded_frame <- result.try(
    h2_frame.encode_data(
      stream_id: stream_id,
      end_stream: end_stream,
      data: data,
      padding: padding,
    )
    |> result.replace_error(FrameEncodingError),
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

/// Returns the number of bytes that can be sent on the given stream,
/// accounting for both the stream and connection flow control windows.
/// Returns `Error(Nil)` if the stream does not exist.
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

@internal
pub fn get_stream_state(
  conn conn: Connection,
  stream_id stream_id: Int,
) -> Result(StreamState, Nil) {
  case dict.get(conn.streams, stream_id) {
    Ok(stream) -> Ok(stream.state)
    Error(Nil) -> Error(Nil)
  }
}

@internal
pub fn get_stream_send_window_size(
  conn conn: Connection,
  stream_id stream_id: Int,
) -> Result(Int, Nil) {
  case dict.get(conn.streams, stream_id) {
    Ok(stream) -> Ok(stream.send_window_size)
    Error(Nil) -> Error(Nil)
  }
}

@internal
pub fn get_stream_recv_window_size(
  conn conn: Connection,
  stream_id stream_id: Int,
) -> Result(Int, Nil) {
  case dict.get(conn.streams, stream_id) {
    Ok(stream) -> Ok(stream.recv_window_size)
    Error(Nil) -> Error(Nil)
  }
}

@internal
pub fn get_connection_state(conn conn: Connection) -> ConnectionState {
  conn.state
}

@internal
pub fn get_connection_send_window_size(conn conn: Connection) -> Int {
  conn.send_window_size
}

@internal
pub fn get_connection_recv_window_size(conn conn: Connection) -> Int {
  conn.recv_window_size
}

@internal
pub fn get_remote_settings(conn conn: Connection) -> Settings {
  conn.remote_settings
}

@internal
pub fn get_local_settings(conn conn: Connection) -> Settings {
  conn.local_settings
}

@internal
pub fn get_pending_settings(conn conn: Connection) -> List(List(Setting)) {
  conn.pending_settings
}

@internal
pub fn get_last_remote_stream_id(conn conn: Connection) -> Int {
  conn.last_remote_stream_id
}

@internal
pub fn get_role(conn conn: Connection) -> Role {
  conn.role
}

@internal
pub fn get_recv_buffer(conn conn: Connection) -> BitArray {
  conn.recv_buffer
}

/// Sends a SETTINGS frame with the given parameters to the peer.
/// The settings take effect once the peer acknowledges them.
///
/// Errors:
/// - `InvalidSettings` - a settings value is outside the allowed range
/// - `FrameEncodingError` - frame encoding failed unexpectedly; if you encounter this, please open an issue
pub fn send_settings(
  conn conn: Connection,
  settings settings: List(Setting),
) -> Result(#(Connection, BitArray), SendError) {
  use _ <- result.try(validate_setting_list(settings))

  let conn =
    Connection(
      ..conn,
      pending_settings: list.append(conn.pending_settings, [settings]),
    )

  use encoded <- result.try(
    h2_frame.encode_settings(ack: False, settings: to_frame_settings(settings))
    |> result.replace_error(FrameEncodingError),
  )
  Ok(#(conn, encoded))
}

fn validate_setting_list(settings: List(Setting)) -> Result(Nil, SendError) {
  case settings {
    [] -> Ok(Nil)
    [setting, ..rest] -> {
      use _ <- result.try(validate_setting(setting))
      validate_setting_list(rest)
    }
  }
}

/// Sends a PING frame with the given 8-byte opaque data.
/// The peer will respond with a PING ACK containing the same data.
///
/// Errors:
/// - `InvalidPingPayload` - data must be exactly 8 bytes
/// - `FrameEncodingError` - frame encoding failed unexpectedly; if you encounter this, please open an issue
pub fn send_ping(
  conn conn: Connection,
  data data: BitArray,
) -> Result(#(Connection, BitArray), SendError) {
  use <- bool.guard(bit_array.byte_size(data) != 8, Error(InvalidPingPayload))

  use encoded <- result.try(
    h2_frame.encode_ping(ack: False, data: data)
    |> result.replace_error(FrameEncodingError),
  )
  Ok(#(conn, encoded))
}

/// Sends a GOAWAY frame to initiate a graceful connection shutdown.
/// The last stream ID is set automatically based on the highest
/// peer-initiated stream the connection has seen.
pub fn send_goaway(
  conn conn: Connection,
  error_code error_code: ErrorCode,
  debug_data debug_data: BitArray,
) -> #(Connection, BitArray) {
  let encoded_frame =
    h2_frame.encode_goaway(
      last_stream_id: conn.last_remote_stream_id,
      error_code: to_frame_error_code(error_code),
      debug_data: debug_data,
    )
  #(Connection(..conn, state: Draining), encoded_frame)
}

/// Sends WINDOW_UPDATE frames to replenish the flow control windows for
/// both the given stream and the connection by `window_size_increment` bytes.
/// Call this after consuming received DATA to allow the peer to send more.
///
/// Errors:
/// - `UnknownStream` - the stream does not exist
/// - `InvalidStreamState` - stream is in Closed, ReservedRemote, or ReservedLocal state
/// - `InvalidWindowIncrement` - increment must be > 0, and must not cause the receive window to exceed 2^31-1
/// - `FrameEncodingError` - frame encoding failed unexpectedly; if you encounter this, please open an issue
pub fn acknowledge_data(
  conn conn: Connection,
  stream_id stream_id: Int,
  window_size_increment window_size_increment: Int,
) -> Result(#(Connection, BitArray), SendError) {
  use stream <- result.try(
    dict.get(conn.streams, stream_id)
    |> result.replace_error(UnknownStream),
  )

  use <- bool.guard(
    window_size_increment <= 0 || window_size_increment > 2_147_483_647,
    Error(InvalidWindowIncrement),
  )

  use <- bool.guard(
    stream.state == Closed
      || stream.state == ReservedRemote
      || stream.state == ReservedLocal,
    Error(InvalidStreamState),
  )

  let stream =
    Stream(
      ..stream,
      recv_window_size: stream.recv_window_size + window_size_increment,
    )

  use <- bool.guard(
    stream.recv_window_size > 2_147_483_647,
    Error(InvalidWindowIncrement),
  )

  let conn =
    Connection(
      ..conn,
      streams: dict.insert(conn.streams, stream_id, stream),
      recv_window_size: conn.recv_window_size + window_size_increment,
    )

  use <- bool.guard(
    conn.recv_window_size > 2_147_483_647,
    Error(InvalidWindowIncrement),
  )

  // Generate both the connection level and stream level stream update
  use connection_update_frame <- result.try(
    h2_frame.encode_window_update(
      stream_id: 0,
      window_size_increment: window_size_increment,
    )
    |> result.replace_error(FrameEncodingError),
  )

  use stream_update_frame <- result.try(
    h2_frame.encode_window_update(
      stream_id: stream_id,
      window_size_increment: window_size_increment,
    )
    |> result.replace_error(FrameEncodingError),
  )

  Ok(#(conn, <<connection_update_frame:bits, stream_update_frame:bits>>))
}

/// Sends a RST_STREAM frame to immediately terminate the given stream.
/// The stream transitions to the closed state.
///
/// Errors:
/// - `UnknownStream` - the stream does not exist
/// - `InvalidStreamState` - stream is in Idle or Closed state
/// - `FrameEncodingError` - frame encoding failed unexpectedly; if you encounter this, please open an issue
pub fn send_rst_stream(
  conn conn: Connection,
  stream_id stream_id: Int,
  error_code error_code: ErrorCode,
) -> Result(#(Connection, BitArray), SendError) {
  // Must be sent on a existing stream
  use stream <- result.try(
    dict.get(conn.streams, stream_id)
    |> result.replace_error(UnknownStream),
  )

  // Must not be sent on a idle stream
  use <- bool.guard(stream.state == Idle, Error(InvalidStreamState))

  use <- bool.guard(stream.state == Closed, Error(InvalidStreamState))

  // Close the stream
  let stream = Stream(..stream, state: Closed, closed_by_rst: True)

  let conn =
    Connection(..conn, streams: dict.insert(conn.streams, stream_id, stream))

  case
    h2_frame.encode_rst_stream(
      stream_id: stream_id,
      error_code: to_frame_error_code(error_code),
    )
  {
    Ok(encoded_frame) -> Ok(#(conn, encoded_frame))
    Error(_) -> Error(FrameEncodingError)
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

  // Set stream state to closed if it is tracked locally
  let conn = case dict.get(conn.streams, stream_id) {
    Ok(stream) -> {
      Connection(
        ..conn,
        streams: dict.insert(
          conn.streams,
          stream_id,
          Stream(..stream, state: Closed),
        ),
      )
    }
    Error(Nil) -> conn
  }

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
        #(
          Connection(
            ..conn,
            recv_window_size: conn.recv_window_size + flow_controlled_length,
          ),
          [StreamReset(stream_id:, error_code:), ..events],
          <<
            to_send:bits,
            encoded_rst_stream_frame:bits,
            encoded_window_update:bits,
          >>,
        ),
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
                          case decode_headers(conn, combined) {
                            Error(err) -> Error(err)
                            Ok(#(conn, decoded_headers)) -> {
                              let conn =
                                Connection(
                                  ..conn,
                                  pending_header_blocks: option.None,
                                )

                              case
                                list.try_map(
                                  decoded_headers,
                                  from_alpacki_header,
                                )
                              {
                                Error(_) -> {
                                  use #(conn, events, to_send) <- result.try(
                                    handle_rst_stream(
                                      conn,
                                      stream_id,
                                      ProtocolError,
                                      0,
                                      events,
                                      to_send,
                                    ),
                                  )
                                  parse_loop(conn, events, to_send)
                                }
                                Ok(parsed_headers) -> {
                                  use #(conn, events, to_send) <- result.try(
                                    handle_decoded_headers(
                                      conn,
                                      stream_id,
                                      end_stream,
                                      parsed_headers,
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
                          case decode_headers(conn, combined) {
                            Error(err) -> Error(err)
                            Ok(#(conn, decoded_headers)) -> {
                              let conn =
                                Connection(
                                  ..conn,
                                  pending_header_blocks: option.None,
                                )

                              case
                                list.try_map(
                                  decoded_headers,
                                  from_alpacki_header,
                                )
                              {
                                Error(_) -> {
                                  use #(conn, events, to_send) <- result.try(
                                    handle_rst_stream(
                                      conn,
                                      stream_id,
                                      ProtocolError,
                                      0,
                                      events,
                                      to_send,
                                    ),
                                  )
                                  parse_loop(conn, events, to_send)
                                }
                                Ok(parsed_headers) -> {
                                  use #(conn, events, to_send) <- result.try(
                                    handle_decoded_push_promise(
                                      conn,
                                      stream_id,
                                      promised_stream_id,
                                      parsed_headers,
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
              use settings <- result.try(
                from_frame_settings(frame_settings)
                |> result.replace_error(ConnectionError(ProtocolError)),
              )
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

                  // Trigger a hpack table resize if we have a new table size
                  let conn = case
                    new_settings.header_table_size
                    == old_settings.header_table_size
                  {
                    True -> conn
                    False ->
                      Connection(
                        ..conn,
                        hpack_encoder: alpacki.resize_dynamic(
                          conn.hpack_encoder,
                          new_settings.header_table_size,
                        ),
                      )
                  }

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

                      // Check if we should update the header table size
                      let conn = case
                        new_settings.header_table_size
                        != old_settings.header_table_size
                      {
                        True ->
                          Connection(
                            ..conn,
                            hpack_decoder: alpacki.expect_table_size_update(
                              conn.hpack_decoder,
                            ),
                          )
                        False -> conn
                      }

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

              let stream = Stream(..stream, state: Closed, closed_by_rst: True)

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
                  case decode_headers(conn, field_block_fragment) {
                    Error(err) -> Error(err)
                    Ok(#(conn, decoded_headers)) -> {
                      case list.try_map(decoded_headers, from_alpacki_header) {
                        Error(_) -> {
                          use #(conn, events, to_send) <- result.try(
                            handle_rst_stream(
                              conn,
                              stream_id,
                              ProtocolError,
                              0,
                              events,
                              to_send,
                            ),
                          )
                          parse_loop(conn, events, to_send)
                        }
                        Ok(parsed_headers) -> {
                          use #(conn, events, to_send) <- result.try(
                            handle_decoded_headers(
                              conn,
                              stream_id,
                              end_stream,
                              parsed_headers,
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

              let conn =
                Connection(..conn, recv_window_size: new_conn_recv_window)

              use <- bool.guard(
                stream.state == Closed,
                handle_rst_stream(
                  conn: conn,
                  stream_id: stream_id,
                  error_code: StreamClosed,
                  flow_controlled_length: payload_length,
                  events: events,
                  to_send: to_send,
                ),
              )

              let new_stream_state = case end_stream {
                True -> {
                  case stream.state {
                    HalfClosedLocal -> Closed
                    _ -> HalfClosedRemote
                  }
                }
                False -> stream.state
              }

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

              let new_events = [
                DataReceived(
                  stream_id: stream_id,
                  data: data,
                  end_stream: end_stream,
                  flow_controlled_length: payload_length,
                ),
              ]

              let new_events = case end_stream {
                False -> new_events
                True -> [StreamEnded(stream_id:), ..new_events]
              }

              parse_loop(conn, list.flatten([new_events, events]), to_send)
            }

            // PUSH_PROMISE
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

              // If the stream was closed naturally (END_STREAM), PUSH_PROMISE is a PROTOCOL_ERROR.
              // If it was closed by RST_STREAM (either side), treat as a race condition and silently ignore.
              use <- bool.guard(
                stream.state == Closed && stream.closed_by_rst,
                parse_loop(conn, events, to_send),
              )

              // Stream state must be Open or HalfClosedLocal
              use <- bool.guard(
                !list.contains([Open, HalfClosedLocal], stream.state),
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
                  case decode_headers(conn, field_block_fragment) {
                    Error(err) -> Error(err)
                    Ok(#(conn, decoded_headers)) -> {
                      case list.try_map(decoded_headers, from_alpacki_header) {
                        Error(_) -> {
                          use #(conn, events, to_send) <- result.try(
                            handle_rst_stream(
                              conn,
                              stream_id,
                              ProtocolError,
                              0,
                              events,
                              to_send,
                            ),
                          )
                          parse_loop(conn, events, to_send)
                        }
                        Ok(parsed_headers) -> {
                          use #(conn, events, to_send) <- result.try(
                            handle_decoded_push_promise(
                              conn,
                              stream_id,
                              promised_stream_id,
                              parsed_headers,
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

    _ -> Error(ConnectionError(InternalError))
  }
}

const client_preface_magic = <<"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n":utf8>>

/// Process incoming bytes from the peer.
///
/// Returns the updated connection, a list of events describing what was
/// received, and a `BitArray` of bytes that **must be sent to the peer**
/// before any other data. The bytes-to-send contain protocol-level responses
/// that h2_core generates automatically, such as SETTINGS ACKs and PING ACKs.
/// Failing to forward them will cause the peer to consider the connection
/// broken.
///
/// The events list is in the order the frames were received. A single call may
/// produce multiple events.
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
