import gleam/bit_array
import gleam/list
import gleam/option.{None}
import h2_core.{
  Client, Header, InvalidHeaders, InvalidRole, Server, WithIndexing, open_stream,
  send_headers,
}
import h2_core/internal/stream.{HalfClosedLocal, Open}
import h2_frame
import helper

// RFC 9113 Section 6.2 - Sending HEADERS

// open_stream encodes headers and produces a HEADERS frame
pub fn open_stream_produces_encoded_frame_test() {
  let conn = helper.connected_connection(Client)
  let headers = [
    Header(":method", <<"GET":utf8>>, WithIndexing),
    Header(":path", <<"/":utf8>>, WithIndexing),
    Header(":scheme", <<"https":utf8>>, WithIndexing),
  ]
  let assert Ok(#(_conn, to_send, stream_id)) =
    open_stream(conn, headers, False)
  let assert Ok(#(frame_data, _)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(frame) = h2_frame.decode_frame(frame_data)
  let assert h2_frame.Headers(stream_id: sid, end_stream: False, ..) = frame
  assert sid == stream_id
}

// open_stream opens a new stream in Open state
pub fn open_stream_opens_stream_test() {
  let conn = helper.connected_connection(Client)
  let headers = helper.request_headers()
  let assert Ok(#(conn, _to_send, _stream_id)) =
    open_stream(conn, headers, False)
  let assert Ok(Open) = h2_core.get_stream_state(conn, 1)
}

// open_stream increments next_stream_id by 2
pub fn open_stream_increments_stream_id_test() {
  let conn = helper.connected_connection(Client)
  let headers = helper.request_headers()
  let assert Ok(#(conn, _to_send, stream_id)) =
    open_stream(conn, headers, False)
  assert stream_id == 1
  let assert Ok(#(_conn, _to_send, stream_id)) =
    open_stream(conn, headers, False)
  assert stream_id == 3
}

// RFC 9113 Section 5.1.1 - "A HEADERS frame will transition the
// client-initiated stream identified by the stream identifier in the
// frame header from 'idle' to 'open'. A PUSH_PROMISE frame will
// transition the server-initiated stream."
// Servers must not open streams via HEADERS; open_stream is client-only.
pub fn open_stream_on_server_is_invalid_role_test() {
  let conn = helper.connected_connection(Server)
  let assert Error(InvalidRole) =
    open_stream(conn, [Header(":status", <<"200":utf8>>, WithIndexing)], False)
}

// RFC 9113 Section 5.1.1 - Server-initiated streams use even-numbered
// identifiers, allocated via PUSH_PROMISE.
pub fn send_push_promise_server_uses_even_stream_ids_test() {
  let #(server, _client) = helper.server_with_open_stream()
  let assert Ok(#(_server, _to_send, promised_id)) =
    h2_core.send_push_promise(server, 1, helper.request_headers())
  assert promised_id == 2
}

// RFC 9113 Section 6.2 - END_STREAM flag transitions stream to half-closed (local)
pub fn open_stream_with_end_stream_test() {
  let conn = helper.connected_connection(Client)
  let headers = helper.request_headers()
  let assert Ok(#(conn, _to_send, _stream_id)) =
    open_stream(conn, headers, True)
  let assert Ok(HalfClosedLocal) = h2_core.get_stream_state(conn, 1)
}

// The encoded frame can be parsed back by h2_frame
pub fn open_stream_produces_parseable_frame_test() {
  let conn = helper.connected_connection(Client)
  let headers = helper.request_headers()
  let assert Ok(#(_conn, to_send, _stream_id)) =
    open_stream(conn, headers, False)
  // Parse the frame back
  let assert Ok(#(frame_data, _rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(frame) = h2_frame.decode_frame(frame_data)
  // Should be a Headers frame on stream 1
  let assert h2_frame.Headers(
    stream_id: 1,
    end_stream: False,
    end_headers: True,
    priority: None,
    field_block_fragment: _,
  ) = frame
}

// END_STREAM is reflected in the encoded frame
pub fn open_stream_end_stream_in_frame_test() {
  let conn = helper.connected_connection(Client)
  let headers = helper.request_headers()
  let assert Ok(#(_conn, to_send, _stream_id)) =
    open_stream(conn, headers, True)
  let assert Ok(#(frame_data, _rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(frame) = h2_frame.decode_frame(frame_data)
  let assert h2_frame.Headers(
    stream_id: 1,
    end_stream: True,
    end_headers: _,
    priority: _,
    field_block_fragment: _,
  ) = frame
}

// HPACK encoder state is updated after sending headers
pub fn open_stream_updates_hpack_encoder_test() {
  let conn = helper.connected_connection(Client)
  let headers =
    list.append(helper.request_headers(), [
      Header("custom-header", <<"custom-value":utf8>>, WithIndexing),
    ])
  let assert Ok(#(conn, first_send, _stream_id)) =
    open_stream(conn, headers, False)
  // Send the same headers again - HPACK should produce smaller output
  // because "custom-header: custom-value" is now in the dynamic table
  let assert Ok(#(_conn, second_send, _stream_id)) =
    open_stream(conn, headers, False)
  assert bit_array.byte_size(second_send) < bit_array.byte_size(first_send)
}

// RFC 9113 Section 8.3.1 - "All HTTP/2 requests MUST include exactly
// one valid value for the ':method', ':scheme', and ':path'
// pseudo-header fields."
//
// Sending headers with empty list is missing mandatory pseudo-headers.
pub fn open_stream_empty_headers_is_error_test() {
  let conn = helper.connected_connection(Client)
  let assert Error(InvalidHeaders) = open_stream(conn, [], False)
}

// RFC 9113 Section 8.3.1 - Missing :scheme is malformed.
pub fn open_stream_missing_scheme_is_error_test() {
  let conn = helper.connected_connection(Client)
  let assert Error(InvalidHeaders) =
    open_stream(
      conn,
      [
        Header(":method", <<"GET":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
      ],
      False,
    )
}

// RFC 9113 Section 8.3.1 - Missing :path is malformed.
pub fn open_stream_missing_path_is_error_test() {
  let conn = helper.connected_connection(Client)
  let assert Error(InvalidHeaders) =
    open_stream(
      conn,
      [
        Header(":method", <<"GET":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
      ],
      False,
    )
}

// RFC 9113 Section 8.3.1 - Missing :method is malformed.
pub fn open_stream_missing_method_is_error_test() {
  let conn = helper.connected_connection(Client)
  let assert Error(InvalidHeaders) =
    open_stream(
      conn,
      [
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
      ],
      False,
    )
}

// RFC 9113 Section 8.3 - "All pseudo-header fields MUST appear in a
// field block before all regular field lines."
pub fn open_stream_pseudo_after_regular_is_error_test() {
  let conn = helper.connected_connection(Client)
  let assert Error(InvalidHeaders) =
    open_stream(
      conn,
      [
        Header(":method", <<"GET":utf8>>, WithIndexing),
        Header("x-foo", <<"bar":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
      ],
      False,
    )
}

// RFC 9113 Section 8.3 - "The same pseudo-header field name MUST NOT
// appear more than once in a field block."
pub fn open_stream_duplicate_pseudo_is_error_test() {
  let conn = helper.connected_connection(Client)
  let assert Error(InvalidHeaders) =
    open_stream(
      conn,
      [
        Header(":method", <<"GET":utf8>>, WithIndexing),
        Header(":method", <<"POST":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
      ],
      False,
    )
}

// RFC 9113 Section 8.2.2 - "Any message containing connection-specific
// header fields MUST be treated as malformed."
pub fn open_stream_with_connection_header_is_error_test() {
  let conn = helper.connected_connection(Client)
  let assert Error(InvalidHeaders) =
    open_stream(
      conn,
      [
        Header(":method", <<"GET":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
        Header("connection", <<"close":utf8>>, WithIndexing),
      ],
      False,
    )
}

// RFC 9113 Section 8.2.2 - TE header must only have value "trailers".
pub fn open_stream_with_te_non_trailers_is_error_test() {
  let conn = helper.connected_connection(Client)
  let assert Error(InvalidHeaders) =
    open_stream(
      conn,
      [
        Header(":method", <<"GET":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
        Header("te", <<"chunked":utf8>>, WithIndexing),
      ],
      False,
    )
}

// Valid headers should succeed.
pub fn open_stream_with_valid_headers_test() {
  let conn = helper.connected_connection(Client)
  let assert Ok(#(conn, _to_send, _stream_id)) =
    open_stream(conn, helper.request_headers(), False)
  let assert Ok(Open) = h2_core.get_stream_state(conn, 1)
}

// RFC 9113 Section 8.3.2 - Server responses must include :status.
pub fn send_headers_response_missing_status_is_error_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) =
    h2_core.receive_data(server, headers)
  // Server sends response without :status
  let assert Error(InvalidHeaders) =
    send_headers(
      server,
      1,
      [Header("content-type", <<"text/html":utf8>>, WithIndexing)],
      False,
    )
}

// Valid server response should succeed.
pub fn send_headers_response_with_status_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) =
    h2_core.receive_data(server, headers)
  let assert Ok(#(_server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
}
