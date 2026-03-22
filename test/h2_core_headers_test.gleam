import gleam/bit_array
import gleam/dict
import gleam/list
import gleam/option.{None}
import h2_core.{
  Client, Connected, HalfClosedLocal, Header, Open, Server, WithIndexing,
  open_stream, send_headers,
}
import h2_frame
import helper

// RFC 9113 Section 6.2 - Sending HEADERS

// open_stream encodes headers and produces a HEADERS frame
pub fn open_stream_produces_encoded_frame_test() {
  let conn = helper.new_connection(Client, Connected)
  let headers = [
    Header(":method", "GET", WithIndexing),
    Header(":path", "/", WithIndexing),
    Header(":scheme", "https", WithIndexing),
  ]
  let assert Ok(#(_conn, to_send)) = open_stream(conn, headers, False)
  // The output should be non-empty (actual HPACK-encoded HEADERS frame)
  assert to_send != <<>>
}

// open_stream opens a new stream in Open state
pub fn open_stream_opens_stream_test() {
  let conn = helper.new_connection(Client, Connected)
  let headers = helper.request_headers()
  let assert Ok(#(conn, _to_send)) = open_stream(conn, headers, False)
  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == Open
}

// open_stream increments next_stream_id by 2
pub fn open_stream_increments_stream_id_test() {
  let conn = helper.new_connection(Client, Connected)
  let headers = helper.request_headers()
  let assert Ok(#(conn, _to_send)) = open_stream(conn, headers, False)
  assert conn.next_stream_id == 3
  let assert Ok(#(conn, _to_send)) = open_stream(conn, headers, False)
  assert conn.next_stream_id == 5
}

// Server uses even stream IDs
pub fn open_stream_server_uses_even_stream_ids_test() {
  let conn = helper.new_connection(Server, Connected)
  let headers = [Header(":status", "200", WithIndexing)]
  let assert Ok(#(conn, _to_send)) = open_stream(conn, headers, False)
  let assert Ok(_stream) = dict.get(conn.streams, 2)
  assert conn.next_stream_id == 4
}

// RFC 9113 Section 6.2 - END_STREAM flag transitions stream to half-closed (local)
pub fn open_stream_with_end_stream_test() {
  let conn = helper.new_connection(Client, Connected)
  let headers = helper.request_headers()
  let assert Ok(#(conn, _to_send)) = open_stream(conn, headers, True)
  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == HalfClosedLocal
}

// The encoded frame can be parsed back by h2_frame
pub fn open_stream_produces_parseable_frame_test() {
  let conn = helper.new_connection(Client, Connected)
  let headers = helper.request_headers()
  let assert Ok(#(_conn, to_send)) = open_stream(conn, headers, False)
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
  let conn = helper.new_connection(Client, Connected)
  let headers = helper.request_headers()
  let assert Ok(#(_conn, to_send)) = open_stream(conn, headers, True)
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
  let conn = helper.new_connection(Client, Connected)
  let headers =
    list.append(helper.request_headers(), [
      Header("custom-header", "custom-value", WithIndexing),
    ])
  let assert Ok(#(conn, first_send)) = open_stream(conn, headers, False)
  // Send the same headers again - HPACK should produce smaller output
  // because "custom-header: custom-value" is now in the dynamic table
  let assert Ok(#(_conn, second_send)) = open_stream(conn, headers, False)
  assert bit_array.byte_size(second_send) < bit_array.byte_size(first_send)
}

// RFC 9113 Section 8.3.1 - "All HTTP/2 requests MUST include exactly
// one valid value for the ':method', ':scheme', and ':path'
// pseudo-header fields."
//
// Sending headers with empty list is missing mandatory pseudo-headers.
pub fn open_stream_empty_headers_is_error_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Error(_) = open_stream(conn, [], False)
}

// RFC 9113 Section 8.3.1 - Missing :scheme is malformed.
pub fn open_stream_missing_scheme_is_error_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Error(_) =
    open_stream(
      conn,
      [
        Header(":method", "GET", WithIndexing),
        Header(":path", "/", WithIndexing),
      ],
      False,
    )
}

// RFC 9113 Section 8.3.1 - Missing :path is malformed.
pub fn open_stream_missing_path_is_error_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Error(_) =
    open_stream(
      conn,
      [
        Header(":method", "GET", WithIndexing),
        Header(":scheme", "https", WithIndexing),
      ],
      False,
    )
}

// RFC 9113 Section 8.3.1 - Missing :method is malformed.
pub fn open_stream_missing_method_is_error_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Error(_) =
    open_stream(
      conn,
      [
        Header(":scheme", "https", WithIndexing),
        Header(":path", "/", WithIndexing),
      ],
      False,
    )
}

// RFC 9113 Section 8.3 - "All pseudo-header fields MUST appear in a
// field block before all regular field lines."
pub fn open_stream_pseudo_after_regular_is_error_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Error(_) =
    open_stream(
      conn,
      [
        Header(":method", "GET", WithIndexing),
        Header("x-foo", "bar", WithIndexing),
        Header(":scheme", "https", WithIndexing),
        Header(":path", "/", WithIndexing),
      ],
      False,
    )
}

// RFC 9113 Section 8.3 - "The same pseudo-header field name MUST NOT
// appear more than once in a field block."
pub fn open_stream_duplicate_pseudo_is_error_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Error(_) =
    open_stream(
      conn,
      [
        Header(":method", "GET", WithIndexing),
        Header(":method", "POST", WithIndexing),
        Header(":scheme", "https", WithIndexing),
        Header(":path", "/", WithIndexing),
      ],
      False,
    )
}

// RFC 9113 Section 8.2.2 - "Any message containing connection-specific
// header fields MUST be treated as malformed."
pub fn open_stream_with_connection_header_is_error_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Error(_) =
    open_stream(
      conn,
      [
        Header(":method", "GET", WithIndexing),
        Header(":scheme", "https", WithIndexing),
        Header(":path", "/", WithIndexing),
        Header("connection", "close", WithIndexing),
      ],
      False,
    )
}

// RFC 9113 Section 8.2.2 - TE header must only have value "trailers".
pub fn open_stream_with_te_non_trailers_is_error_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Error(_) =
    open_stream(
      conn,
      [
        Header(":method", "GET", WithIndexing),
        Header(":scheme", "https", WithIndexing),
        Header(":path", "/", WithIndexing),
        Header("te", "chunked", WithIndexing),
      ],
      False,
    )
}

// Valid headers should succeed.
pub fn open_stream_with_valid_headers_test() {
  let conn = helper.new_connection(Client, Connected)
  let assert Ok(#(conn, _to_send)) =
    open_stream(conn, helper.request_headers(), False)
  let assert Ok(stream) = dict.get(conn.streams, 1)
  assert stream.state == Open
}

// RFC 9113 Section 8.3.2 - Server responses must include :status.
pub fn send_headers_response_missing_status_is_error_test() {
  let server = helper.new_connection(Server, Connected)
  let client = helper.new_connection(Client, Connected)
  let assert Ok(#(_client, headers)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) =
    h2_core.receive_data(server, headers)
  // Server sends response without :status
  let assert Error(_) =
    send_headers(
      server,
      1,
      [Header("content-type", "text/html", WithIndexing)],
      False,
    )
}

// Valid server response should succeed.
pub fn send_headers_response_with_status_test() {
  let server = helper.new_connection(Server, Connected)
  let client = helper.new_connection(Client, Connected)
  let assert Ok(#(_client, headers)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) =
    h2_core.receive_data(server, headers)
  let assert Ok(#(_server, _to_send)) =
    send_headers(server, 1, [Header(":status", "200", WithIndexing)], False)
}
