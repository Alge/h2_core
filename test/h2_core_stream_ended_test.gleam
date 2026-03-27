import gleam/option.{None}
import h2_core.{
  DataReceived, Header, HeadersReceived, NoError, StreamEnded, StreamReset,
  WithIndexing, open_stream, receive_data, send_data, send_headers,
  send_rst_stream,
}
import helper

// RFC 9113 Section 5.1:
// "For the purpose of state transitions, the END_STREAM flag is processed as
// a separate event to the frame that bears it; a HEADERS frame with the
// END_STREAM flag set can cause two state transitions."

// --- HEADERS + END_STREAM on a new stream ---

// Receiving HEADERS+END_STREAM on a new stream emits HeadersReceived followed
// by StreamEnded. Covers the "no-body request" case (e.g. GET with no body).
pub fn headers_end_stream_on_new_stream_emits_stream_ended_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(_client, encoded, _stream_id)) =
    open_stream(client, helper.request_headers(), True)

  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded)
  let assert [
    HeadersReceived(stream_id: 1, headers: _, end_stream: True),
    StreamEnded(stream_id: 1),
  ] = events
}

// Receiving HEADERS+END_STREAM for a response (server sends response with no
// body, e.g. reply to a HEAD request) emits HeadersReceived then StreamEnded.
pub fn response_headers_end_stream_emits_stream_ended_test() {
  let #(server, client) = helper.server_with_open_stream()
  let assert Ok(#(_server, response_bytes)) =
    send_headers(server, 1, helper.response_headers(), True)

  let assert Ok(#(_client, events, _to_send)) =
    receive_data(client, response_bytes)
  let assert [
    HeadersReceived(stream_id: 1, headers: _, end_stream: True),
    StreamEnded(stream_id: 1),
  ] = events
}

// --- DATA + END_STREAM ---

// Receiving DATA+END_STREAM emits DataReceived followed by StreamEnded.
pub fn data_end_stream_emits_stream_ended_test() {
  let #(server, client) = helper.server_with_open_stream()
  // Server sends response headers without END_STREAM so the stream stays open
  let assert Ok(#(server, header_bytes)) =
    send_headers(server, 1, helper.response_headers(), False)
  let assert Ok(#(client, _events, _to_send)) =
    receive_data(client, header_bytes)

  let assert Ok(#(_server, data_bytes)) =
    send_data(server, 1, <<"hello":utf8>>, True, None)

  let assert Ok(#(_client, events, _to_send)) = receive_data(client, data_bytes)
  let assert [
    DataReceived(stream_id: 1, data: <<"hello":utf8>>, end_stream: True, ..),
    StreamEnded(stream_id: 1),
  ] = events
}

// Receiving DATA+END_STREAM from a client (e.g. POST body) emits DataReceived
// followed by StreamEnded on the server side.
pub fn client_data_end_stream_emits_stream_ended_test() {
  let #(server, client) = helper.server_with_open_stream()

  let assert Ok(#(_client, data_bytes)) =
    send_data(client, 1, <<"body":utf8>>, True, None)

  let assert Ok(#(_server, events, _to_send)) = receive_data(server, data_bytes)
  let assert [
    DataReceived(stream_id: 1, data: <<"body":utf8>>, end_stream: True, ..),
    StreamEnded(stream_id: 1),
  ] = events
}

// DATA without END_STREAM must NOT emit StreamEnded.
pub fn data_without_end_stream_does_not_emit_stream_ended_test() {
  let #(server, client) = helper.server_with_open_stream()
  let assert Ok(#(server, header_bytes)) =
    send_headers(server, 1, helper.response_headers(), False)
  let assert Ok(#(client, _events, _to_send)) =
    receive_data(client, header_bytes)

  let assert Ok(#(_server, data_bytes)) =
    send_data(server, 1, <<"hello":utf8>>, False, None)

  let assert Ok(#(_client, events, _to_send)) = receive_data(client, data_bytes)
  let assert [DataReceived(stream_id: 1, end_stream: False, ..)] = events
}

// --- Trailers (HEADERS + END_STREAM on an existing stream) ---

// Receiving trailers (HEADERS+END_STREAM on an already-open stream) emits
// HeadersReceived then StreamEnded. Trailers always carry END_STREAM per
// RFC 9113 Section 8.1.
pub fn trailers_end_stream_emits_stream_ended_test() {
  let #(server, client) = helper.server_with_open_stream()
  // Client sends trailers on stream 1
  let assert Ok(#(_client, trailer_bytes)) =
    send_headers(
      client,
      1,
      [Header("x-trailer", <<"done":utf8>>, WithIndexing)],
      True,
    )

  let assert Ok(#(_server, events, _to_send)) =
    receive_data(server, trailer_bytes)
  let assert [
    HeadersReceived(stream_id: 1, headers: _, end_stream: True),
    StreamEnded(stream_id: 1),
  ] = events
}

// --- HEADERS + END_STREAM on a push promise (reserved remote) stream ---

// Receiving HEADERS+END_STREAM on a reserved (remote) stream (server push
// with no body) emits HeadersReceived then StreamEnded.
// RFC 9113 Section 5.1: reserved (remote) --recv H--> half-closed (local)
// --recv ES--> closed, with END_STREAM as a separate event.
pub fn push_promise_headers_end_stream_emits_stream_ended_test() {
  let #(server, client, promised_id) =
    helper.client_with_reserved_remote_stream()

  // Server sends HEADERS+END_STREAM on the promised stream
  let assert Ok(#(_server, push_response_bytes)) =
    send_headers(
      server,
      promised_id,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      True,
    )

  let assert Ok(#(_client, events, _to_send)) =
    receive_data(client, push_response_bytes)
  let assert [
    HeadersReceived(stream_id: _, headers: _, end_stream: True),
    StreamEnded(stream_id: _),
  ] = events
}

// --- HEADERS without END_STREAM must NOT emit StreamEnded ---

// Opening a stream without END_STREAM must not produce StreamEnded.
pub fn headers_without_end_stream_does_not_emit_stream_ended_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(_client, encoded, _stream_id)) =
    open_stream(client, helper.request_headers(), False)

  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded)
  let assert [HeadersReceived(stream_id: 1, end_stream: False, ..)] = events
}

// --- RST_STREAM must NOT emit StreamEnded ---

// RST_STREAM is an abrupt abort, not a graceful half-close. It must emit
// StreamReset only, never StreamEnded.
// RFC 9113 Section 6.4 distinguishes RST_STREAM from END_STREAM explicitly.
pub fn rst_stream_does_not_emit_stream_ended_test() {
  let #(server, client) = helper.server_with_open_stream()

  let assert Ok(#(_server, rst_bytes)) = send_rst_stream(server, 1, NoError)
  let assert Ok(#(_client, events, _to_send)) = receive_data(client, rst_bytes)

  let assert [StreamReset(stream_id: 1, error_code: NoError)] = events
}

// --- Ordering: StreamEnded comes after the frame event ---

// RFC 9113 Section 5.1: END_STREAM is processed as a separate event AFTER
// the frame that bears it. The events list must preserve this order.
pub fn stream_ended_comes_after_data_received_test() {
  let #(server, client) = helper.server_with_open_stream()
  let assert Ok(#(server, header_bytes)) =
    send_headers(server, 1, helper.response_headers(), False)
  let assert Ok(#(client, _events, _to_send)) =
    receive_data(client, header_bytes)

  let assert Ok(#(_server, data_bytes)) =
    send_data(server, 1, <<"body":utf8>>, True, None)

  let assert Ok(#(_client, events, _to_send)) = receive_data(client, data_bytes)

  // DataReceived must precede StreamEnded
  let assert [DataReceived(stream_id: 1, ..), StreamEnded(stream_id: 1)] =
    events
}

pub fn stream_ended_comes_after_headers_received_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(_client, encoded, _stream_id)) =
    open_stream(client, helper.request_headers(), True)

  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded)

  // HeadersReceived must precede StreamEnded
  let assert [HeadersReceived(stream_id: 1, ..), StreamEnded(stream_id: 1)] =
    events
}
