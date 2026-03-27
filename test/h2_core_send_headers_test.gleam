import gleam/bit_array
import h2_core.{
  type Connection, ConnectionError, Header, ProtocolError, Server, StreamClosed,
  StreamError, WithIndexing, open_stream, receive_data, send_headers,
}
import h2_core/internal/stream.{Closed, HalfClosedLocal, HalfClosedRemote, Open}
import h2_frame
import helper

// =============================================================================
// Helpers
// =============================================================================

fn server_with_open_stream() -> #(Connection, Connection) {
  helper.server_with_open_stream()
}

fn server_with_half_closed_remote_stream() -> #(Connection, Connection) {
  helper.server_with_half_closed_remote_stream()
}

// =============================================================================
// Valid states - RFC 9113 Section 5.1
//
// "HEADERS frames can be sent on a stream in the 'idle', 'reserved (local)',
// 'open', or 'half-closed (remote)' state." (Section 6.2)
// =============================================================================

// RFC 9113 Section 5.1 - A stream in the "open" state may be used by both
// peers to send frames of any type.
pub fn send_headers_on_open_stream_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(_server, to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(#(frame_data, _)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(h2_frame.Headers(stream_id: 1, end_stream: False, ..)) =
    h2_frame.decode_frame(frame_data)
}

// RFC 9113 Section 5.1 - "An endpoint sending an END_STREAM flag causes the
// stream state to become 'half-closed (local)'."
pub fn send_headers_with_end_stream_on_open_stream_transitions_to_half_closed_local_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      True,
    )
  let assert Ok(HalfClosedLocal) = h2_core.get_stream_state(server, 1)
}

// RFC 9113 Section 5.1 - A stream that is "half-closed (remote)" can be used
// by the endpoint to send frames of any type.
pub fn send_headers_on_half_closed_remote_stream_test() {
  let #(server, _client) = server_with_half_closed_remote_stream()
  let assert Ok(#(_server, to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(#(frame_data, _)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(h2_frame.Headers(stream_id: 1, end_stream: False, ..)) =
    h2_frame.decode_frame(frame_data)
}

// RFC 9113 Section 5.1 - "A stream can transition from this ['half-closed
// (remote)'] state to 'closed' by sending a frame with the END_STREAM flag
// set."
pub fn send_headers_with_end_stream_on_half_closed_remote_closes_stream_test() {
  let #(server, _client) = server_with_half_closed_remote_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      True,
    )
  let assert Ok(Closed) = h2_core.get_stream_state(server, 1)
}

// RFC 9113 Section 5.1 - "reserved (local): The endpoint can send a HEADERS
// frame. This causes the stream to open in a 'half-closed (remote)' state."
pub fn send_headers_on_reserved_local_transitions_to_half_closed_remote_test() {
  let #(server, _client, promised_id) =
    helper.server_with_reserved_local_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      promised_id,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(HalfClosedRemote) =
    h2_core.get_stream_state(server, promised_id)
}

// RFC 9113 Section 5.1: "the END_STREAM flag is processed as a separate event
// to the frame that bears it; a HEADERS frame with the END_STREAM flag set can
// cause two state transitions."
// reserved (local) --send H--> half-closed (remote) --send ES--> closed
pub fn send_headers_end_stream_on_reserved_local_transitions_to_closed_test() {
  let #(server, _client, promised_id) =
    helper.server_with_reserved_local_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      promised_id,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      True,
    )
  let assert Ok(Closed) =
    h2_core.get_stream_state(server, promised_id)
}

// RFC 9113 Section 8.1 - A server MAY send interim (1xx) responses before the
// final response. Interim responses are HEADERS without END_STREAM.
pub fn send_headers_interim_response_does_not_close_stream_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"100":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(Open) = h2_core.get_stream_state(server, 1)
}

// RFC 9113 Section 5.1 - Without END_STREAM the stream state stays Open.
pub fn send_headers_without_end_stream_does_not_change_state_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(server, _to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(Open) = h2_core.get_stream_state(server, 1)
}

// =============================================================================
// Trailers - RFC 9113 Section 8.1
// =============================================================================

// RFC 9113 Section 8.1: "Trailers MUST NOT include pseudo-header fields
// (Section 8.3). An endpoint that receives pseudo-header fields in trailers
// MUST treat the request or response as malformed (Section 8.1.1)."
pub fn send_trailers_with_pseudo_header_is_malformed_test() {
  let #(server, _client) = server_with_open_stream()
  // Send initial response — marks headers_sent: True on the stream
  let assert Ok(#(server, _)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  // Sending trailers containing a pseudo-header field must be rejected
  let assert Error(StreamError(1, ProtocolError)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      True,
    )
}

// =============================================================================
// Invalid states - RFC 9113 Section 5.1
// =============================================================================

// RFC 9113 Section 5.1 - "A stream that is in the 'half-closed (local)' state
// cannot be used for sending frames other than WINDOW_UPDATE, PRIORITY, and
// RST_STREAM."
pub fn send_headers_on_half_closed_local_is_stream_closed_error_test() {
  let #(server, _client) = helper.server_with_half_closed_local_stream()
  let assert Error(StreamError(1, StreamClosed)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
}

// RFC 9113 Section 5.1 - "An endpoint MUST NOT send frames other than PRIORITY
// on a closed stream."
pub fn send_headers_on_closed_stream_is_error_test() {
  let #(server, _client) = helper.server_with_closed_stream()
  let assert Error(StreamError(1, StreamClosed)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
}

// RFC 9113 Section 5.1 - "An endpoint MUST NOT send any type of frame other
// than RST_STREAM, WINDOW_UPDATE, or PRIORITY in this ['reserved (remote)']
// state."
pub fn send_headers_on_reserved_remote_is_protocol_error_test() {
  let #(_server, client, promised_id) =
    helper.client_with_reserved_remote_stream()
  let assert Error(ConnectionError(ProtocolError)) =
    send_headers(
      client,
      promised_id,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
}

// RFC 9113 Section 5.1 - An idle stream has no existing connection state;
// use open_stream to initiate new streams instead.
pub fn send_headers_on_idle_stream_is_error_test() {
  let server = helper.connected_connection(Server)
  let assert Error(StreamError(99, StreamClosed)) =
    send_headers(
      server,
      99,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
}

// RFC 9113 Section 6.2 - "HEADERS frames MUST be associated with a stream.
// If a HEADERS frame is received whose Stream Identifier field is 0x00, the
// recipient MUST respond with a connection error of type PROTOCOL_ERROR."
// The same applies when sending.
pub fn send_headers_on_stream_zero_is_protocol_error_test() {
  let server = helper.connected_connection(Server)
  let assert Error(ConnectionError(ProtocolError)) =
    send_headers(
      server,
      0,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
}

// =============================================================================
// Output correctness
// =============================================================================

// RFC 9113 Section 6.2 - The encoded frame must carry the correct stream ID.
pub fn send_headers_encodes_correct_stream_id_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(_server, to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(#(frame_data, _rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(frame) = h2_frame.decode_frame(frame_data)
  let assert h2_frame.Headers(stream_id: 1, ..) = frame
}

// RFC 9113 Section 6.2 - END_STREAM flag must be set in the encoded frame
// when requested.
pub fn send_headers_end_stream_flag_set_in_frame_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(_server, to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      True,
    )
  let assert Ok(#(frame_data, _rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(frame) = h2_frame.decode_frame(frame_data)
  let assert h2_frame.Headers(end_stream: True, ..) = frame
}

// RFC 9113 Section 6.2 - END_STREAM flag must NOT be set when end_stream is
// False.
pub fn send_headers_end_stream_flag_not_set_when_false_test() {
  let #(server, _client) = server_with_open_stream()
  let assert Ok(#(_server, to_send)) =
    send_headers(
      server,
      1,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(#(frame_data, _rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(frame) = h2_frame.decode_frame(frame_data)
  let assert h2_frame.Headers(end_stream: False, ..) = frame
}

// RFC 9113 Section 4.3 - Header compression state is maintained across frames
// on the same connection; the HPACK encoder must be updated after sending.
pub fn send_headers_updates_hpack_encoder_test() {
  let #(server, client) = server_with_open_stream()
  // Open a second stream on the same client — will use stream 3
  let assert Ok(#(_client, request2, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, request2)
  let headers = [
    Header(":status", <<"200":utf8>>, WithIndexing),
    Header("content-type", <<"text/plain":utf8>>, WithIndexing),
  ]
  let assert Ok(#(server, first)) = send_headers(server, 1, headers, False)
  // Same headers on stream 3 — HPACK should produce smaller output
  let assert Ok(#(_server, second)) = send_headers(server, 3, headers, False)
  // Second encoding should be smaller due to HPACK dynamic table
  let assert Ok(#(first_frame, _)) = h2_frame.extract_frame(first, 16_384)
  let assert Ok(#(second_frame, _)) = h2_frame.extract_frame(second, 16_384)
  assert bit_array.byte_size(second_frame) < bit_array.byte_size(first_frame)
}
