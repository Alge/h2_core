import gleam/bit_array
import gleam/dict
import gleam/option
import h2_core.{
  Client, Closed, Connected, Connection, ConnectionError, HalfClosedLocal,
  HalfClosedRemote, Header, HeadersReceived, NeverIndexed, Open, Server,
  StreamReset, WithIndexing, WithoutIndexing, open_stream, receive_data,
  send_settings,
}
import h2_frame
import helper

// RFC 9113 Section 6.2 - Receiving HEADERS opens a stream

// Receiving a valid HEADERS frame emits HeadersReceived event
pub fn receive_headers_emits_event_test() {
  // Use a client to produce a valid HEADERS frame
  let client = helper.new_connection(Client, Connected)
  let headers = [
    Header(":method", "GET", WithIndexing),
    Header(":path", "/", WithIndexing),
  ]
  let assert Ok(#(_client, _events, encoded)) =
    open_stream(client, headers, False)

  // Feed it to a server connection
  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded)
  let assert [
    HeadersReceived(stream_id: 1, headers: recv_headers, end_stream: False),
  ] = events
  // Check we got the right headers back
  let assert [Header(":method", "GET", _), Header(":path", "/", _)] =
    recv_headers
}

// Receiving HEADERS creates the stream in Open state
pub fn receive_headers_opens_stream_test() {
  let client = helper.new_connection(Client, Connected)
  let headers = [Header(":method", "GET", WithIndexing)]
  let assert Ok(#(_client, _events, encoded)) =
    open_stream(client, headers, False)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Open
}

// Receiving HEADERS with END_STREAM creates stream in HalfClosedRemote
pub fn receive_headers_end_stream_test() {
  let client = helper.new_connection(Client, Connected)
  let headers = [Header(":method", "GET", WithIndexing)]
  let assert Ok(#(_client, _events, encoded)) =
    open_stream(client, headers, True)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, events, _to_send)) = receive_data(server, encoded)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == HalfClosedRemote
  let assert [HeadersReceived(stream_id: 1, headers: _, end_stream: True)] =
    events
}

// Receiving HEADERS updates the HPACK decoder state
pub fn receive_headers_updates_hpack_decoder_test() {
  let client = helper.new_connection(Client, Connected)
  let headers = [
    Header(":method", "GET", WithIndexing),
    Header("custom-header", "custom-value", WithIndexing),
  ]
  // Send headers twice from the same client (HPACK state accumulates)
  let assert Ok(#(client, _events, first_encoded)) =
    open_stream(client, headers, False)
  let assert Ok(#(_client, _events, second_encoded)) =
    open_stream(client, headers, False)

  // Feed both to the server sequentially - both should decode successfully
  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, events1, _to_send)) =
    receive_data(server, first_encoded)
  let assert [HeadersReceived(stream_id: 1, headers: h1, end_stream: False)] =
    events1
  let assert [
    Header(":method", "GET", _),
    Header("custom-header", "custom-value", _),
  ] = h1

  let assert Ok(#(_server, events2, _to_send)) =
    receive_data(server, second_encoded)
  let assert [HeadersReceived(stream_id: 3, headers: h2, end_stream: False)] =
    events2
  let assert [
    Header(":method", "GET", _),
    Header("custom-header", "custom-value", _),
  ] = h2
}

// The second encoded frame should be smaller due to HPACK dynamic table
pub fn receive_headers_hpack_compression_works_test() {
  let client = helper.new_connection(Client, Connected)
  let headers = [
    Header("custom-header", "custom-value", WithIndexing),
  ]
  let assert Ok(#(client, _events, first_encoded)) =
    open_stream(client, headers, False)
  let assert Ok(#(_client, _events, second_encoded)) =
    open_stream(client, headers, False)

  // Second should be smaller due to dynamic table indexing
  assert bit_array.byte_size(second_encoded)
    < bit_array.byte_size(first_encoded)

  // Both should decode correctly on the server
  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, first_encoded)
  let assert Ok(#(_server, events, _to_send)) =
    receive_data(server, second_encoded)
  let assert [HeadersReceived(stream_id: 3, headers: h, end_stream: False)] =
    events
  let assert [Header("custom-header", "custom-value", _)] = h
}

// RFC 9113 Section 6.2 - HEADERS on stream 0 is PROTOCOL_ERROR
pub fn receive_headers_on_stream_zero_is_protocol_error_test() {
  let server = helper.new_connection(Server, Connected)
  // Manually craft a HEADERS frame on stream 0
  // Length=0, Type=0x01, Flags=0x04 (END_HEADERS), Stream ID=0
  let bad_headers = <<
    0:size(24),
    0x01:size(8),
    0x04:size(8),
    0:size(1),
    0:size(31),
  >>
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, bad_headers)
}

// Receiving HEADERS does not send any response frame
pub fn receive_headers_no_response_test() {
  let client = helper.new_connection(Client, Connected)
  let headers = [Header(":method", "GET", WithIndexing)]
  let assert Ok(#(_client, _events, encoded)) =
    open_stream(client, headers, False)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(_server, _events, to_send)) = receive_data(server, encoded)
  assert to_send == <<>>
}

// Receiving HEADERS with empty field block fragment
pub fn receive_headers_empty_block_test() {
  let client = helper.new_connection(Client, Connected)
  let assert Ok(#(_client, _events, encoded)) = open_stream(client, [], False)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, events, _to_send)) = receive_data(server, encoded)
  let assert [HeadersReceived(stream_id: 1, headers: [], end_stream: False)] =
    events
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Open
}

// Receiving multiple HEADERS frames creates separate streams
pub fn receive_multiple_headers_creates_streams_test() {
  let client = helper.new_connection(Client, Connected)
  let h1 = [Header(":method", "GET", WithIndexing)]
  let assert Ok(#(client, _events, encoded1)) = open_stream(client, h1, False)
  let h2 = [Header(":method", "POST", WithIndexing)]
  let assert Ok(#(_client, _events, encoded2)) = open_stream(client, h2, False)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, events1, _to_send)) = receive_data(server, encoded1)
  let assert [HeadersReceived(stream_id: 1, headers: _, end_stream: False)] =
    events1
  let assert Ok(#(server, events2, _to_send)) = receive_data(server, encoded2)
  let assert [HeadersReceived(stream_id: 3, headers: _, end_stream: False)] =
    events2

  // Both streams should exist
  let assert Ok(s1) = dict.get(server.streams, 1)
  let assert Ok(s3) = dict.get(server.streams, 3)
  assert s1.state == Open
  assert s3.state == Open
}

// Receiving both HEADERS frames in a single receive_data call
pub fn receive_multiple_headers_in_one_call_test() {
  let client = helper.new_connection(Client, Connected)
  let h1 = [Header(":method", "GET", WithIndexing)]
  let assert Ok(#(client, _events, encoded1)) = open_stream(client, h1, False)
  let h2 = [Header(":method", "POST", WithIndexing)]
  let assert Ok(#(_client, _events, encoded2)) = open_stream(client, h2, False)

  let server = helper.new_connection(Server, Connected)
  // Feed both frames at once
  let combined = <<encoded1:bits, encoded2:bits>>
  let assert Ok(#(server, events, _to_send)) = receive_data(server, combined)
  // Should get two events (order preserved)
  let assert [
    HeadersReceived(stream_id: 1, headers: _, end_stream: False),
    HeadersReceived(stream_id: 3, headers: _, end_stream: False),
  ] = events
  let assert Ok(s1) = dict.get(server.streams, 1)
  let assert Ok(s3) = dict.get(server.streams, 3)
  assert s1.state == Open
  assert s3.state == Open
}

// Receiving HEADERS updates last_remote_stream_id
pub fn receive_headers_updates_last_remote_stream_id_test() {
  let client = helper.new_connection(Client, Connected)
  let headers = [Header(":method", "GET", WithIndexing)]
  let assert Ok(#(client, _events, encoded1)) =
    open_stream(client, headers, False)
  let assert Ok(#(_client, _events, encoded2)) =
    open_stream(client, headers, False)

  let server = helper.new_connection(Server, Connected)
  assert server.last_remote_stream_id == 0
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  assert server.last_remote_stream_id == 1
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded2)
  assert server.last_remote_stream_id == 3
}

// RFC 9113 Section 5.1.1 - Stream IDs must be monotonically increasing
// Receiving HEADERS with a stream ID that doesn't exist in conn.streams
// and is <= last_remote_stream_id is a connection error PROTOCOL_ERROR.
pub fn receive_headers_decreasing_stream_id_is_protocol_error_test() {
  let client = helper.new_connection(Client, Connected)
  let headers = [Header(":method", "GET", WithIndexing)]
  // Open streams 1 and 3
  let assert Ok(#(client, _events, encoded1)) =
    open_stream(client, headers, False)
  let assert Ok(#(_client, _events, encoded3)) =
    open_stream(client, headers, False)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded3)
  assert server.last_remote_stream_id == 3

  // Craft a HEADERS frame for stream 2 (even = server-initiated, never opened)
  // stream 2 doesn't exist in conn.streams and 2 < 3 = last_remote_stream_id
  let assert Ok(#(_fresh, _events, encoded_new)) =
    open_stream(helper.new_connection(Client, Connected), headers, False)
  let patched = helper.patch_stream_id(encoded_new, 2)
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, patched)
}

// RFC 9113 Section 5.1 - Receiving HEADERS on an open stream is valid
// (e.g. trailer headers after message body).
pub fn receive_headers_on_open_stream_is_valid_test() {
  let client = helper.new_connection(Client, Connected)
  let headers = [Header(":method", "GET", WithIndexing)]
  let assert Ok(#(client, _events, encoded1)) =
    open_stream(client, headers, False)
  // Produce a second HEADERS for stream 3, then patch to stream 1
  let assert Ok(#(_client, _events, encoded2)) =
    open_stream(client, [Header("x-trailer", "value", WithIndexing)], False)
  let patched = helper.patch_stream_id(encoded2, 1)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Open

  // HEADERS on an open stream should succeed and keep state Open
  let assert Ok(#(server, events, to_send)) = receive_data(server, patched)
  let assert [HeadersReceived(stream_id: 1, headers: _, end_stream: False)] =
    events
  assert to_send == <<>>
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Open
}

// RFC 9113 Section 5.1 - Receiving HEADERS on a half-closed(local) stream
// is valid. Half-closed(local) means we sent END_STREAM but the remote
// peer can still send frames, including HEADERS (trailers).
pub fn receive_headers_on_half_closed_local_stream_is_valid_test() {
  let client = helper.new_connection(Client, Connected)
  let assert Ok(#(client, _events, encoded1)) =
    open_stream(client, [Header(":method", "GET", WithIndexing)], False)
  let assert Ok(#(_client, _events, encoded2)) =
    open_stream(client, [Header("x-trailer", "value", WithIndexing)], False)
  let patched = helper.patch_stream_id(encoded2, 1)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  // Simulate server having sent END_STREAM on stream 1 (half-closed local)
  let server = helper.set_stream_state(server, 1, HalfClosedLocal)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == HalfClosedLocal

  // HEADERS on half-closed(local) should succeed
  let assert Ok(#(_server, events, to_send)) = receive_data(server, patched)
  let assert [HeadersReceived(stream_id: 1, headers: _, end_stream: False)] =
    events
  assert to_send == <<>>
}

// RFC 9113 Section 5.1 - Receiving HEADERS with END_STREAM on an open
// stream transitions it to half-closed(remote).
pub fn receive_headers_end_stream_on_open_stream_transitions_state_test() {
  let client = helper.new_connection(Client, Connected)
  let assert Ok(#(client, _events, encoded1)) =
    open_stream(client, [Header(":method", "GET", WithIndexing)], False)
  let assert Ok(#(_client, _events, encoded2)) =
    open_stream(client, [Header("x-trailer", "done", WithIndexing)], True)
  let patched = helper.patch_stream_id(encoded2, 1)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Open

  let assert Ok(#(server, events, _to_send)) = receive_data(server, patched)
  let assert [HeadersReceived(stream_id: 1, headers: _, end_stream: True)] =
    events
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == HalfClosedRemote
}

// RFC 9113 Section 5.1 - Receiving HEADERS with END_STREAM on a
// half-closed(local) stream transitions it to Closed.
pub fn receive_headers_end_stream_on_half_closed_local_transitions_to_closed_test() {
  let client = helper.new_connection(Client, Connected)
  let assert Ok(#(client, _events, encoded1)) =
    open_stream(client, [Header(":method", "GET", WithIndexing)], False)
  let assert Ok(#(_client, _events, encoded2)) =
    open_stream(client, [Header("x-trailer", "done", WithIndexing)], True)
  let patched = helper.patch_stream_id(encoded2, 1)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  // Simulate server having sent END_STREAM on stream 1 (half-closed local)
  let server = helper.set_stream_state(server, 1, HalfClosedLocal)

  let assert Ok(#(server, events, _to_send)) = receive_data(server, patched)
  let assert [HeadersReceived(stream_id: 1, headers: _, end_stream: True)] =
    events
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Closed
}

// RFC 9113 Section 4.3 - Invalid HPACK data is COMPRESSION_ERROR
pub fn receive_headers_invalid_hpack_is_compression_error_test() {
  let server = helper.new_connection(Server, Connected)
  // Craft a HEADERS frame with invalid HPACK data
  // Length=3, Type=0x01, Flags=0x04 (END_HEADERS), Stream ID=1
  // Payload: 0xFF, 0xFF, 0xFF (not valid HPACK)
  let bad_headers = <<
    3:size(24),
    0x01:size(8),
    0x04:size(8),
    0:size(1),
    1:size(31),
    0xFF,
    0xFF,
    0xFF,
  >>
  let assert Error(ConnectionError(h2_frame.CompressionError)) =
    receive_data(server, bad_headers)
}

// Receiving HEADERS preserves indexing information from WithoutIndexing headers
pub fn receive_headers_without_indexing_test() {
  let client = helper.new_connection(Client, Connected)
  let headers = [
    Header(":method", "GET", WithIndexing),
    Header("authorization", "Bearer secret", WithoutIndexing),
  ]
  let assert Ok(#(_client, _events, encoded)) =
    open_stream(client, headers, False)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded)
  let assert [
    HeadersReceived(stream_id: 1, headers: recv_headers, end_stream: False),
  ] = events
  let assert [
    Header(":method", "GET", WithIndexing),
    Header("authorization", "Bearer secret", WithoutIndexing),
  ] = recv_headers
}

// Receiving HEADERS preserves NeverIndexed headers
pub fn receive_headers_never_indexed_test() {
  let client = helper.new_connection(Client, Connected)
  let headers = [
    Header("secret-token", "abc123", NeverIndexed),
  ]
  let assert Ok(#(_client, _events, encoded)) =
    open_stream(client, headers, False)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded)
  let assert [
    HeadersReceived(stream_id: 1, headers: recv_headers, end_stream: False),
  ] = events
  let assert [Header("secret-token", "abc123", NeverIndexed)] = recv_headers
}

// --- Stream state validation ---

// RFC 9113 Section 5.1 - An endpoint that sends RST_STREAM "might
// receive frames that were in transit" and "MUST minimally process
// and then discard any frames it receives in this state. This means
// updating header compression state for HEADERS [...] frames."
// HEADERS on a closed stream must be silently discarded (HPACK decoded
// but no event, no response).
pub fn receive_headers_on_closed_stream_is_discarded_test() {
  let client = helper.new_connection(Client, Connected)
  let headers = [Header(":method", "GET", WithIndexing)]
  // Open stream 1
  let assert Ok(#(client, _events, encoded1)) =
    open_stream(client, headers, False)
  // Encode a RST_STREAM to close stream 1
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.Cancel)
  // Produce a second HEADERS, patch to stream 1
  let assert Ok(#(_client, _events, encoded2)) =
    open_stream(client, headers, False)
  let patched = helper.patch_stream_id(encoded2, 1)

  let server = helper.new_connection(Server, Connected)
  // Receive HEADERS to open stream 1
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Open
  // Receive RST_STREAM to close stream 1
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, rst)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Closed

  // HEADERS on closed stream: silently discarded
  let assert Ok(#(_server, events, to_send)) = receive_data(server, patched)
  assert events == []
  assert to_send == <<>>
}

// --- Half-closed (remote) stream error handling ---
// RFC 9113 Section 5.1 - "If an endpoint receives additional frames,
// other than WINDOW_UPDATE, PRIORITY, or RST_STREAM, for a stream that
// is in this state, it MUST respond with a stream error of type
// STREAM_CLOSED."

// Receiving HEADERS on a half-closed(remote) stream is a stream error,
// not a connection error — the connection must survive.
pub fn receive_headers_on_half_closed_remote_is_stream_error_test() {
  let client = helper.new_connection(Client, Connected)
  let assert Ok(#(client, _events, encoded1)) =
    open_stream(client, [Header(":method", "GET", WithIndexing)], True)
  let assert Ok(#(_client, _events, encoded2)) =
    open_stream(client, [Header(":method", "POST", WithIndexing)], False)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == HalfClosedRemote

  // Patch frame 2 to target stream 1 (half-closed remote)
  let patched = helper.patch_stream_id(encoded2, 1)
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, patched)
  let assert [StreamReset(stream_id: 1, error_code: h2_frame.StreamClosed)] =
    events
}

// RFC 9113 Section 5.4.2 - Stream error sends RST_STREAM
pub fn receive_headers_on_half_closed_remote_sends_rst_stream_test() {
  let client = helper.new_connection(Client, Connected)
  let assert Ok(#(client, _events, encoded1)) =
    open_stream(client, [Header(":method", "GET", WithIndexing)], True)
  let assert Ok(#(_client, _events, encoded2)) =
    open_stream(client, [Header(":method", "POST", WithIndexing)], False)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)

  let patched = helper.patch_stream_id(encoded2, 1)
  let assert Ok(#(_server, _events, to_send)) = receive_data(server, patched)
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.StreamClosed)
  assert to_send == expected_rst
}

// The connection continues processing after a stream error —
// subsequent valid HEADERS on a new stream must succeed.
pub fn receive_headers_after_stream_error_succeeds_test() {
  let client = helper.new_connection(Client, Connected)
  let assert Ok(#(client, _events, encoded1)) =
    open_stream(client, [Header(":method", "GET", WithIndexing)], True)
  let assert Ok(#(client, _events, encoded2)) =
    open_stream(client, [Header(":method", "POST", WithIndexing)], False)
  let assert Ok(#(_client, _events, encoded3)) =
    open_stream(client, [Header(":method", "PUT", WithIndexing)], False)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)

  // Stream error on stream 1
  let patched = helper.patch_stream_id(encoded2, 1)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, patched)

  // Stream 5 should still work
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded3)
  let assert [HeadersReceived(stream_id: 5, headers: _, end_stream: False)] =
    events
}

// RFC 9113 Section 4.3 - HPACK state must be preserved even when
// HEADERS are rejected. "An endpoint receiving HEADERS, PUSH_PROMISE,
// or CONTINUATION frames needs to reassemble field blocks and perform
// decompression even if the frames are to be discarded."
//
// If the rejected frame's HPACK data is not decoded, the dynamic table
// goes out of sync and all subsequent header decoding fails with
// CompressionError.
pub fn rejected_headers_must_still_update_hpack_state_test() {
  let client = helper.new_connection(Client, Connected)
  let assert Ok(#(client, _events, encoded1)) =
    open_stream(client, [Header("x-custom", "value1", WithIndexing)], True)
  let assert Ok(#(client, _events, encoded2)) =
    open_stream(client, [Header("x-custom", "value2", WithIndexing)], False)
  let assert Ok(#(_client, _events, encoded3)) =
    open_stream(client, [Header("x-custom", "value3", WithIndexing)], False)

  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)

  // Rejected: HEADERS on half-closed(remote) stream 1
  let patched = helper.patch_stream_id(encoded2, 1)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, patched)

  // Frame 3 on stream 5 proves HPACK state survived the rejection.
  // Without HPACK decoding of the rejected frame, the dynamic table
  // is out of sync and this fails with CompressionError.
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded3)
  let assert [HeadersReceived(stream_id: 5, headers: h, end_stream: False)] =
    events
  let assert [Header("x-custom", "value3", _)] = h
}

// --- MAX_CONCURRENT_STREAMS enforcement ---

// RFC 9113 Section 5.1.2 - "Endpoints MUST NOT exceed the limit set
// by their peer. An endpoint that receives a HEADERS frame that causes
// its advertised concurrent stream limit to be exceeded MUST treat
// this as a stream error (Section 5.4.2) of type PROTOCOL_ERROR or
// REFUSED_STREAM."
//
// When the server has advertised MAX_CONCURRENT_STREAMS=1, a second
// concurrent stream must be refused.
pub fn receive_headers_exceeding_max_concurrent_streams_test() {
  let server = helper.new_connection(Server, Connected)
  let client = helper.new_connection(Client, Connected)

  // Server advertises MAX_CONCURRENT_STREAMS=1
  let assert Ok(#(server, _events, _to_send)) =
    send_settings(server, [h2_frame.MaxConcurrentStreams(1)])
  // Simulate the client having received and acked our settings
  // by directly setting local_settings (the ack path is tested elsewhere)
  let server =
    Connection(
      ..server,
      local_settings: h2_core.Settings(
        ..server.local_settings,
        max_concurrent_streams: option.Some(1),
      ),
      pending_settings: [],
    )

  // Open stream 1
  let assert Ok(#(client, _events, encoded1)) =
    open_stream(client, [Header(":method", "GET", WithIndexing)], False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Open

  // Open stream 3 — should be refused (exceeds MAX_CONCURRENT_STREAMS=1)
  let assert Ok(#(_client, _events, encoded3)) =
    open_stream(client, [Header(":method", "GET", WithIndexing)], False)
  let assert Ok(#(_server, events, to_send)) = receive_data(server, encoded3)

  // Should get a RST_STREAM with REFUSED_STREAM
  let assert [StreamReset(stream_id: 3, error_code: h2_frame.RefusedStream)] =
    events
  let assert Ok(#(frame_data, _rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(h2_frame.RstStream(3, h2_frame.RefusedStream)) =
    h2_frame.decode_frame(frame_data)
}

// --- Stream ID parity validation ---

// RFC 9113 Section 5.1.1 - "Streams initiated by a client MUST use
// odd-numbered stream identifiers; those initiated by the server MUST
// use even-numbered stream identifiers." and "An endpoint that
// receives an unexpected stream identifier MUST respond with a
// connection error (Section 5.4.1) of type PROTOCOL_ERROR."
//
// A server receiving HEADERS on an even stream ID (which is reserved
// for server-initiated streams) must reject it.
pub fn receive_headers_on_even_stream_id_is_protocol_error_test() {
  let client = helper.new_connection(Client, Connected)
  let assert Ok(#(_client, _events, encoded)) =
    open_stream(client, [Header(":method", "GET", WithIndexing)], False)
  // Patch stream ID from 1 (odd/client) to 2 (even/server)
  let patched = helper.patch_stream_id(encoded, 2)

  let server = helper.new_connection(Server, Connected)
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, patched)
}

// A client receiving HEADERS on an odd stream ID (which is reserved
// for client-initiated streams) must reject it.
pub fn client_receive_headers_on_odd_stream_id_is_protocol_error_test() {
  let server = helper.new_connection(Server, Connected)
  let assert Ok(#(_server, _events, encoded)) =
    open_stream(server, [Header(":status", "200", WithIndexing)], False)
  // Patch stream ID from 2 (even/server) to 1 (odd/client)
  let patched = helper.patch_stream_id(encoded, 1)

  let client = helper.new_connection(Client, Connected)
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(client, patched)
}
