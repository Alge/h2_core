import gleam/bit_array
import gleam/dict
import h2_core.{
  Client, ConnectionError, HalfClosedRemote, Header, HeadersReceived,
  NeverIndexed, Open, Server, WithIndexing, WithoutIndexing, new_connection,
  receive_data, send_headers,
}
import h2_frame

// RFC 9113 Section 6.2 - Receiving HEADERS opens a stream

// Receiving a valid HEADERS frame emits HeadersReceived event
pub fn receive_headers_emits_event_test() {
  // Use a client to produce a valid HEADERS frame
  let client = new_connection(Client)
  let headers = [
    Header(":method", "GET", WithIndexing),
    Header(":path", "/", WithIndexing),
  ]
  let assert Ok(#(_client, _events, encoded)) =
    send_headers(client, headers, False)

  // Feed it to a server connection
  let server = new_connection(Server)
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
  let client = new_connection(Client)
  let headers = [Header(":method", "GET", WithIndexing)]
  let assert Ok(#(_client, _events, encoded)) =
    send_headers(client, headers, False)

  let server = new_connection(Server)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Open
}

// Receiving HEADERS with END_STREAM creates stream in HalfClosedRemote
pub fn receive_headers_end_stream_test() {
  let client = new_connection(Client)
  let headers = [Header(":method", "GET", WithIndexing)]
  let assert Ok(#(_client, _events, encoded)) =
    send_headers(client, headers, True)

  let server = new_connection(Server)
  let assert Ok(#(server, events, _to_send)) = receive_data(server, encoded)
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == HalfClosedRemote
  let assert [HeadersReceived(stream_id: 1, headers: _, end_stream: True)] =
    events
}

// Receiving HEADERS updates the HPACK decoder state
pub fn receive_headers_updates_hpack_decoder_test() {
  let client = new_connection(Client)
  let headers = [
    Header(":method", "GET", WithIndexing),
    Header("custom-header", "custom-value", WithIndexing),
  ]
  // Send headers twice from the same client (HPACK state accumulates)
  let assert Ok(#(client, _events, first_encoded)) =
    send_headers(client, headers, False)
  let assert Ok(#(_client, _events, second_encoded)) =
    send_headers(client, headers, False)

  // Feed both to the server sequentially - both should decode successfully
  let server = new_connection(Server)
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
  let client = new_connection(Client)
  let headers = [
    Header("custom-header", "custom-value", WithIndexing),
  ]
  let assert Ok(#(client, _events, first_encoded)) =
    send_headers(client, headers, False)
  let assert Ok(#(_client, _events, second_encoded)) =
    send_headers(client, headers, False)

  // Second should be smaller due to dynamic table indexing
  assert bit_array.byte_size(second_encoded)
    < bit_array.byte_size(first_encoded)

  // Both should decode correctly on the server
  let server = new_connection(Server)
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
  let server = new_connection(Server)
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
  let client = new_connection(Client)
  let headers = [Header(":method", "GET", WithIndexing)]
  let assert Ok(#(_client, _events, encoded)) =
    send_headers(client, headers, False)

  let server = new_connection(Server)
  let assert Ok(#(_server, _events, to_send)) = receive_data(server, encoded)
  assert to_send == <<>>
}

// Receiving HEADERS with empty field block fragment
pub fn receive_headers_empty_block_test() {
  let client = new_connection(Client)
  let assert Ok(#(_client, _events, encoded)) = send_headers(client, [], False)

  let server = new_connection(Server)
  let assert Ok(#(server, events, _to_send)) = receive_data(server, encoded)
  let assert [HeadersReceived(stream_id: 1, headers: [], end_stream: False)] =
    events
  let assert Ok(stream) = dict.get(server.streams, 1)
  assert stream.state == Open
}

// Receiving multiple HEADERS frames creates separate streams
pub fn receive_multiple_headers_creates_streams_test() {
  let client = new_connection(Client)
  let h1 = [Header(":method", "GET", WithIndexing)]
  let assert Ok(#(client, _events, encoded1)) = send_headers(client, h1, False)
  let h2 = [Header(":method", "POST", WithIndexing)]
  let assert Ok(#(_client, _events, encoded2)) = send_headers(client, h2, False)

  let server = new_connection(Server)
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
  let client = new_connection(Client)
  let h1 = [Header(":method", "GET", WithIndexing)]
  let assert Ok(#(client, _events, encoded1)) = send_headers(client, h1, False)
  let h2 = [Header(":method", "POST", WithIndexing)]
  let assert Ok(#(_client, _events, encoded2)) = send_headers(client, h2, False)

  let server = new_connection(Server)
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
  let client = new_connection(Client)
  let headers = [Header(":method", "GET", WithIndexing)]
  let assert Ok(#(client, _events, encoded1)) =
    send_headers(client, headers, False)
  let assert Ok(#(_client, _events, encoded2)) =
    send_headers(client, headers, False)

  let server = new_connection(Server)
  assert server.last_remote_stream_id == 0
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  assert server.last_remote_stream_id == 1
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded2)
  assert server.last_remote_stream_id == 3
}

// RFC 9113 Section 5.1.1 - Stream IDs must be monotonically increasing
// Receiving HEADERS with a stream ID <= last_remote_stream_id is PROTOCOL_ERROR
pub fn receive_headers_decreasing_stream_id_is_protocol_error_test() {
  let headers = [Header(":method", "GET", WithIndexing)]
  // Produce a HEADERS frame for stream 1
  let assert Ok(#(_client, _events, encoded1)) =
    send_headers(new_connection(Client), headers, False)

  let server = new_connection(Server)
  // Receive stream 1
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  assert server.last_remote_stream_id == 1
  // Produce another HEADERS frame for stream 1 (from a fresh client)
  let assert Ok(#(_client, _events, dup_encoded)) =
    send_headers(new_connection(Client), headers, False)
  // Feeding stream 1 again should be PROTOCOL_ERROR (not increasing)
  let assert Error(ConnectionError(h2_frame.ProtocolError)) =
    receive_data(server, dup_encoded)
}

// RFC 9113 Section 4.3 - Invalid HPACK data is COMPRESSION_ERROR
pub fn receive_headers_invalid_hpack_is_compression_error_test() {
  let server = new_connection(Server)
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
  let client = new_connection(Client)
  let headers = [
    Header(":method", "GET", WithIndexing),
    Header("authorization", "Bearer secret", WithoutIndexing),
  ]
  let assert Ok(#(_client, _events, encoded)) =
    send_headers(client, headers, False)

  let server = new_connection(Server)
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded)
  let assert [
    HeadersReceived(stream_id: 1, headers: recv_headers, end_stream: False),
  ] = events
  let assert [
    Header(":method", "GET", _),
    Header("authorization", "Bearer secret", _),
  ] = recv_headers
}

// Receiving HEADERS preserves NeverIndexed headers
pub fn receive_headers_never_indexed_test() {
  let client = new_connection(Client)
  let headers = [
    Header("secret-token", "abc123", NeverIndexed),
  ]
  let assert Ok(#(_client, _events, encoded)) =
    send_headers(client, headers, False)

  let server = new_connection(Server)
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded)
  let assert [
    HeadersReceived(stream_id: 1, headers: recv_headers, end_stream: False),
  ] = events
  let assert [Header("secret-token", "abc123", _)] = recv_headers
}
