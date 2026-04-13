import gleam/bit_array
import gleam/list
import gleam/option
import h2_core.{
  Client, CompressionError, ConnectionError, Header, HeadersReceived,
  MaxConcurrentStreams, NeverIndexed, ProtocolError, RefusedStream, Server,
  StreamClosed, StreamEnded, StreamRefused, StreamReset, WithIndexing,
  WithoutIndexing, get_stream_state, open_stream, receive_data, send_data,
  send_headers, send_settings,
}
import h2_core/internal/stream.{
  Closed, HalfClosedLocal, HalfClosedRemote, Open, ReservedRemote,
}
import h2_frame
import helper

// RFC 9113 Section 6.2 - Receiving HEADERS opens a stream

// Receiving a valid HEADERS frame emits HeadersReceived event
pub fn receive_headers_emits_event_test() {
  // Use a client to produce a valid HEADERS frame
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, encoded, _stream_id)) =
    open_stream(client, helper.request_headers(), False)

  // Feed it to a server connection
  let server = helper.connected_connection(Server)
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded)
  let assert [
    HeadersReceived(stream_id: 1, headers: recv_headers, end_stream: False),
  ] = events
  let assert [
    Header(":method", <<"GET":utf8>>, _),
    Header(":scheme", <<"https":utf8>>, _),
    Header(":path", <<"/":utf8>>, _),
  ] = recv_headers
}

// Receiving HEADERS creates the stream in Open state
pub fn receive_headers_opens_stream_test() {
  let client = helper.connected_connection(Client)
  let headers = helper.request_headers()
  let assert Ok(#(_client, encoded, _stream_id)) =
    open_stream(client, headers, False)

  let server = helper.connected_connection(Server)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded)
  let assert Ok(Open) = get_stream_state(server, 1)
}

// Receiving HEADERS with END_STREAM creates stream in HalfClosedRemote
pub fn receive_headers_end_stream_test() {
  let client = helper.connected_connection(Client)
  let headers = helper.request_headers()
  let assert Ok(#(_client, encoded, _stream_id)) =
    open_stream(client, headers, True)

  let server = helper.connected_connection(Server)
  let assert Ok(#(server, events, _to_send)) = receive_data(server, encoded)
  let assert Ok(HalfClosedRemote) = get_stream_state(server, 1)
  let assert [
    HeadersReceived(stream_id: 1, headers: _, end_stream: True),
    StreamEnded(stream_id: 1),
  ] = events
}

// Receiving HEADERS updates the HPACK decoder state
pub fn receive_headers_updates_hpack_decoder_test() {
  let client = helper.connected_connection(Client)
  let headers =
    list.append(helper.request_headers(), [
      Header("custom-header", <<"custom-value":utf8>>, WithIndexing),
    ])
  // Send headers twice from the same client (HPACK state accumulates)
  let assert Ok(#(client, first_encoded, _stream_id)) =
    open_stream(client, headers, False)
  let assert Ok(#(_client, second_encoded, _stream_id)) =
    open_stream(client, headers, False)

  // Feed both to the server sequentially - both should decode successfully
  let server = helper.connected_connection(Server)
  let assert Ok(#(server, events1, _to_send)) =
    receive_data(server, first_encoded)
  let assert [HeadersReceived(stream_id: 1, headers: h1, end_stream: False)] =
    events1
  let assert [
    Header(":method", <<"GET":utf8>>, _),
    Header(":scheme", <<"https":utf8>>, _),
    Header(":path", <<"/":utf8>>, _),
    Header("custom-header", <<"custom-value":utf8>>, _),
  ] = h1

  let assert Ok(#(_server, events2, _to_send)) =
    receive_data(server, second_encoded)
  let assert [HeadersReceived(stream_id: 3, headers: h2, end_stream: False)] =
    events2
  let assert [
    Header(":method", <<"GET":utf8>>, _),
    Header(":scheme", <<"https":utf8>>, _),
    Header(":path", <<"/":utf8>>, _),
    Header("custom-header", <<"custom-value":utf8>>, _),
  ] = h2
}

// The second encoded frame should be smaller due to HPACK dynamic table
pub fn receive_headers_hpack_compression_works_test() {
  let client = helper.connected_connection(Client)
  let headers =
    list.append(helper.request_headers(), [
      Header("custom-header", <<"custom-value":utf8>>, WithIndexing),
    ])
  let assert Ok(#(client, first_encoded, _stream_id)) =
    open_stream(client, headers, False)
  let assert Ok(#(_client, second_encoded, _stream_id)) =
    open_stream(client, headers, False)

  // Second should be smaller due to dynamic table indexing
  assert bit_array.byte_size(second_encoded)
    < bit_array.byte_size(first_encoded)

  // Both should decode correctly on the server
  let server = helper.connected_connection(Server)
  let assert Ok(#(server, _events, _to_send)) =
    receive_data(server, first_encoded)
  let assert Ok(#(_server, events, _to_send)) =
    receive_data(server, second_encoded)
  let assert [HeadersReceived(stream_id: 3, headers: h, end_stream: False)] =
    events
  let assert [
    Header(":method", <<"GET":utf8>>, _),
    Header(":scheme", <<"https":utf8>>, _),
    Header(":path", <<"/":utf8>>, _),
    Header("custom-header", <<"custom-value":utf8>>, _),
  ] = h
}

// RFC 9113 Section 6.2 - HEADERS on stream 0 is PROTOCOL_ERROR
pub fn receive_headers_on_stream_zero_is_protocol_error_test() {
  let server = helper.connected_connection(Server)
  // Manually craft a HEADERS frame on stream 0
  // Length=0, Type=0x01, Flags=0x04 (END_HEADERS), Stream ID=0
  let bad_headers = <<
    0:size(24),
    0x01:size(8),
    0x04:size(8),
    0:size(1),
    0:size(31),
  >>
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(server, bad_headers)
}

// Receiving HEADERS does not send any response frame
pub fn receive_headers_no_response_test() {
  let client = helper.connected_connection(Client)
  let headers = helper.request_headers()
  let assert Ok(#(_client, encoded, _stream_id)) =
    open_stream(client, headers, False)

  let server = helper.connected_connection(Server)
  let assert Ok(#(_server, _events, to_send)) = receive_data(server, encoded)
  assert to_send == <<>>
}

// Receiving HEADERS with empty field block fragment
// RFC 9113 Section 8.3.1 - A request with no pseudo-headers at all is
// malformed - it's missing :method, :scheme, :path.
pub fn receive_headers_empty_block_is_malformed_test() {
  // Manually craft HEADERS with empty field block to bypass outbound validation
  // Length=0, Type=0x01, Flags=0x04 (END_HEADERS), Stream ID=1
  let encoded = <<
    0:size(24),
    0x01:size(8),
    0x04:size(8),
    0:size(1),
    1:size(31),
  >>

  let server = helper.connected_connection(Server)
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal.
  let assert Ok(#(_server, events, to_send)) = receive_data(server, encoded)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// Receiving multiple HEADERS frames creates separate streams
pub fn receive_multiple_headers_creates_streams_test() {
  let client = helper.connected_connection(Client)
  let h1 = helper.request_headers()
  let assert Ok(#(client, encoded1, _stream_id)) =
    open_stream(client, h1, False)
  let h2 = [
    Header(":method", <<"POST":utf8>>, WithIndexing),
    Header(":scheme", <<"https":utf8>>, WithIndexing),
    Header(":path", <<"/":utf8>>, WithIndexing),
  ]
  let assert Ok(#(_client, encoded2, _stream_id)) =
    open_stream(client, h2, False)

  let server = helper.connected_connection(Server)
  let assert Ok(#(server, events1, _to_send)) = receive_data(server, encoded1)
  let assert [HeadersReceived(stream_id: 1, headers: _, end_stream: False)] =
    events1
  let assert Ok(#(server, events2, _to_send)) = receive_data(server, encoded2)
  let assert [HeadersReceived(stream_id: 3, headers: _, end_stream: False)] =
    events2

  // Both streams should exist
  let assert Ok(Open) = get_stream_state(server, 1)
  let assert Ok(Open) = get_stream_state(server, 3)
}

// Receiving both HEADERS frames in a single receive_data call
pub fn receive_multiple_headers_in_one_call_test() {
  let client = helper.connected_connection(Client)
  let h1 = helper.request_headers()
  let assert Ok(#(client, encoded1, _stream_id)) =
    open_stream(client, h1, False)
  let h2 = [
    Header(":method", <<"POST":utf8>>, WithIndexing),
    Header(":scheme", <<"https":utf8>>, WithIndexing),
    Header(":path", <<"/":utf8>>, WithIndexing),
  ]
  let assert Ok(#(_client, encoded2, _stream_id)) =
    open_stream(client, h2, False)

  let server = helper.connected_connection(Server)
  // Feed both frames at once
  let combined = <<encoded1:bits, encoded2:bits>>
  let assert Ok(#(server, events, _to_send)) = receive_data(server, combined)
  // Should get two events (order preserved)
  let assert [
    HeadersReceived(stream_id: 1, headers: _, end_stream: False),
    HeadersReceived(stream_id: 3, headers: _, end_stream: False),
  ] = events
  let assert Ok(Open) = get_stream_state(server, 1)
  let assert Ok(Open) = get_stream_state(server, 3)
}

// Receiving HEADERS updates last_remote_stream_id
pub fn receive_headers_updates_last_remote_stream_id_test() {
  let client = helper.connected_connection(Client)
  let headers = helper.request_headers()
  let assert Ok(#(client, encoded1, _stream_id)) =
    open_stream(client, headers, False)
  let assert Ok(#(_client, encoded2, _stream_id)) =
    open_stream(client, headers, False)

  let server = helper.connected_connection(Server)
  assert h2_core.get_last_remote_stream_id(server) == 0
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  assert h2_core.get_last_remote_stream_id(server) == 1
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded2)
  assert h2_core.get_last_remote_stream_id(server) == 3
}

// RFC 9113 Section 5.1.1 - Stream IDs must be monotonically increasing
// Receiving HEADERS with a stream ID that doesn't exist in conn.streams
// and is <= last_remote_stream_id is a connection error PROTOCOL_ERROR.
pub fn receive_headers_decreasing_stream_id_is_protocol_error_test() {
  let client = helper.connected_connection(Client)
  let headers = helper.request_headers()
  // Open streams 1 and 3
  let assert Ok(#(client, encoded1, _stream_id)) =
    open_stream(client, headers, False)
  let assert Ok(#(_client, encoded3, _stream_id)) =
    open_stream(client, headers, False)

  let server = helper.connected_connection(Server)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded3)
  assert h2_core.get_last_remote_stream_id(server) == 3

  // Craft a HEADERS frame for stream 2 (even = server-initiated, never opened)
  // stream 2 doesn't exist in conn.streams and 2 < 3 = last_remote_stream_id
  let assert Ok(#(_fresh, encoded_new, _stream_id)) =
    open_stream(helper.connected_connection(Client), headers, False)
  let patched = helper.patch_stream_id(encoded_new, 2)
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(server, patched)
}

// RFC 9113 Section 5.1 - Receiving HEADERS on an open stream is valid
// (e.g. trailer headers after message body).
pub fn receive_headers_on_open_stream_is_valid_test() {
  let client = helper.connected_connection(Client)
  let headers = helper.request_headers()
  let assert Ok(#(client, encoded1, _stream_id)) =
    open_stream(client, headers, False)
  // Produce a second HEADERS for stream 3, then patch to stream 1
  let assert Ok(#(_client, encoded2, _stream_id)) =
    open_stream(
      client,
      list.append(helper.request_headers(), [
        Header("x-trailer", <<"value":utf8>>, WithIndexing),
      ]),
      False,
    )
  let patched = helper.patch_stream_id(encoded2, 1)

  let server = helper.connected_connection(Server)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  let assert Ok(Open) = get_stream_state(server, 1)

  // HEADERS on an open stream should succeed and keep state Open
  let assert Ok(#(server, events, to_send)) = receive_data(server, patched)
  let assert [HeadersReceived(stream_id: 1, headers: _, end_stream: False)] =
    events
  assert to_send == <<>>
  let assert Ok(Open) = get_stream_state(server, 1)
}

// RFC 9113 Section 5.1 - Receiving HEADERS on a half-closed(local) stream
// is valid. Half-closed(local) means we sent END_STREAM but the remote
// peer can still send frames, including HEADERS (trailers).
pub fn receive_headers_on_half_closed_local_stream_is_valid_test() {
  let client = helper.connected_connection(Client)
  let assert Ok(#(client, encoded1, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(_client, encoded2, _stream_id)) =
    open_stream(
      client,
      list.append(helper.request_headers(), [
        Header("x-trailer", <<"value":utf8>>, WithIndexing),
      ]),
      False,
    )
  let patched = helper.patch_stream_id(encoded2, 1)

  let server = helper.connected_connection(Server)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  // Server sends response headers with END_STREAM on stream 1 (half-closed local)
  let assert Ok(#(server, _to_send)) =
    h2_core.send_headers(server, 1, helper.response_headers(), True)
  let assert Ok(HalfClosedLocal) = get_stream_state(server, 1)

  // HEADERS on half-closed(local) should succeed
  let assert Ok(#(_server, events, to_send)) = receive_data(server, patched)
  let assert [HeadersReceived(stream_id: 1, headers: _, end_stream: False)] =
    events
  assert to_send == <<>>
}

// RFC 9113 Section 5.1 - Receiving HEADERS with END_STREAM on an open
// stream transitions it to half-closed(remote).
pub fn receive_headers_end_stream_on_open_stream_transitions_state_test() {
  let client = helper.connected_connection(Client)
  let assert Ok(#(client, encoded1, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  // Send trailers on stream 1 (headers_sent is True, so validation treats as trailers)
  let assert Ok(#(_client, encoded2)) =
    send_headers(
      client,
      1,
      [Header("x-trailer", <<"done":utf8>>, WithIndexing)],
      True,
    )

  let server = helper.connected_connection(Server)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  let assert Ok(Open) = get_stream_state(server, 1)

  let assert Ok(#(server, events, _to_send)) = receive_data(server, encoded2)
  let assert [
    HeadersReceived(stream_id: 1, headers: _, end_stream: True),
    StreamEnded(stream_id: 1),
  ] = events
  let assert Ok(HalfClosedRemote) = get_stream_state(server, 1)
}

// RFC 9113 Section 5.1 - Receiving HEADERS with END_STREAM on a
// half-closed(local) stream transitions it to Closed.
pub fn receive_headers_end_stream_on_half_closed_local_transitions_to_closed_test() {
  let client = helper.connected_connection(Client)
  let assert Ok(#(client, encoded1, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  // Send trailers on stream 1
  let assert Ok(#(_client, encoded2)) =
    send_headers(
      client,
      1,
      [Header("x-trailer", <<"done":utf8>>, WithIndexing)],
      True,
    )

  let server = helper.connected_connection(Server)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  // Server sends response headers with END_STREAM on stream 1 (half-closed local)
  let assert Ok(#(server, _to_send)) =
    h2_core.send_headers(server, 1, helper.response_headers(), True)

  let assert Ok(#(server, events, _to_send)) = receive_data(server, encoded2)
  let assert [
    HeadersReceived(stream_id: 1, headers: _, end_stream: True),
    StreamEnded(stream_id: 1),
  ] = events
  let assert Ok(Closed) = get_stream_state(server, 1)
}

// RFC 9113 Section 6.2 - "A receiver is not obligated to verify padding
// but MAY treat non-zero padding as a connection error (Section 5.4.1) of
// type PROTOCOL_ERROR."
//
// By default, non-zero padding bytes in HEADERS frames MUST be silently
// accepted (the receiver is not obligated to verify them).
pub fn receive_headers_nonzero_padding_bytes_accepted_by_default_test() {
  let server = helper.connected_connection(Server)
  // Manually craft a padded HEADERS frame with non-zero padding bytes.
  // Length=7 (1 pad_length + 3 HPACK data + 3 padding), Type=0x01,
  // Flags=0x0C (PADDED | END_HEADERS), Stream ID=1
  // HPACK: 0x82 = :method GET, 0x87 = :scheme https, 0x84 = :path /
  let non_zero_padded = <<
    7:size(24), 0x01:size(8), 0x0C:size(8), 0:size(1), 1:size(31), 3:size(8),
    0x82, 0x87, 0x84, 0xFF, 0xFF, 0xFF,
  >>
  let assert Ok(#(_server, events, _to_send)) =
    receive_data(server, non_zero_padded)
  let assert [HeadersReceived(stream_id: 1, headers: _, end_stream: False)] =
    events
}

// RFC 9113 Section 4.3 - Invalid HPACK data is COMPRESSION_ERROR
pub fn receive_headers_invalid_hpack_is_compression_error_test() {
  let server = helper.connected_connection(Server)
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
  let assert Error(ConnectionError(CompressionError)) =
    receive_data(server, bad_headers)
}

// Receiving HEADERS preserves indexing information from WithoutIndexing headers
pub fn receive_headers_without_indexing_test() {
  let client = helper.connected_connection(Client)
  let headers =
    list.append(helper.request_headers(), [
      Header("authorization", <<"Bearer secret":utf8>>, WithoutIndexing),
    ])
  let assert Ok(#(_client, encoded, _stream_id)) =
    open_stream(client, headers, False)

  let server = helper.connected_connection(Server)
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded)
  let assert [
    HeadersReceived(stream_id: 1, headers: recv_headers, end_stream: False),
  ] = events
  let assert [
    Header(":method", <<"GET":utf8>>, WithIndexing),
    Header(":scheme", <<"https":utf8>>, WithIndexing),
    Header(":path", <<"/":utf8>>, WithIndexing),
    Header("authorization", <<"Bearer secret":utf8>>, WithoutIndexing),
  ] = recv_headers
}

// Receiving HEADERS preserves NeverIndexed headers
pub fn receive_headers_never_indexed_test() {
  let client = helper.connected_connection(Client)
  let headers =
    list.append(helper.request_headers(), [
      Header("secret-token", <<"abc123":utf8>>, NeverIndexed),
    ])
  let assert Ok(#(_client, encoded, _stream_id)) =
    open_stream(client, headers, False)

  let server = helper.connected_connection(Server)
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, encoded)
  let assert [
    HeadersReceived(stream_id: 1, headers: recv_headers, end_stream: False),
  ] = events
  let assert [
    Header(":method", <<"GET":utf8>>, _),
    Header(":scheme", <<"https":utf8>>, _),
    Header(":path", <<"/":utf8>>, _),
    Header("secret-token", <<"abc123":utf8>>, NeverIndexed),
  ] = recv_headers
}

// --- Stream state validation ---

// RFC 9113 Section 5.1 - An endpoint that sends RST_STREAM "might
// receive frames that were in transit" and "MUST minimally process
// and then discard any frames it receives in this state. This means
// updating header compression state for HEADERS [...] frames."
// HEADERS on a closed stream must be silently discarded (HPACK decoded
// but no event, no response).
pub fn receive_headers_on_closed_stream_is_discarded_test() {
  let client = helper.connected_connection(Client)
  let headers = helper.request_headers()
  // Open stream 1
  let assert Ok(#(client, encoded1, _stream_id)) =
    open_stream(client, headers, False)
  // Encode a RST_STREAM to close stream 1
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.Cancel)
  // Produce a second HEADERS, patch to stream 1
  let assert Ok(#(_client, encoded2, _stream_id)) =
    open_stream(client, headers, False)
  let patched = helper.patch_stream_id(encoded2, 1)

  let server = helper.connected_connection(Server)
  // Receive HEADERS to open stream 1
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  let assert Ok(Open) = get_stream_state(server, 1)
  // Receive RST_STREAM to close stream 1
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, rst)
  let assert Ok(Closed) = get_stream_state(server, 1)

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
// not a connection error - the connection must survive.
pub fn receive_headers_on_half_closed_remote_is_stream_error_test() {
  let client = helper.connected_connection(Client)
  let assert Ok(#(client, encoded1, _stream_id)) =
    open_stream(client, helper.request_headers(), True)
  let assert Ok(#(_client, encoded2, _stream_id)) =
    open_stream(
      client,
      [
        Header(":method", <<"POST":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
      ],
      False,
    )

  let server = helper.connected_connection(Server)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  let assert Ok(HalfClosedRemote) = get_stream_state(server, 1)

  // Patch frame 2 to target stream 1 (half-closed remote)
  let patched = helper.patch_stream_id(encoded2, 1)
  let assert Ok(#(_server, events, _to_send)) = receive_data(server, patched)
  let assert [StreamReset(stream_id: 1, error_code: StreamClosed)] = events
}

// RFC 9113 Section 5.4.2 - Stream error sends RST_STREAM
pub fn receive_headers_on_half_closed_remote_sends_rst_stream_test() {
  let client = helper.connected_connection(Client)
  let assert Ok(#(client, encoded1, _stream_id)) =
    open_stream(client, helper.request_headers(), True)
  let assert Ok(#(_client, encoded2, _stream_id)) =
    open_stream(
      client,
      [
        Header(":method", <<"POST":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
      ],
      False,
    )

  let server = helper.connected_connection(Server)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)

  let patched = helper.patch_stream_id(encoded2, 1)
  let assert Ok(#(_server, _events, to_send)) = receive_data(server, patched)
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.StreamClosed)
  assert to_send == expected_rst
}

// The connection continues processing after a stream error -
// subsequent valid HEADERS on a new stream must succeed.
pub fn receive_headers_after_stream_error_succeeds_test() {
  let client = helper.connected_connection(Client)
  let assert Ok(#(client, encoded1, _stream_id)) =
    open_stream(client, helper.request_headers(), True)
  let assert Ok(#(client, encoded2, _stream_id)) =
    open_stream(
      client,
      [
        Header(":method", <<"POST":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
      ],
      False,
    )
  let assert Ok(#(_client, encoded3, _stream_id)) =
    open_stream(
      client,
      [
        Header(":method", <<"PUT":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
      ],
      False,
    )

  let server = helper.connected_connection(Server)
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
  let client = helper.connected_connection(Client)
  let assert Ok(#(client, encoded1, _stream_id)) =
    open_stream(
      client,
      list.append(helper.request_headers(), [
        Header("x-custom", <<"value1":utf8>>, WithIndexing),
      ]),
      True,
    )
  let assert Ok(#(client, encoded2, _stream_id)) =
    open_stream(
      client,
      list.append(helper.request_headers(), [
        Header("x-custom", <<"value2":utf8>>, WithIndexing),
      ]),
      False,
    )
  let assert Ok(#(_client, encoded3, _stream_id)) =
    open_stream(
      client,
      list.append(helper.request_headers(), [
        Header("x-custom", <<"value3":utf8>>, WithIndexing),
      ]),
      False,
    )

  let server = helper.connected_connection(Server)
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
  // Verify the custom header survived - mandatory pseudo-headers are also present
  let assert [_, _, _, Header("x-custom", <<"value3":utf8>>, _)] = h
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
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)

  // Server advertises MAX_CONCURRENT_STREAMS=1
  let assert Ok(#(server, settings_bytes)) =
    send_settings(server, [MaxConcurrentStreams(1)])
  // Client receives the settings and ACKs
  let assert Ok(#(client, _events, ack_bytes)) =
    receive_data(client, settings_bytes)
  // Server receives the ACK, applying the settings locally
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, ack_bytes)

  // Open stream 1
  let assert Ok(#(_client, encoded1, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  let assert Ok(Open) = get_stream_state(server, 1)

  // Craft a raw HEADERS frame for stream 3 using the static table only
  // (0x82=:method GET, 0x87=:scheme https, 0x84=:path /) so that HPACK state
  // is irrelevant. This bypasses the client-side limit check so we can still
  // test server-side enforcement.
  let assert Ok(encoded3) =
    h2_frame.encode_headers(
      stream_id: 3,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: <<0x82, 0x87, 0x84>>,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) = receive_data(server, encoded3)

  // Should get a RST_STREAM with REFUSED_STREAM
  let assert [StreamReset(stream_id: 3, error_code: RefusedStream)] = events
  let assert Ok(#(frame_data, _rest)) = h2_frame.extract_frame(to_send, 16_384)
  let assert Ok(h2_frame.RstStream(3, h2_frame.RefusedStream)) =
    h2_frame.decode_frame(frame_data)
}

// --- Client-side MAX_CONCURRENT_STREAMS enforcement ---
//
// RFC 9113 Section 5.1.2 - "Endpoints MUST NOT exceed the limit set by
// their peer." open_stream must enforce this before sending, not leave it
// to the server to reject with RST_STREAM.

// Basic case: client may not open a second stream when max is 1 and one is
// already open.
pub fn open_stream_respects_remote_max_concurrent_streams_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(_server, settings_bytes)) =
    send_settings(server, [MaxConcurrentStreams(1)])
  // Client receives the settings - remote_settings takes effect immediately.
  let assert Ok(#(client, _events, _ack)) = receive_data(client, settings_bytes)

  let assert Ok(#(client, _bytes, _id)) =
    open_stream(client, helper.request_headers(), False)

  let assert Error(StreamRefused) =
    open_stream(client, helper.request_headers(), False)
}

// A stream in HalfClosedLocal state (we sent END_STREAM, awaiting response)
// still counts toward the limit.
pub fn open_stream_half_closed_local_counts_toward_limit_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(_server, settings_bytes)) =
    send_settings(server, [MaxConcurrentStreams(1)])
  let assert Ok(#(client, _events, _ack)) = receive_data(client, settings_bytes)

  // Open with END_STREAM - stream goes to HalfClosedLocal.
  let assert Ok(#(client, _bytes, _id)) =
    open_stream(client, helper.request_headers(), True)

  // Still at the limit - HalfClosedLocal counts.
  let assert Error(StreamRefused) =
    open_stream(client, helper.request_headers(), False)
}

// Once a stream closes (RST received from the peer), the slot is freed and
// a new stream may be opened.
pub fn open_stream_closed_stream_frees_slot_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(_server, settings_bytes)) =
    send_settings(server, [MaxConcurrentStreams(1)])
  let assert Ok(#(client, _events, _ack)) = receive_data(client, settings_bytes)

  let assert Ok(#(client, _bytes, _id)) =
    open_stream(client, helper.request_headers(), False)

  // Server RST_STREAMs stream 1 - client closes it.
  let assert Ok(rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.NoError)
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, rst)

  // Slot freed - next open_stream must succeed.
  let assert Ok(#(_client, _bytes, _id)) =
    open_stream(client, helper.request_headers(), False)
}

// MAX_CONCURRENT_STREAMS=0 must prevent any stream from being opened.
pub fn open_stream_max_concurrent_streams_zero_blocks_all_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(_server, settings_bytes)) =
    send_settings(server, [MaxConcurrentStreams(0)])
  let assert Ok(#(client, _events, _ack)) = receive_data(client, settings_bytes)

  let assert Error(StreamRefused) =
    open_stream(client, helper.request_headers(), False)
}

// When the server has not set MAX_CONCURRENT_STREAMS (option.None), there is
// no limit and multiple streams may be opened freely.
pub fn open_stream_no_limit_when_max_concurrent_streams_not_set_test() {
  let client = helper.connected_connection(Client)

  let assert Ok(#(client, _bytes, _)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(client, _bytes, _)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(_client, _bytes, _)) =
    open_stream(client, helper.request_headers(), False)
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
  let client = helper.connected_connection(Client)
  let assert Ok(#(_client, encoded, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  // Patch stream ID from 1 (odd/client) to 2 (even/server)
  let patched = helper.patch_stream_id(encoded, 2)

  let server = helper.connected_connection(Server)
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(server, patched)
}

// A client receiving HEADERS on an odd stream ID (which is reserved
// for client-initiated streams) must reject it.
pub fn client_receive_headers_on_odd_stream_id_is_protocol_error_test() {
  // Build a raw HEADERS frame on stream 2 (even/server-initiated), then patch
  // the stream ID to 1 (odd/client-reserved) to simulate a misbehaving peer.
  let assert Ok(encoded) =
    h2_frame.encode_headers(
      stream_id: 2,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: <<0x88>>,
      padding: option.None,
    )
  // Patch stream ID from 2 to 1
  let patched = helper.patch_stream_id(encoded, 1)

  let client = helper.connected_connection(Client)
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(client, patched)
}

// RFC 9113 Section 5.1.1 - "The identifier of a newly established stream
// MUST be numerically greater than all streams that the initiating endpoint
// has opened or reserved."
//
// A server receiving HEADERS on stream 3 after already having received
// stream 5 must reject it as out-of-order.
pub fn receive_headers_with_decreasing_stream_id_is_protocol_error_test() {
  let client = helper.connected_connection(Client)
  let assert Ok(#(client, encoded1, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(_client, encoded2, _stream_id)) =
    open_stream(client, helper.request_headers(), False)

  let server = helper.connected_connection(Server)
  // Receive stream 1 normally
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, encoded1)
  // Patch stream 5's frame to use stream ID 5, then receive it
  let patched5 = helper.patch_stream_id(encoded2, 5)
  let assert Ok(#(server, _events, _to_send)) = receive_data(server, patched5)

  // Now send HEADERS on stream 3 (lower than 5) - must be rejected
  let assert Ok(#(_client2, encoded3, _stream_id)) =
    open_stream(
      helper.connected_connection(Client),
      helper.request_headers(),
      False,
    )
  let patched3 = helper.patch_stream_id(encoded3, 3)
  let assert Error(ConnectionError(ProtocolError)) =
    receive_data(server, patched3)
}

// RFC 9113 Section 8.3 - "All pseudo-header fields MUST appear in a field
// block before all regular field lines. Any request or response that
// contains a pseudo-header field that appears in a field block after a
// regular field line MUST be treated as malformed (Section 8.1.1)."
//
// A malformed message is a stream error of type PROTOCOL_ERROR.
pub fn receive_headers_pseudo_after_regular_is_malformed_test() {
  let server = helper.connected_connection(Server)
  // HPACK: regular header first, then pseudo-header
  // 0x40 0x05 "x-foo" 0x03 "bar" = literal with indexing: x-foo: bar
  // 0x82 = :method GET (indexed)
  let bad_hpack = <<0x40, 0x05, "x-foo":utf8, 0x03, "bar":utf8, 0x82>>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal. The endpoint
  // sends RST_STREAM and continues processing.
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.3 - "The same pseudo-header field name MUST NOT
// appear more than once in a field block. A field block for an HTTP
// request or response that contains a repeated pseudo-header field name
// MUST be treated as malformed (Section 8.1.1)."
pub fn receive_headers_duplicate_pseudo_header_is_malformed_test() {
  let server = helper.connected_connection(Server)
  // HPACK: :method GET twice
  // 0x82 = :method GET, 0x82 = :method GET (duplicate)
  let bad_hpack = <<0x82, 0x82>>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal. The endpoint
  // sends RST_STREAM and continues processing.
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.3 - "Pseudo-header fields MUST NOT appear in a
// trailer section." and "Endpoints MUST treat a request or response
// that contains undefined or invalid pseudo-header fields as malformed."
//
// Receive trailing HEADERS (with END_STREAM) containing a pseudo-header
// - must be treated as a stream error of type PROTOCOL_ERROR.
pub fn receive_trailers_with_pseudo_header_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let client = helper.connected_connection(Client)
  // Open stream 1 with valid headers
  let assert Ok(#(_client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)

  // Trailers with pseudo-header :method GET - invalid
  // END_STREAM=True marks this as trailers
  let bad_hpack = <<0x82>>
  let assert Ok(trailer_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: True,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal.
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, trailer_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.3 - "Endpoints MUST treat a request or response that
// contains undefined or invalid pseudo-header fields as malformed
// (Section 8.1.1)."
//
// An unknown pseudo-header in a request must be a stream error.
pub fn receive_request_with_unknown_pseudo_header_is_malformed_test() {
  let server = helper.connected_connection(Server)
  // :method GET, :scheme https, :path /, then unknown :foo: bar
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x04, ":foo":utf8, 0x03, "bar":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.3 - "Endpoints MUST treat a request or response that
// contains undefined or invalid pseudo-header fields as malformed
// (Section 8.1.1)."
//
// An unknown pseudo-header in a response must be a stream error.
pub fn receive_response_with_unknown_pseudo_header_is_malformed_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)

  // :status 200, then unknown :bar: baz
  let bad_hpack = <<0x88, 0x40, 0x04, ":bar":utf8, 0x03, "baz":utf8>>
  let assert Ok(response_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_client, events, to_send)) =
    receive_data(client, response_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.3 - "Pseudo-header fields defined for responses MUST
// NOT appear in requests."
//
// :status is a response-only pseudo-header. Receiving it in a request
// must be treated as a stream error.
pub fn receive_request_with_status_pseudo_header_is_malformed_test() {
  let server = helper.connected_connection(Server)
  // :method GET, :scheme https, :path /, :status 200
  let bad_hpack = <<0x82, 0x87, 0x84, 0x88>>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.3 - "Pseudo-header fields defined for requests MUST
// NOT appear in responses."
//
// :method, :scheme, :path, :authority, and :protocol are request-only
// pseudo-headers. Each one appearing in a response must be a stream error.
pub fn receive_response_with_method_pseudo_header_is_malformed_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)
  // :status 200, :method GET
  let bad_hpack = <<0x88, 0x82>>
  let assert Ok(response_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_client, events, to_send)) =
    receive_data(client, response_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

pub fn receive_response_with_scheme_pseudo_header_is_malformed_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)
  // :status 200, :scheme https
  let bad_hpack = <<0x88, 0x87>>
  let assert Ok(response_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_client, events, to_send)) =
    receive_data(client, response_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

pub fn receive_response_with_path_pseudo_header_is_malformed_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)
  // :status 200, :path /
  let bad_hpack = <<0x88, 0x84>>
  let assert Ok(response_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_client, events, to_send)) =
    receive_data(client, response_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

pub fn receive_response_with_authority_pseudo_header_is_malformed_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)
  // :status 200, :authority example.com
  let bad_hpack = <<
    0x88, 0x40, 0x0a, ":authority":utf8, 0x0b, "example.com":utf8,
  >>
  let assert Ok(response_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_client, events, to_send)) =
    receive_data(client, response_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

pub fn receive_response_with_protocol_pseudo_header_is_malformed_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(client, headers, _stream_id)) =
    open_stream(client, helper.request_headers(), False)
  let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)
  // :status 200, :protocol websocket
  let bad_hpack = <<0x88, 0x40, 0x09, ":protocol":utf8, 0x09, "websocket":utf8>>
  let assert Ok(response_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_client, events, to_send)) =
    receive_data(client, response_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.2.2 - "Any message containing connection-specific
// header fields MUST be treated as malformed (Section 8.1.1)."
//
// Connection-specific headers: Connection, Transfer-Encoding, Keep-Alive,
// Proxy-Connection, Upgrade.
pub fn receive_headers_with_connection_header_is_malformed_test() {
  let server = helper.connected_connection(Server)
  // HPACK: :method GET, :scheme https, :path /, then "connection: close"
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x0A, "connection":utf8, 0x05, "close":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal. The endpoint
  // sends RST_STREAM and continues processing.
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

pub fn receive_headers_with_transfer_encoding_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x11, "transfer-encoding":utf8, 0x07, "chunked":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal. The endpoint
  // sends RST_STREAM and continues processing.
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.1 - "A HEADERS frame with the END_STREAM flag set
// that carries an informational status code is malformed (Section 8.1.1)."
//
// A 1xx response with END_STREAM is invalid.
pub fn receive_informational_response_with_end_stream_is_malformed_test() {
  let #(_server, client) = {
    let server = helper.connected_connection(Server)
    let client = helper.connected_connection(Client)
    let assert Ok(#(client, headers, _stream_id)) =
      open_stream(client, helper.request_headers(), False)
    let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)
    #(server, client)
  }
  // 1xx status with END_STREAM - malformed
  // 0x48 0x03 "100" = :status 100 (literal with indexing, name index 8)
  let bad_hpack = <<0x48, 0x03, "100":utf8>>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: True,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal.
  let assert Ok(#(_client, events, to_send)) =
    receive_data(client, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.3.1 - "All HTTP/2 requests MUST include exactly one
// valid value for the ':method', ':scheme', and ':path' pseudo-header
// fields, unless they are CONNECT requests (Section 8.5)."
//
// "An HTTP request that omits mandatory pseudo-header fields is
// malformed (Section 8.1.1)."
pub fn receive_request_missing_method_is_malformed_test() {
  let server = helper.connected_connection(Server)
  // HPACK: :scheme https + :path / - missing :method
  let bad_hpack = <<0x87, 0x84>>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal. The endpoint
  // sends RST_STREAM and continues processing.
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

pub fn receive_request_missing_scheme_is_malformed_test() {
  let server = helper.connected_connection(Server)
  // HPACK: :method GET + :path / - missing :scheme
  let bad_hpack = <<0x82, 0x84>>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal. The endpoint
  // sends RST_STREAM and continues processing.
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

pub fn receive_request_missing_path_is_malformed_test() {
  let server = helper.connected_connection(Server)
  // HPACK: :method GET + :scheme https - missing :path
  let bad_hpack = <<0x82, 0x87>>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal. The endpoint
  // sends RST_STREAM and continues processing.
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.3.2 - "This pseudo-header field [:status] MUST be
// included in all responses, including interim responses; otherwise,
// the response is malformed (Section 8.1.1)."
pub fn receive_response_missing_status_is_malformed_test() {
  let #(_server, client) = {
    let server = helper.connected_connection(Server)
    let client = helper.connected_connection(Client)
    let assert Ok(#(client, headers, _stream_id)) =
      open_stream(client, helper.request_headers(), False)
    let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)
    #(server, client)
  }
  // Response HEADERS with no :status - just a regular header
  // 0x40 0x0C "content-type" 0x09 "text/html"
  let bad_hpack = <<0x40, 0x0C, "content-type":utf8, 0x09, "text/html":utf8>>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal.
  let assert Ok(#(_client, events, to_send)) =
    receive_data(client, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.2.2 - "The only exception to this is the TE header
// field, which MAY be present in an HTTP/2 request; when it is, it MUST
// NOT contain any value other than 'trailers'."
pub fn receive_headers_with_te_non_trailers_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x02, "te":utf8, 0x07, "chunked":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal. The endpoint
  // sends RST_STREAM and continues processing.
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.2.2 - TE header with value "trailers" is allowed.
pub fn receive_headers_with_te_trailers_is_accepted_test() {
  let server = helper.connected_connection(Server)
  let valid_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x02, "te":utf8, 0x08, "trailers":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: valid_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, _to_send)) =
    receive_data(server, headers_frame)
  let assert [HeadersReceived(stream_id: 1, ..)] = events
}

// RFC 9113 Section 8.1 - "An endpoint that receives a HEADERS frame
// without the END_STREAM flag set after receiving the HEADERS frame
// that opens a request or after receiving a final (non-informational)
// status code MUST treat the corresponding request or response as
// malformed (Section 8.1.1)."
pub fn receive_informational_response_after_final_is_malformed_test() {
  let #(_server, client) = {
    let server = helper.connected_connection(Server)
    let client = helper.connected_connection(Client)
    let assert Ok(#(client, headers, _stream_id)) =
      open_stream(client, helper.request_headers(), False)
    let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)
    #(server, client)
  }
  // Final response: 200 OK (0x88 = :status 200 indexed)
  let assert Ok(final_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: <<0x88>>,
      padding: option.None,
    )
  let assert Ok(#(client, _events, _to_send)) =
    receive_data(client, final_frame)
  // 100 Continue after final response - malformed
  let assert Ok(informational_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: <<0x48, 0x03, "100":utf8>>,
      padding: option.None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal.
  let assert Ok(#(_client, events, to_send)) =
    receive_data(client, informational_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.1 - "a server MAY send any number of interim
// responses before the HEADERS frame containing a final response."
pub fn receive_multiple_informational_responses_before_final_test() {
  let #(_server, client) = {
    let server = helper.connected_connection(Server)
    let client = helper.connected_connection(Client)
    let assert Ok(#(client, headers, _stream_id)) =
      open_stream(client, helper.request_headers(), False)
    let assert Ok(#(_server, _events, _to_send)) = receive_data(server, headers)
    #(server, client)
  }
  let assert Ok(info1) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: <<0x48, 0x03, "100":utf8>>,
      padding: option.None,
    )
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, info1)
  let assert Ok(info2) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: <<0x48, 0x03, "100":utf8>>,
      padding: option.None,
    )
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, info2)
  // Final 200 OK
  let assert Ok(final_resp) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: <<0x88>>,
      padding: option.None,
    )
  let assert Ok(#(_client, events, _to_send)) = receive_data(client, final_resp)
  let assert [HeadersReceived(stream_id: 1, ..)] = events
}

// RFC 9113 Section 8.3.1 - "This pseudo-header field [:path] MUST NOT
// be empty for 'http' or 'https' URIs; 'http' or 'https' URIs that do
// not contain a path component MUST include a value of '/'."
pub fn receive_request_with_empty_path_is_malformed_test() {
  let server = helper.connected_connection(Server)
  // HPACK: :method GET, :scheme https, :path "" (empty)
  // 0x44 = literal with indexing, name index 4 (:path)
  // 0x00 = value length 0
  let bad_hpack = <<0x82, 0x87, 0x44, 0x00>>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal. The endpoint
  // sends RST_STREAM and continues processing.
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.5 - "The ':scheme' and ':path' pseudo-header
// fields MUST be omitted" for CONNECT requests.
// "A CONNECT request that does not conform to these restrictions is
// malformed (Section 8.1.1)."
//
// A CONNECT request with :path present is malformed.
pub fn receive_connect_request_with_path_is_malformed_test() {
  let server = helper.connected_connection(Server)
  // HPACK: :method CONNECT, :authority example.com, :path /
  // 0x86 = :method CONNECT? No - not in static table. Use literal.
  // Literal with indexing, name index 2 (:method), value "CONNECT"
  let bad_hpack = <<
    0x42, 0x07, "CONNECT":utf8, 0x41, 0x0B, "example.com":utf8, 0x84,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal. The endpoint
  // sends RST_STREAM and continues processing.
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.5 - A valid CONNECT request has only :method and
// :authority (no :scheme, no :path).
pub fn receive_valid_connect_request_is_accepted_test() {
  let server = helper.connected_connection(Server)
  // HPACK: :method CONNECT, :authority example.com
  let valid_hpack = <<
    0x42, 0x07, "CONNECT":utf8, 0x41, 0x0B, "example.com":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: valid_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, _to_send)) =
    receive_data(server, headers_frame)
  let assert [HeadersReceived(stream_id: 1, ..)] = events
}

// RFC 9113 Section 8.2.1 - "A field name MUST NOT contain characters
// in the ranges 0x00-0x20, 0x41-0x5a, or 0x7f-0xff (all ranges
// inclusive)."
//
// Valid UTF-8 non-ASCII characters (e.g. "é" = 0xC3 0xA9) use bytes
// in the 0x80-0xff range, which the RFC explicitly forbids in field names.
pub fn receive_headers_non_ascii_utf8_field_name_is_malformed_test() {
  let server = helper.connected_connection(Server)
  // "x-café" is valid UTF-8 but contains bytes 0xC3 0xA9 (> 0x7f)
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x07, "x-caf":utf8, 0xC3, 0xA9, 0x05, "value":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.2.1 - "A field name MUST NOT contain characters
// in the ranges 0x00-0x20, 0x41-0x5a, or 0x7f-0xff (all ranges
// inclusive). This specifically excludes... uppercase characters
// ('A' to 'Z', ASCII 0x41 to 0x5a)."
pub fn receive_headers_uppercase_field_name_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x07, "X-Upper":utf8, 0x05, "value":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  // RFC 9113 Section 5.4.2 - Stream errors are non-fatal.
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.2.1 - "A field name MUST NOT contain characters
// in the ranges 0x00-0x20, 0x41-0x5a, or 0x7f-0xff (all ranges
// inclusive)." NUL (0x00) in a field name must be rejected.
pub fn receive_headers_nul_in_field_name_is_malformed_test() {
  let server = helper.connected_connection(Server)
  // Field name "x\x00a" contains NUL byte
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x03, "x":utf8, 0x00, "a":utf8, 0x05, "value":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.2.1 - "A field name MUST NOT contain characters
// in the ranges 0x00-0x20, 0x41-0x5a, or 0x7f-0xff (all ranges
// inclusive)." Control character BEL (0x07) in a field name must be rejected.
pub fn receive_headers_control_char_in_field_name_is_malformed_test() {
  let server = helper.connected_connection(Server)
  // Field name "x\x07a" contains BEL control character
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x03, "x":utf8, 0x07, "a":utf8, 0x05, "value":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.2.1 - "A field name MUST NOT contain characters
// in the ranges 0x00-0x20, 0x41-0x5a, or 0x7f-0xff (all ranges
// inclusive). This specifically excludes all non-visible ASCII
// characters, ASCII SP (0x20)..."
pub fn receive_headers_space_in_field_name_is_malformed_test() {
  let server = helper.connected_connection(Server)
  // Field name "x a" contains space (0x20)
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x03, "x":utf8, 0x20, "a":utf8, 0x05, "value":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.2.1 - "A field name MUST NOT contain characters
// in the ranges 0x00-0x20, 0x41-0x5a, or 0x7f-0xff (all ranges
// inclusive)." DEL (0x7f) sits at the boundary of the 0x7f-0xff range
// and must be rejected.
pub fn receive_headers_del_in_field_name_is_malformed_test() {
  let server = helper.connected_connection(Server)
  // Field name "x\x7fa" contains DEL character
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x03, "x":utf8, 0x7F, "a":utf8, 0x05, "value":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.2.1 - "With the exception of pseudo-header fields,
// which have a name that starts with a single colon, field names MUST
// NOT include a colon (ASCII COLON, 0x3a)."
pub fn receive_headers_colon_in_field_name_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x05, "x:bad":utf8, 0x05, "value":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.2.1 / [HTTP] Section 5.1 - field-name = token = 1*tchar.
// A zero-length field name contains no characters and is therefore not a valid
// token. It must be rejected as a malformed message.
//
// HPACK: 0x40 = literal with indexing, new name; 0x00 = name length 0;
// 0x05 = value length 5; "value" = value bytes.
pub fn receive_headers_empty_field_name_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let bad_hpack = <<0x82, 0x87, 0x84, 0x40, 0x00, 0x05, "value":utf8>>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.2.1 - "A field value MUST NOT contain the zero
// value (ASCII NUL, 0x00), line feed (ASCII LF, 0x0a), or carriage
// return (ASCII CR, 0x0d) at any position."
pub fn receive_headers_field_value_with_nul_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x05, "x-foo":utf8, 0x07, "bad":utf8, 0x00,
    "val":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

pub fn receive_headers_field_value_with_lf_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x05, "x-foo":utf8, 0x07, "bad":utf8, 0x0a,
    "val":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

pub fn receive_headers_field_value_with_cr_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x05, "x-foo":utf8, 0x07, "bad":utf8, 0x0d,
    "val":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.2.1 - "A field value MUST NOT start or end with
// an ASCII whitespace character (ASCII SP or HTAB, 0x20 or 0x09)."
pub fn receive_headers_field_value_leading_space_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x05, "x-foo":utf8, 0x06, " value":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

pub fn receive_headers_field_value_trailing_space_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x05, "x-foo":utf8, 0x06, "value ":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

pub fn receive_headers_field_value_leading_tab_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x05, "x-foo":utf8, 0x06, "\tvalue":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

pub fn receive_headers_field_value_trailing_tab_is_malformed_test() {
  let server = helper.connected_connection(Server)
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x05, "x-foo":utf8, 0x06, "value\t":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.2.1 - Field names and values are conveyed as byte
// sequences via HPACK (RFC 7541). Since h2_core exposes headers as Strings,
// we MUST reject any header whose name or value is not valid UTF-8.
// Invalid UTF-8 makes the header unrepresentable, so this is treated as
// a malformed message: stream error of type PROTOCOL_ERROR (Section 8.1.1).

// Invalid UTF-8 in a header field name (truncated 2-byte sequence: 0xC3
// without continuation byte).
pub fn receive_headers_invalid_utf8_in_field_name_is_protocol_error_test() {
  let server = helper.connected_connection(Server)
  // 0x82=:method GET, 0x87=:scheme https, 0x84=:path /
  // 0x40=literal with indexing, new name
  // 0x03=name length 3, then 3 raw bytes including 0xC3 (invalid UTF-8)
  // 0x05=value length 5, "value"
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x03, "x":utf8, 0xC3, 0x00, 0x05, "value":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, to_send)) =
    receive_data(server, headers_frame)
  assert events == [StreamReset(stream_id: 1, error_code: ProtocolError)]
  let assert Ok(expected_rst) =
    h2_frame.encode_rst_stream(stream_id: 1, error_code: h2_frame.ProtocolError)
  assert to_send == expected_rst
}

// RFC 9113 Section 8.2.1: a field value MUST NOT contain NUL, LF, or CR,
// and MUST NOT start or end with whitespace. Bytes 0x80-0xFF (obs-text) are
// NOT prohibited. A value containing obs-text bytes MUST be passed through
// as HeadersReceived with the raw bytes intact.
pub fn receive_headers_obs_text_in_field_value_is_accepted_test() {
  let server = helper.connected_connection(Server)
  // 0x40=literal with indexing, new name
  // 0x05=name length 5, "x-foo"
  // 0x03=value length 3, then 3 raw bytes including 0x80 (obs-text)
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x05, "x-foo":utf8, 0x03, "a":utf8, 0x80, "b":utf8,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, _to_send)) =
    receive_data(server, headers_frame)
  assert events
    == [
      HeadersReceived(
        stream_id: 1,
        headers: [
          Header(":method", <<"GET":utf8>>, WithIndexing),
          Header(":scheme", <<"https":utf8>>, WithIndexing),
          Header(":path", <<"/":utf8>>, WithIndexing),
          Header("x-foo", <<97, 128, 98>>, WithIndexing),
        ],
        end_stream: False,
      ),
    ]
}

// RFC 9113 Section 8.2.1: bytes 0x80-0xFF (obs-text) are not prohibited in
// field values. A value consisting entirely of obs-text bytes MUST be passed
// through as HeadersReceived with the raw bytes intact.
pub fn receive_headers_all_obs_text_bytes_in_value_is_accepted_test() {
  let server = helper.connected_connection(Server)
  let bad_hpack = <<
    0x82, 0x87, 0x84, 0x40, 0x05, "x-foo":utf8, 0x02, 0xFF, 0xFE,
  >>
  let assert Ok(headers_frame) =
    h2_frame.encode_headers(
      stream_id: 1,
      end_stream: False,
      end_headers: True,
      priority: option.None,
      field_block_fragment: bad_hpack,
      padding: option.None,
    )
  let assert Ok(#(_server, events, _to_send)) =
    receive_data(server, headers_frame)
  assert events
    == [
      HeadersReceived(
        stream_id: 1,
        headers: [
          Header(":method", <<"GET":utf8>>, WithIndexing),
          Header(":scheme", <<"https":utf8>>, WithIndexing),
          Header(":path", <<"/":utf8>>, WithIndexing),
          Header("x-foo", <<0xFF, 0xFE>>, WithIndexing),
        ],
        end_stream: False,
      ),
    ]
}

// =============================================================================
// Bug: ReservedRemote -> HalfClosedLocal transition must set
// final_response_received so that subsequent trailers are validated correctly.
// =============================================================================

// RFC 9113 Section 8.1: "An HTTP request/response exchange fully consumes a
// single stream. [...] Trailers are indicated by a header block with the
// END_STREAM flag set."
//
// When a client receives a push response (HEADERS on a ReservedRemote stream),
// final_response_received must be set so that subsequent HEADERS+END_STREAM
// are treated as trailers rather than a second response missing :status.
pub fn receive_trailers_on_push_promise_stream_test() {
  let #(server, client, promised_id) =
    helper.client_with_reserved_remote_stream()
  let assert Ok(ReservedRemote) = get_stream_state(client, promised_id)

  // Server sends the push response (200) on the promised stream without END_STREAM
  let assert Ok(#(server, response_bytes)) =
    send_headers(
      server,
      promised_id,
      [Header(":status", <<"200":utf8>>, WithIndexing)],
      False,
    )
  let assert Ok(#(client, events, _to_send)) =
    receive_data(client, response_bytes)
  let assert [HeadersReceived(stream_id: sid, end_stream: False, ..)] = events
  assert sid == promised_id
  let assert Ok(HalfClosedLocal) = get_stream_state(client, promised_id)

  // Server sends some data
  let assert Ok(#(server, data_bytes)) =
    send_data(server, promised_id, <<"body":utf8>>, False, option.None)
  let assert Ok(#(client, _events, _to_send)) = receive_data(client, data_bytes)

  // Server sends trailers (HEADERS + END_STREAM) - no :status pseudo-header,
  // which is correct for trailers. This must be accepted.
  let assert Ok(#(_server, trailer_bytes)) =
    send_headers(
      server,
      promised_id,
      [Header("x-checksum", <<"abc123":utf8>>, WithIndexing)],
      True,
    )
  let assert Ok(#(client, events, _to_send)) =
    receive_data(client, trailer_bytes)
  // Should get HeadersReceived for the trailers and StreamEnded
  let assert [
    HeadersReceived(stream_id: trailer_sid, end_stream: True, ..),
    StreamEnded(stream_id: end_sid),
  ] = events
  assert trailer_sid == promised_id
  assert end_sid == promised_id
  let assert Ok(Closed) = get_stream_state(client, promised_id)
}

// =============================================================================
// Content-Length mismatch on HEADERS + END_STREAM
// RFC 9113 Section 8.1.1: "A request or response is also malformed if the
// value of a content-length header field does not equal the sum of the DATA
// frame payload lengths that form the content"
// =============================================================================

// Server-side: a POST request with content-length but END_STREAM on HEADERS
// means zero DATA frames will follow. content-length > 0 is a mismatch.
pub fn receive_request_with_content_length_and_end_stream_is_malformed_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(_client, headers_bytes, _stream_id)) =
    open_stream(
      client,
      [
        Header(":method", <<"POST":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
        Header("content-length", <<"100":utf8>>, WithIndexing),
      ],
      True,
    )
  let assert Ok(#(server, events, _to_send)) =
    receive_data(server, headers_bytes)
  // Should be rejected as malformed - content-length says 100 but no DATA
  let assert [StreamReset(stream_id: 1, error_code: ProtocolError)] = events
  let assert Ok(Closed) = get_stream_state(server, 1)
}

// Server-side: content-length: 0 with END_STREAM is valid - zero bytes
// promised, zero bytes delivered.
pub fn receive_request_with_zero_content_length_and_end_stream_is_valid_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(_client, headers_bytes, _stream_id)) =
    open_stream(
      client,
      [
        Header(":method", <<"POST":utf8>>, WithIndexing),
        Header(":scheme", <<"https":utf8>>, WithIndexing),
        Header(":path", <<"/":utf8>>, WithIndexing),
        Header("content-length", <<"0":utf8>>, WithIndexing),
      ],
      True,
    )
  let assert Ok(#(server, events, _to_send)) =
    receive_data(server, headers_bytes)
  let assert [
    HeadersReceived(stream_id: 1, end_stream: True, ..),
    StreamEnded(stream_id: 1),
  ] = events
  let assert Ok(HalfClosedRemote) = get_stream_state(server, 1)
}

// Server-side: a GET request with no content-length and END_STREAM is normal.
pub fn receive_get_request_with_end_stream_and_no_content_length_is_valid_test() {
  let #(server, client) = helper.connected_pair()
  let assert Ok(#(_client, headers_bytes, _stream_id)) =
    open_stream(client, helper.request_headers(), True)
  let assert Ok(#(server, events, _to_send)) =
    receive_data(server, headers_bytes)
  let assert [
    HeadersReceived(stream_id: 1, end_stream: True, ..),
    StreamEnded(stream_id: 1),
  ] = events
  let assert Ok(HalfClosedRemote) = get_stream_state(server, 1)
}

// Client-side: a 200 response with content-length: 100 and END_STREAM on
// HEADERS means zero DATA. This is malformed for a normal response.
pub fn receive_response_with_content_length_and_end_stream_is_malformed_test() {
  let #(server, client) = helper.server_with_open_stream()
  let assert Ok(#(_server, response_bytes)) =
    send_headers(
      server,
      1,
      [
        Header(":status", <<"200":utf8>>, WithIndexing),
        Header("content-length", <<"100":utf8>>, WithIndexing),
      ],
      True,
    )
  let assert Ok(#(client, events, _to_send)) =
    receive_data(client, response_bytes)
  let assert [StreamReset(stream_id: 1, error_code: ProtocolError)] = events
  let assert Ok(Closed) = get_stream_state(client, 1)
}

// Client-side: RFC 9113 Section 8.1.1 - "A response that is defined as having
// no content... MAY have a non-zero content-length header field, even though
// no content is included in DATA frames."
// 204 No Content with content-length is valid.
pub fn receive_204_response_with_content_length_and_end_stream_is_valid_test() {
  let #(server, client) = helper.server_with_open_stream()
  let assert Ok(#(_server, response_bytes)) =
    send_headers(
      server,
      1,
      [
        Header(":status", <<"204":utf8>>, WithIndexing),
        Header("content-length", <<"100":utf8>>, WithIndexing),
      ],
      True,
    )
  let assert Ok(#(client, events, _to_send)) =
    receive_data(client, response_bytes)
  let assert [
    HeadersReceived(stream_id: 1, end_stream: True, ..),
    StreamEnded(stream_id: 1),
  ] = events
  // Open -> HalfClosedRemote (client received END_STREAM, but hasn't sent it)
  let assert Ok(HalfClosedRemote) = get_stream_state(client, 1)
}

// Client-side: 304 Not Modified with content-length is valid per RFC 9113
// Section 8.1.1 (defined as having no content).
pub fn receive_304_response_with_content_length_and_end_stream_is_valid_test() {
  let #(server, client) = helper.server_with_open_stream()
  let assert Ok(#(_server, response_bytes)) =
    send_headers(
      server,
      1,
      [
        Header(":status", <<"304":utf8>>, WithIndexing),
        Header("content-length", <<"100":utf8>>, WithIndexing),
      ],
      True,
    )
  let assert Ok(#(client, events, _to_send)) =
    receive_data(client, response_bytes)
  let assert [
    HeadersReceived(stream_id: 1, end_stream: True, ..),
    StreamEnded(stream_id: 1),
  ] = events
  let assert Ok(HalfClosedRemote) = get_stream_state(client, 1)
}
