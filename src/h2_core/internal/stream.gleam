import gleam/option

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
  Stream(
    state: StreamState,
    send_window_size: Int,
    recv_window_size: Int,
    headers_sent: Bool,
    final_response_received: Bool,
    expected_content_length: option.Option(Int),
    received_content_length: Int,
  )
}

pub fn new_stream(
  send_window_size send_window_size: Int,
  recv_window_size recv_window_size: Int,
) -> Stream {
  Stream(
    state: Idle,
    send_window_size: send_window_size,
    recv_window_size: recv_window_size,
    headers_sent: False,
    final_response_received: False,
    expected_content_length: option.None,
    received_content_length: 0,
  )
}
