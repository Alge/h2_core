import gleam/option.{None}
import gleeunit
import h2_core.{Client, Server, new_connection}

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn new_client_connection_test() {
  let conn = new_connection(Client)
  assert conn.role == Client
}

pub fn new_server_connection_test() {
  let conn = new_connection(Server)
  assert conn.role == Server
}

// RFC 9113 Section 6.5.2 - Default settings values
pub fn new_connection_default_settings_test() {
  let conn = new_connection(Client)
  let settings = conn.local_settings

  assert settings.header_table_size == 4096
  assert settings.enable_push == True
  assert settings.max_concurrent_streams == None
  assert settings.initial_window_size == 65_535
  assert settings.max_frame_size == 16_384
  assert settings.max_header_list_size == None
}

pub fn new_connection_remote_settings_match_defaults_test() {
  let conn = new_connection(Client)
  assert conn.local_settings == conn.remote_settings
}
