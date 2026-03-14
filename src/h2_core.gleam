import gleam/option

pub type Role{
    Client
    Server
}


pub type Settings{
    Setting(
        header_table_size: Int,
        enable_push: Bool,
        max_concurrent_streams: option.Option(Int),
        initial_window_size: Int,
        max_frame_size: Int,
        max_header_list_size: option.Option(Int),
    )
}

fn default_settings() -> Settings {
    Setting(
        header_table_size: 4096,
        enable_push: True,
        max_concurrent_streams: option.None,
        initial_window_size: 65535,
        max_frame_size: 16384,
        max_header_list_size: option.None
    )
}

pub type Connection{
    Connection(
        role: Role,
        local_settings: Settings,
        remote_settings: Settings,
    )
}

pub fn new_connection(role: Role) -> Connection{
    Connection(
        role: role,
        local_settings: default_settings(),
        remote_settings: default_settings(),
    )
}