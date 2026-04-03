// Re-export modules needed by integration tests.
// The main binary is in main.rs; this lib target allows `cargo test --test integration_remote`.

pub mod azure_blob;
pub mod debug_log;
pub mod gcs;
pub mod nfs_client;
pub mod panel;
pub mod remote_fs;
pub mod s3;
pub mod sftp;
pub mod smb_client;
pub mod ssh;
pub mod webdav;
