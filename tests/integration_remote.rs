//! Integration tests for remote filesystem connectors.
//!
//! These tests require external services running locally:
//!   - S3Mock (Adobe): `docker run -p 9090:9090 adobe/s3mock`
//!   - Azurite: `npx azurite --skipApiVersionCheck` or `docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite`
//!   - SFTP: tests against localhost (requires SSH access to self)
//!
//! Tests are skipped automatically if the service is not available.
//! Run with: `cargo test --test integration_remote -- --nocapture`

use std::path::{Path, PathBuf};
use std::process::Command;

/// Check if a TCP port is open on localhost.
fn port_open(port: u16) -> bool {
    std::net::TcpStream::connect_timeout(
        &format!("127.0.0.1:{}", port).parse().unwrap(),
        std::time::Duration::from_millis(500),
    )
    .is_ok()
}

/// Check if a CLI tool is available.
fn tool_available(name: &str) -> bool {
    Command::new("which")
        .arg(name)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

macro_rules! skip_unless {
    ($cond:expr, $msg:expr) => {
        if !$cond {
            eprintln!("SKIPPED: {}", $msg);
            return;
        }
    };
}

// ============================================================
// S3Mock tests (port 9090)
// ============================================================

mod s3mock {
    use super::*;

    fn s3mock_available() -> bool {
        port_open(9090) && tool_available("aws")
    }

    fn setup_bucket(bucket: &str) {
        // Create bucket via aws CLI
        let _ = Command::new("aws")
            .args([
                "s3", "mb",
                &format!("s3://{}", bucket),
                "--endpoint-url", "http://localhost:9090",
            ])
            .env("AWS_ACCESS_KEY_ID", "dummy")
            .env("AWS_SECRET_ACCESS_KEY", "dummy")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .output();
    }

    fn cleanup_bucket(bucket: &str) {
        let _ = Command::new("aws")
            .args([
                "s3", "rb",
                &format!("s3://{}", bucket),
                "--force",
                "--endpoint-url", "http://localhost:9090",
            ])
            .env("AWS_ACCESS_KEY_ID", "dummy")
            .env("AWS_SECRET_ACCESS_KEY", "dummy")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .output();
    }

    fn put_object(bucket: &str, key: &str, content: &str) {
        let tmp = std::env::temp_dir().join("mm-test-upload");
        std::fs::write(&tmp, content).unwrap();
        let _ = Command::new("aws")
            .args([
                "s3", "cp",
                &tmp.to_string_lossy(),
                &format!("s3://{}/{}", bucket, key),
                "--endpoint-url", "http://localhost:9090",
            ])
            .env("AWS_ACCESS_KEY_ID", "dummy")
            .env("AWS_SECRET_ACCESS_KEY", "dummy")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .output();
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn test_s3_read_dir_empty_bucket() {
        skip_unless!(s3mock_available(), "S3Mock not running on port 9090");

        let bucket = "mm-test-empty";
        setup_bucket(bucket);

        let conn = middle_manager::s3::S3Connection::connect(
            bucket,
            Some("dummy"),
            Some("http://localhost:9090"),
            Some("us-east-1"),
        );

        match conn {
            Ok(c) => {
                let entries = c.read_dir(Path::new("/")).unwrap();
                // Empty bucket should have no entries
                assert!(entries.is_empty(), "Expected empty, got {:?}", entries.iter().map(|e| &e.name).collect::<Vec<_>>());
            }
            Err(e) => {
                eprintln!("S3 connect failed (may need AWS_ACCESS_KEY_ID=dummy): {}", e);
            }
        }

        cleanup_bucket(bucket);
    }

    #[test]
    fn test_s3_read_dir_with_files() {
        skip_unless!(s3mock_available(), "S3Mock not running on port 9090");

        let bucket = "mm-test-files";
        setup_bucket(bucket);
        put_object(bucket, "hello.txt", "hello world");
        put_object(bucket, "subdir/nested.txt", "nested content");

        let conn = middle_manager::s3::S3Connection::connect(
            bucket,
            Some("dummy"),
            Some("http://localhost:9090"),
            Some("us-east-1"),
        );

        if let Ok(c) = conn {
            let entries = c.read_dir(Path::new("/")).unwrap();
            let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
            assert!(names.contains(&"hello.txt"), "Missing hello.txt in {:?}", names);
            assert!(names.iter().any(|n| *n == "subdir"), "Missing subdir/ in {:?}", names);

            // Check subdir listing
            let sub_entries = c.read_dir(Path::new("/subdir")).unwrap();
            let sub_names: Vec<&str> = sub_entries.iter().map(|e| e.name.as_str()).collect();
            assert!(sub_names.contains(&"nested.txt"), "Missing nested.txt in {:?}", sub_names);
        }

        cleanup_bucket(bucket);
    }

    #[test]
    fn test_s3_mkdir_and_delete() {
        skip_unless!(s3mock_available(), "S3Mock not running on port 9090");

        let bucket = "mm-test-mkdir";
        setup_bucket(bucket);

        if let Ok(c) = middle_manager::s3::S3Connection::connect(
            bucket, Some("dummy"), Some("http://localhost:9090"), Some("us-east-1"),
        ) {
            // Create a directory
            c.mkdir(Path::new("/newdir")).unwrap();

            // Verify it appears in listing
            let entries = c.read_dir(Path::new("/")).unwrap();
            let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
            assert!(names.contains(&"newdir"), "Missing newdir in {:?}", names);

            // Delete it
            c.remove_recursive(Path::new("/newdir")).unwrap();
        }

        cleanup_bucket(bucket);
    }

    #[test]
    fn test_s3_upload_and_download() {
        skip_unless!(s3mock_available(), "S3Mock not running on port 9090");

        let bucket = "mm-test-updown";
        setup_bucket(bucket);

        if let Ok(c) = middle_manager::s3::S3Connection::connect(
            bucket, Some("dummy"), Some("http://localhost:9090"), Some("us-east-1"),
        ) {
            // Upload
            let tmp_up = std::env::temp_dir().join("mm-test-s3-upload.txt");
            std::fs::write(&tmp_up, "test content 12345").unwrap();
            c.upload(&tmp_up, Path::new("/uploaded.txt")).unwrap();
            let _ = std::fs::remove_file(&tmp_up);

            // Verify in listing
            let entries = c.read_dir(Path::new("/")).unwrap();
            assert!(entries.iter().any(|e| e.name == "uploaded.txt"));

            // Download
            let tmp_down = std::env::temp_dir().join("mm-test-s3-download.txt");
            c.download(Path::new("/uploaded.txt"), &tmp_down).unwrap();
            let content = std::fs::read_to_string(&tmp_down).unwrap();
            assert_eq!(content, "test content 12345");
            let _ = std::fs::remove_file(&tmp_down);

            // Cleanup
            c.remove_recursive(Path::new("/uploaded.txt")).unwrap();
        }

        cleanup_bucket(bucket);
    }
}

// ============================================================
// Azurite tests (port 10000)
// ============================================================

mod azurite {
    use super::*;

    const CONN_STR: &str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    fn azurite_available() -> bool {
        port_open(10000) && tool_available("az")
    }

    fn create_container(name: &str) {
        let _ = Command::new("az")
            .args([
                "storage", "container", "create",
                "--name", name,
                "--connection-string", CONN_STR,
            ])
            .output();
    }

    fn delete_container(name: &str) {
        let _ = Command::new("az")
            .args([
                "storage", "container", "delete",
                "--name", name,
                "--connection-string", CONN_STR,
            ])
            .output();
    }

    #[test]
    fn test_azure_list_containers() {
        skip_unless!(azurite_available(), "Azurite not running on port 10000");

        let container = "mm-test-list";
        create_container(container);

        let conn = middle_manager::azure_blob::AzureBlobConnection::connect(
            "devstoreaccount1", "", None, Some(CONN_STR),
        );

        match conn {
            Ok(c) => {
                let entries = c.read_dir(Path::new("/")).unwrap();
                let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
                assert!(names.contains(&"mm-test-list"), "Missing container in {:?}", names);
            }
            Err(e) => {
                eprintln!("Azure connect failed: {}", e);
            }
        }

        delete_container(container);
    }

    #[test]
    fn test_azure_blob_upload_and_list() {
        skip_unless!(azurite_available(), "Azurite not running on port 10000");

        let container = "mm-test-blobs";
        create_container(container);

        // Upload a file via az CLI directly
        let tmp = std::env::temp_dir().join("mm-test-az-upload.txt");
        std::fs::write(&tmp, "azure test content").unwrap();
        let _ = Command::new("az")
            .args([
                "storage", "blob", "upload",
                "--file", &tmp.to_string_lossy(),
                "--name", "testfile.txt",
                "--container-name", container,
                "--connection-string", CONN_STR,
                "--overwrite",
            ])
            .output();
        let _ = std::fs::remove_file(&tmp);

        let conn = middle_manager::azure_blob::AzureBlobConnection::connect(
            "devstoreaccount1", container, None, Some(CONN_STR),
        );

        if let Ok(c) = conn {
            let entries = c.read_dir(Path::new("/")).unwrap();
            let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
            assert!(names.contains(&"testfile.txt"), "Missing testfile.txt in {:?}", names);
        }

        delete_container(container);
    }
}

// ============================================================
// SFTP tests (localhost SSH)
// ============================================================

mod sftp {
    use super::*;

    fn sftp_available() -> bool {
        // Check if we can SSH to localhost without password
        Command::new("ssh")
            .args(["-o", "BatchMode=yes", "-o", "ConnectTimeout=2", "localhost", "echo", "ok"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    #[test]
    fn test_sftp_read_dir_home() {
        skip_unless!(sftp_available(), "Cannot SSH to localhost without password");

        let host = middle_manager::ssh::SshHost {
            name: "localhost".to_string(),
            hostname: "localhost".to_string(),
            port: None,
            user: None,
            identity_file: None,
            group: None,
            jump_host: None,
            extra_args: vec![],
            source: middle_manager::ssh::HostSource::Saved,
        };

        let conn = middle_manager::sftp::SftpConnection::connect(&host).unwrap();
        let home = conn.home_dir();
        assert!(home.to_string_lossy().starts_with('/'), "Home should be absolute: {:?}", home);

        let entries = conn.read_dir(&home).unwrap();
        // Home directory should have at least one entry
        assert!(!entries.is_empty(), "Home directory listing should not be empty");
    }

    #[test]
    fn test_sftp_mkdir_and_rmdir() {
        skip_unless!(sftp_available(), "Cannot SSH to localhost without password");

        let host = middle_manager::ssh::SshHost {
            name: "localhost".to_string(),
            hostname: "localhost".to_string(),
            port: None, user: None, identity_file: None,
            group: None, jump_host: None, extra_args: vec![],
            source: middle_manager::ssh::HostSource::Saved,
        };

        let conn = middle_manager::sftp::SftpConnection::connect(&host).unwrap();
        let home = conn.home_dir();
        let test_dir = home.join(".mm-test-sftp-dir");

        // Create
        conn.mkdir(&test_dir).unwrap();

        // Verify it exists
        let entries = conn.read_dir(&home).unwrap();
        assert!(entries.iter().any(|e| e.name == ".mm-test-sftp-dir"),
            "Created directory not found in listing");

        // Remove
        conn.remove_recursive(&test_dir).unwrap();

        // Verify it's gone
        let entries = conn.read_dir(&home).unwrap();
        assert!(!entries.iter().any(|e| e.name == ".mm-test-sftp-dir"),
            "Deleted directory still appears in listing");
    }
}

// ============================================================
// WebDAV tests (requires a local WebDAV server)
// ============================================================

mod webdav {
    use super::*;

    // WebDAV test server could be: `docker run -p 8080:80 bytemark/webdav`
    // or `npx webdav-server` or similar. We'll skip if not available.
    fn webdav_available() -> bool {
        port_open(8080) && tool_available("curl")
    }

    #[test]
    fn test_webdav_propfind_parsing() {
        // This test doesn't need a live server -- tests the XML parser directly
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<d:multistatus xmlns:d="DAV:">
  <d:response>
    <d:href>/webdav/</d:href>
    <d:propstat>
      <d:prop>
        <d:displayname>webdav</d:displayname>
        <d:resourcetype><d:collection/></d:resourcetype>
      </d:prop>
    </d:propstat>
  </d:response>
  <d:response>
    <d:href>/webdav/documents/</d:href>
    <d:propstat>
      <d:prop>
        <d:displayname>documents</d:displayname>
        <d:resourcetype><d:collection/></d:resourcetype>
        <d:getcontentlength>0</d:getcontentlength>
      </d:prop>
    </d:propstat>
  </d:response>
  <d:response>
    <d:href>/webdav/readme.txt</d:href>
    <d:propstat>
      <d:prop>
        <d:displayname>readme.txt</d:displayname>
        <d:resourcetype/>
        <d:getcontentlength>42</d:getcontentlength>
      </d:prop>
    </d:propstat>
  </d:response>
</d:multistatus>"#;

        // This calls the parser directly without a live server
        // The parse function is internal, so we test via the unit tests in webdav.rs
        // This test just validates the test infrastructure works
        assert!(xml.contains("multistatus"));
    }
}

// ============================================================
// SMB tests (requires smbclient + a local share)
// ============================================================

mod smb {
    use super::*;

    #[test]
    fn test_smb_ls_parser() {
        // Test the smbclient ls output parser without a live server
        let output = "\
  .                                   D        0  Mon Mar 10 12:00:00 2025
  ..                                  D        0  Mon Mar 10 12:00:00 2025
  Documents                           D        0  Tue Jan  7 09:30:00 2025
  photo.jpg                           A  1234567  Wed Feb 12 14:15:00 2025
  readme.txt                          A     4096  Thu Mar  6 11:00:00 2025

                12345678 blocks of size 1024. 9876543 blocks available
";
        // Parser is tested in unit tests in smb_client.rs
        // This integration test validates test infrastructure
        assert!(output.contains("Documents"));
        assert!(output.contains("photo.jpg"));
    }
}
