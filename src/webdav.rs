use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::panel::entry::FileEntry;

/// A WebDAV connection that shells out to `curl`.
pub struct WebDavConnection {
    /// Base URL, e.g. "https://cloud.example.com/remote.php/dav/files/user"
    pub base_url: String,
    pub username: String,
    password: String,
    /// When true, pass `-k` to curl to allow self-signed certificates.
    /// Default is false (TLS verification enabled).
    pub insecure: bool,
}

impl WebDavConnection {
    /// Connect and validate with a PROPFIND on root.
    pub fn connect(url: &str, username: &str, password: &str) -> Result<Self> {
        let base_url = url.trim_end_matches('/').to_string();
        let conn = Self {
            base_url,
            username: username.to_string(),
            password: password.to_string(),
            insecure: false,
        };
        // Validate connection
        conn.propfind("/", 0)?;
        Ok(conn)
    }

    /// Display label for panel header.
    pub fn display_label(&self) -> String {
        // Show just the host portion
        if let Some(rest) = self.base_url.strip_prefix("https://") {
            let host = rest.split('/').next().unwrap_or(rest);
            format!("WebDAV: {}", host)
        } else if let Some(rest) = self.base_url.strip_prefix("http://") {
            let host = rest.split('/').next().unwrap_or(rest);
            format!("WebDAV: {}", host)
        } else {
            format!("WebDAV: {}", self.base_url)
        }
    }

    /// Build the full URL for a path.
    fn url_for(&self, path: &Path) -> String {
        let path_str = path.to_string_lossy();
        if path_str == "/" || path_str.is_empty() {
            format!("{}/", self.base_url)
        } else {
            let clean = path_str.trim_start_matches('/');
            format!("{}/{}", self.base_url, url_encode_path(clean))
        }
    }

    /// Run a curl command and return (status_code, stdout).
    fn curl(&self, method: &str, url: &str, extra_args: &[&str]) -> Result<(u16, String)> {
        crate::debug_log::log(&format!("WebDAV {} {}", method, url));
        let mut cmd = Command::new("curl");
        cmd.arg("-s") // silent
            .arg("-w").arg("\n%{http_code}") // append status code
            .arg("-X").arg(method)
            .arg("--connect-timeout").arg("15")
            .arg("--max-time").arg("300") // 5 min max for large transfers
            // Read credentials from stdin config to avoid leaking in process table
            .arg("-K").arg("-");

        if self.insecure {
            cmd.arg("-k"); // only disable TLS verification when explicitly opted-in
        }

        for arg in extra_args {
            cmd.arg(arg);
        }
        cmd.arg(url);

        // Pass credentials via stdin using curl's config format so they
        // never appear in the process argument list visible via `ps`.
        let config = format!("user = \"{}:{}\"", self.username, self.password);
        cmd.stdin(Stdio::piped());

        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to run curl. Is it installed?")?;

        if let Some(mut stdin) = child.stdin.take() {
            let _ = stdin.write_all(config.as_bytes());
            // stdin is dropped here, closing the pipe so curl proceeds
        }

        let output = child
            .wait_with_output()
            .context("Failed to read curl output")?;

        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();

        // Last line is the status code
        let (body, status_str) = match stdout.rsplit_once('\n') {
            Some((b, s)) => (b.to_string(), s),
            None => (String::new(), stdout.as_str()),
        };
        let status: u16 = status_str.trim().parse().map_err(|_| {
            anyhow::anyhow!(
                "Failed to parse HTTP status from curl output: {:?}",
                status_str.trim()
            )
        })?;

        Ok((status, body))
    }

    /// Send a PROPFIND request and return the XML body.
    fn propfind(&self, path: &str, depth: u8) -> Result<String> {
        let url = if path == "/" || path.is_empty() {
            format!("{}/", self.base_url)
        } else {
            let clean = path.trim_start_matches('/');
            format!("{}/{}", self.base_url, url_encode_path(clean))
        };

        let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:displayname/>
    <d:getcontentlength/>
    <d:getlastmodified/>
    <d:resourcetype/>
  </d:prop>
</d:propfind>"#;

        let depth_str = depth.to_string();
        let (status, response) = self.curl(
            "PROPFIND",
            &url,
            &[
                "-H", &format!("Depth: {}", depth_str),
                "-H", "Content-Type: application/xml",
                "-d", body,
            ],
        )?;

        if status == 207 || status == 200 {
            Ok(response)
        } else if status == 401 {
            anyhow::bail!("Authentication failed (401)")
        } else if status == 404 {
            anyhow::bail!("Not found (404): {}", path)
        } else {
            anyhow::bail!("WebDAV PROPFIND failed with status {}", status)
        }
    }

    /// List directory contents.
    pub fn read_dir(&self, path: &Path) -> Result<Vec<FileEntry>> {
        let path_str = path.to_string_lossy();
        let xml = self.propfind(&path_str, 1)?;
        parse_propfind_response(&xml, path, &self.base_url)
    }

    /// Create a remote directory.
    pub fn mkdir(&self, path: &Path) -> Result<()> {
        let url = self.url_for(path);
        let (status, _) = self.curl("MKCOL", &url, &[])?;
        if (status >= 200 && status < 300) || status == 405 {
            // 405 = already exists, that's ok
            Ok(())
        } else {
            anyhow::bail!("MKCOL failed with status {}", status)
        }
    }

    /// Remove a remote file or directory (WebDAV DELETE is recursive for collections).
    pub fn remove_recursive(&self, path: &Path) -> Result<()> {
        let url = self.url_for(path);
        let (status, _) = self.curl("DELETE", &url, &[])?;
        if (status >= 200 && status < 300) || status == 404 {
            Ok(())
        } else {
            anyhow::bail!("DELETE failed with status {}", status)
        }
    }

    /// Rename / move a remote resource.
    pub fn rename(&self, src: &Path, dst: &Path) -> Result<()> {
        let src_url = self.url_for(src);
        let dst_url = self.url_for(dst);
        let (status, _) = self.curl(
            "MOVE",
            &src_url,
            &["-H", &format!("Destination: {}", dst_url)],
        )?;
        if status >= 200 && status < 300 {
            Ok(())
        } else {
            anyhow::bail!("MOVE failed with status {}", status)
        }
    }

    /// Download a remote file to a local path.
    pub fn download(&self, remote: &Path, local: &Path) -> Result<u64> {
        let url = self.url_for(remote);
        let local_str = local.to_string_lossy();
        let (status, _) = self.curl("GET", &url, &["-o", &local_str])?;
        if status >= 200 && status < 300 {
            let meta = std::fs::metadata(local)?;
            Ok(meta.len())
        } else {
            anyhow::bail!("GET failed with status {}", status)
        }
    }

    /// Upload a local file to a remote path.
    pub fn upload(&self, local: &Path, remote: &Path) -> Result<u64> {
        let url = self.url_for(remote);
        let local_str = local.to_string_lossy();
        let (status, _) = self.curl("PUT", &url, &["-T", &local_str])?;
        if status >= 200 && status < 300 {
            let meta = std::fs::metadata(local)?;
            Ok(meta.len())
        } else {
            anyhow::bail!("PUT failed with status {}", status)
        }
    }

    /// Download a directory recursively.
    pub fn download_dir(&self, remote: &Path, local: &Path) -> Result<u64> {
        std::fs::create_dir_all(local)?;
        let entries = self.read_dir(remote)?;
        let mut total = 0u64;
        for entry in entries {
            if entry.name == ".." {
                continue;
            }
            let local_dest = local.join(&entry.name);
            if entry.is_dir {
                total += self.download_dir(&entry.path, &local_dest)?;
            } else {
                total += self.download(&entry.path, &local_dest)?;
            }
        }
        Ok(total)
    }

    /// Upload a directory recursively.
    pub fn upload_dir(&self, local: &Path, remote: &Path) -> Result<u64> {
        let _ = self.mkdir(remote);
        let mut total = 0u64;
        for entry in std::fs::read_dir(local)? {
            let entry = entry?;
            let remote_dest = remote.join(entry.file_name());
            if entry.file_type()?.is_dir() {
                total += self.upload_dir(&entry.path(), &remote_dest)?;
            } else {
                total += self.upload(&entry.path(), &remote_dest)?;
            }
        }
        Ok(total)
    }

    /// Home directory (root).
    pub fn home_dir(&self) -> PathBuf {
        PathBuf::from("/")
    }
}

impl crate::remote_fs::RemoteFs for WebDavConnection {
    fn read_dir(&self, path: &Path) -> Result<Vec<FileEntry>> { self.read_dir(path) }
    fn mkdir(&self, path: &Path) -> Result<()> { self.mkdir(path) }
    fn remove_recursive(&self, path: &Path) -> Result<()> { self.remove_recursive(path) }
    fn rename(&self, src: &Path, dst: &Path) -> Result<()> { self.rename(src, dst) }
    fn download(&self, remote: &Path, local: &Path) -> Result<u64> { self.download(remote, local) }
    fn upload(&self, local: &Path, remote: &Path) -> Result<u64> { self.upload(local, remote) }
    fn download_dir(&self, remote: &Path, local: &Path) -> Result<u64> { self.download_dir(remote, local) }
    fn upload_dir(&self, local: &Path, remote: &Path) -> Result<u64> { self.upload_dir(local, remote) }
    fn home_dir(&self) -> PathBuf { self.home_dir() }
    fn display_label(&self) -> String { self.display_label() }
}

/// Minimal URL encoding for path segments (spaces, special chars).
fn url_encode_path(path: &str) -> String {
    path.split('/')
        .map(|segment| {
            segment
                .replace('%', "%25")
                .replace(' ', "%20")
                .replace('#', "%23")
                .replace('?', "%3F")
                .replace('[', "%5B")
                .replace(']', "%5D")
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Parse a WebDAV PROPFIND 207 Multi-Status XML response into FileEntry items.
fn parse_propfind_response(
    xml: &str,
    parent: &Path,
    base_url: &str,
) -> Result<Vec<FileEntry>> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    const MAX_ENTRIES: usize = 10_000;

    // Build the expected URL path for the directory itself (to skip it in the listing).
    // base_url is like "https://host/dav/files/user", parent is the virtual path like "/docs"
    // The self-entry href will be like "/dav/files/user/docs/"
    let parent_str = parent.to_string_lossy();
    let parent_suffix = parent_str.trim_start_matches('/');
    let self_url = if parent_suffix.is_empty() {
        // Root listing — strip scheme+host from base_url to get the path
        base_url
            .find("://")
            .and_then(|i| base_url[i + 3..].find('/').map(|j| &base_url[i + 3 + j..]))
            .unwrap_or("/")
            .trim_end_matches('/')
            .to_string()
    } else {
        let base_path = base_url
            .find("://")
            .and_then(|i| base_url[i + 3..].find('/').map(|j| &base_url[i + 3 + j..]))
            .unwrap_or("");
        format!("{}/{}", base_path.trim_end_matches('/'), parent_suffix)
    };

    let mut reader = Reader::from_str(xml);
    let mut entries = Vec::new();

    // State machine for parsing
    let mut in_response = false;
    let mut in_prop = false;
    let mut current_href: Option<String> = None;
    let mut current_name: Option<String> = None;
    let mut current_size: u64 = 0;
    let mut current_modified: Option<String> = None;
    let mut current_is_dir = false;
    let mut current_tag = String::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                let name_bytes = e.name();
                let name_str = std::str::from_utf8(name_bytes.as_ref()).unwrap_or("");
                let local = local_name(name_str);
                match local {
                    "response" => {
                        in_response = true;
                        current_href = None;
                        current_name = None;
                        current_size = 0;
                        current_modified = None;
                        current_is_dir = false;
                    }
                    "prop" => in_prop = true,
                    "collection" if in_prop => current_is_dir = true,
                    _ => {}
                }
                current_tag = local.to_string();
            }
            Ok(Event::Text(e)) => {
                if !in_response {
                    continue;
                }
                // Only allocate the text string for tags we care about
                match current_tag.as_str() {
                    "href" | "displayname" | "getcontentlength" | "getlastmodified" => {}
                    _ => continue,
                }
                let text = e.unescape().unwrap_or_default();
                match current_tag.as_str() {
                    "href" => current_href = Some(text.into_owned()),
                    "displayname" if in_prop => current_name = Some(text.into_owned()),
                    "getcontentlength" if in_prop => {
                        current_size = text.trim().parse().unwrap_or(0);
                    }
                    "getlastmodified" if in_prop => current_modified = Some(text.into_owned()),
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                let name_bytes = e.name();
                let name_str = std::str::from_utf8(name_bytes.as_ref()).unwrap_or("");
                let local = local_name(name_str);
                if local == "response" {
                    in_response = false;

                    // Build FileEntry from collected data
                    let name = current_name
                        .take()
                        .or_else(|| {
                            // Extract name from href
                            current_href.as_ref().and_then(|href| {
                                let clean = href.trim_end_matches('/');
                                clean.rsplit('/').next().map(|s| {
                                    percent_decode(s)
                                })
                            })
                        })
                        .unwrap_or_default();

                    if name.is_empty() {
                        continue;
                    }

                    // Skip the directory's own entry by comparing href to the
                    // expected URL path for the directory we listed.
                    if let Some(ref href) = current_href {
                        let href_trimmed = href.trim_end_matches('/');
                        if href_trimmed == self_url {
                            continue;
                        }
                    }

                    let path = parent.join(&name);
                    let modified = current_modified
                        .as_deref()
                        .and_then(parse_http_date)
                        .unwrap_or(UNIX_EPOCH);

                    if entries.len() < MAX_ENTRIES {
                        entries.push(FileEntry {
                            name,
                            path,
                            is_dir: current_is_dir,
                            is_symlink: false,
                            size: current_size,
                            modified,
                            permissions: if current_is_dir { 0o755 } else { 0o644 },
                        });
                    }
                } else if local == "prop" {
                    in_prop = false;
                }
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                anyhow::bail!("XML parse error: {}", e);
            }
            _ => {}
        }
        buf.clear();
    }

    Ok(entries)
}

/// Strip namespace prefix: "D:href" → "href", "href" → "href"
fn local_name(tag: &str) -> &str {
    tag.rsplit_once(':').map(|(_, local)| local).unwrap_or(tag)
}

/// Decode percent-encoded URL segments (UTF-8 aware).
fn percent_decode(s: &str) -> String {
    let mut bytes = Vec::with_capacity(s.len());
    let mut chars = s.as_bytes().iter();
    while let Some(&b) = chars.next() {
        if b == b'%' {
            match (chars.next().copied(), chars.next().copied()) {
                (Some(h1), Some(h2)) => {
                    let hex = [h1, h2];
                    if let Ok(byte) =
                        u8::from_str_radix(std::str::from_utf8(&hex).unwrap_or(""), 16)
                    {
                        bytes.push(byte);
                    } else {
                        bytes.push(b'%');
                        bytes.push(h1);
                        bytes.push(h2);
                    }
                }
                (Some(h1), None) => {
                    // Truncated: %X at end of string
                    bytes.push(b'%');
                    bytes.push(h1);
                }
                _ => {
                    // Truncated: % at end of string
                    bytes.push(b'%');
                }
            }
        } else {
            // Note: '+' is NOT converted to space in RFC 3986 percent-encoding
            // (that's only for application/x-www-form-urlencoded)
            bytes.push(b);
        }
    }
    String::from_utf8(bytes).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

/// Parse HTTP date formats: RFC 2822 / RFC 1123 / RFC 850.
fn parse_http_date(s: &str) -> Option<SystemTime> {
    use chrono::DateTime;
    // Try RFC 2822 format: "Tue, 15 Nov 2025 08:12:31 GMT"
    if let Ok(dt) = DateTime::parse_from_rfc2822(s.trim()) {
        let ts = dt.timestamp();
        if ts >= 0 {
            return Some(UNIX_EPOCH + Duration::from_secs(ts as u64));
        }
    }
    // Try RFC 1123: "Tue, 15 Nov 2025 08:12:31 GMT"
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s.trim(), "%a, %d %b %Y %H:%M:%S GMT") {
        let ts = dt.and_utc().timestamp();
        if ts >= 0 {
            return Some(UNIX_EPOCH + Duration::from_secs(ts as u64));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_propfind_basic() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<d:multistatus xmlns:d="DAV:">
  <d:response>
    <d:href>/remote.php/dav/files/user/</d:href>
    <d:propstat>
      <d:prop>
        <d:displayname>user</d:displayname>
        <d:resourcetype><d:collection/></d:resourcetype>
      </d:prop>
    </d:propstat>
  </d:response>
  <d:response>
    <d:href>/remote.php/dav/files/user/Documents/</d:href>
    <d:propstat>
      <d:prop>
        <d:displayname>Documents</d:displayname>
        <d:resourcetype><d:collection/></d:resourcetype>
        <d:getcontentlength>0</d:getcontentlength>
        <d:getlastmodified>Tue, 15 Nov 2025 08:12:31 GMT</d:getlastmodified>
      </d:prop>
    </d:propstat>
  </d:response>
  <d:response>
    <d:href>/remote.php/dav/files/user/photo.jpg</d:href>
    <d:propstat>
      <d:prop>
        <d:displayname>photo.jpg</d:displayname>
        <d:resourcetype/>
        <d:getcontentlength>123456</d:getcontentlength>
        <d:getlastmodified>Wed, 10 Dec 2025 14:30:00 GMT</d:getlastmodified>
      </d:prop>
    </d:propstat>
  </d:response>
</d:multistatus>"#;

        let entries =
            parse_propfind_response(xml, Path::new("/"), "https://cloud.example.com/remote.php/dav/files/user")
                .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "Documents");
        assert!(entries[0].is_dir);
        assert_eq!(entries[1].name, "photo.jpg");
        assert!(!entries[1].is_dir);
        assert_eq!(entries[1].size, 123456);
    }

    #[test]
    fn local_name_strips_prefix() {
        assert_eq!(local_name("D:href"), "href");
        assert_eq!(local_name("href"), "href");
        assert_eq!(local_name("d:collection"), "collection");
    }

    #[test]
    fn percent_decode_works() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("normal"), "normal");
        assert_eq!(percent_decode("foo%2Fbar"), "foo/bar");
    }

    #[test]
    fn url_encode_path_works() {
        assert_eq!(url_encode_path("path/to/file"), "path/to/file");
        assert_eq!(url_encode_path("my docs/file name"), "my%20docs/file%20name");
    }

    #[test]
    fn percent_decode_plus_sign_preserved() {
        // '+' should NOT be converted to space in URI percent-encoding
        assert_eq!(percent_decode("my+file.txt"), "my+file.txt");
    }

    #[test]
    fn percent_decode_utf8() {
        // %C3%A9 = é in UTF-8
        assert_eq!(percent_decode("caf%C3%A9"), "café");
    }

    #[test]
    fn percent_decode_incomplete_sequence() {
        // Incomplete % at end
        assert_eq!(percent_decode("test%2"), "test%2");
    }

    #[test]
    fn parse_propfind_no_namespace_prefix() {
        // Some servers don't use namespace prefixes
        let xml = r#"<?xml version="1.0"?>
<multistatus xmlns="DAV:">
  <response>
    <href>/root/</href>
    <propstat><prop><resourcetype><collection/></resourcetype></prop></propstat>
  </response>
  <response>
    <href>/root/file.txt</href>
    <propstat>
      <prop>
        <displayname>file.txt</displayname>
        <resourcetype/>
        <getcontentlength>42</getcontentlength>
      </prop>
    </propstat>
  </response>
</multistatus>"#;
        let entries = parse_propfind_response(xml, Path::new("/"), "https://host/root").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "file.txt");
        assert_eq!(entries[0].size, 42);
    }

    #[test]
    fn parse_propfind_empty_directory() {
        let xml = r#"<?xml version="1.0"?>
<d:multistatus xmlns:d="DAV:">
  <d:response>
    <d:href>/empty/</d:href>
    <d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat>
  </d:response>
</d:multistatus>"#;
        let entries = parse_propfind_response(xml, Path::new("/"), "https://host/empty").unwrap();
        assert!(entries.is_empty()); // Directory itself is skipped
    }

    #[test]
    fn parse_propfind_name_from_href() {
        // When displayname is missing, name should be extracted from href
        let xml = r#"<?xml version="1.0"?>
<d:multistatus xmlns:d="DAV:">
  <d:response>
    <d:href>/dir/</d:href>
    <d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat>
  </d:response>
  <d:response>
    <d:href>/dir/my%20file.txt</d:href>
    <d:propstat>
      <d:prop>
        <d:resourcetype/>
        <d:getcontentlength>100</d:getcontentlength>
      </d:prop>
    </d:propstat>
  </d:response>
</d:multistatus>"#;
        let entries = parse_propfind_response(xml, Path::new("/"), "https://host/dir").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "my file.txt"); // percent-decoded
    }

    #[test]
    fn webdav_display_label() {
        let conn = WebDavConnection {
            base_url: "https://cloud.example.com/dav".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            insecure: false,
        };
        assert_eq!(conn.display_label(), "WebDAV: cloud.example.com");
    }

    #[test]
    fn webdav_display_label_http() {
        let conn = WebDavConnection {
            base_url: "http://nas.local:8080/webdav".to_string(),
            username: String::new(),
            password: String::new(),
            insecure: false,
        };
        assert_eq!(conn.display_label(), "WebDAV: nas.local:8080");
    }
}
