//! Azure DevOps OAuth2 authentication.
//!
//! Two authentication methods:
//! 1. **PAT** — user pastes a Personal Access Token, stored in keychain
//! 2. **Browser login** — OAuth2 authorization code flow with localhost redirect

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Command, Stdio};

/// Token obtained from the OAuth2 flow.
#[derive(Debug, Clone)]
pub struct TokenResponse {
    pub access_token: String,
    pub refresh_token: Option<String>,
}

/// Result of the browser auth flow (sent from background thread).
#[derive(Debug)]
pub enum AuthResult {
    Success(TokenResponse),
    Error(String),
}

/// Azure DevOps resource ID.
const ADO_RESOURCE: &str = "499b84ac-1321-427f-aa17-267ca6975798";
/// Azure CLI well-known public client ID (fallback).
const DEFAULT_CLIENT_ID: &str = "04b07795-a71b-4346-935c-02f183830150";

/// Get the stored tenant from the keychain.
pub fn get_stored_tenant() -> Option<String> {
    keychain_lookup("azure-tenant")
}

/// Store the tenant in the keychain.
pub fn store_tenant(tenant: &str) -> Result<(), String> {
    keychain_store("azure-tenant", "middle-manager azure tenant", tenant)
}

/// Check if the `az` CLI is installed and reachable.
pub fn az_cli_available() -> bool {
    Command::new("az")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Get currently-logged-in az CLI account info: (account_name, tenant_id).
/// Returns None if `az` is not installed or not logged in.
pub fn az_cli_account() -> Option<(String, String)> {
    let output = Command::new("az")
        .args([
            "account",
            "show",
            "--query",
            "{name:user.name,tenant:tenantId}",
            "-o",
            "tsv",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let line = text.lines().next()?.trim();
    let mut parts = line.split('\t');
    let name = parts.next()?.to_string();
    let tenant = parts.next()?.to_string();
    if name.is_empty() || tenant.is_empty() {
        return None;
    }
    Some((name, tenant))
}

/// Fetch an Azure DevOps access token via the `az` CLI.
/// Requires `az login` to have been run already.
pub fn get_token_via_az_cli() -> Result<TokenResponse, String> {
    let output = Command::new("az")
        .args([
            "account",
            "get-access-token",
            "--resource",
            ADO_RESOURCE,
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| format!("Failed to run az: {}", e))?;

    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        let msg = err.lines().next().unwrap_or("az command failed").trim();
        return Err(if msg.is_empty() {
            "az account get-access-token failed — is `az login` done?".to_string()
        } else {
            msg.to_string()
        });
    }

    let token = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if token.is_empty() {
        return Err("az returned empty token".to_string());
    }
    Ok(TokenResponse {
        access_token: token,
        refresh_token: None,
    })
}

fn get_client_id() -> String {
    std::env::var("AZURE_DEVOPS_CLIENT_ID")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_CLIENT_ID.to_string())
}

fn authorize_url(tenant: &str) -> String {
    format!(
        "https://login.microsoftonline.com/{}/oauth2/authorize",
        tenant
    )
}

fn token_url(tenant: &str) -> String {
    format!("https://login.microsoftonline.com/{}/oauth2/token", tenant)
}

/// Braille spinner frames, shared with `ci` for consistent animation.
pub(crate) const SPINNER: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

/// Success page shown in the browser after authentication completes.
const SUCCESS_HTML: &str = r#"<html><body style="font-family:sans-serif;text-align:center;padding:60px">
<h2>Authentication successful</h2><p>You can close this tab and return to Middle Manager.</p>
</body></html>"#;

/// Error page shown in the browser when authentication fails.
const ERROR_HTML: &str = r#"<html><body style="font-family:sans-serif;text-align:center;padding:60px">
<h2>Authentication failed</h2><p>Check Middle Manager for details.</p>
</body></html>"#;

/// Exchange an authorization code for tokens via the token endpoint.
fn exchange_code(
    code: &str,
    redirect_uri: &str,
    code_verifier: &str,
    tenant: &str,
) -> Result<TokenResponse, String> {
    let client_id = get_client_id();
    let body = format!(
        "client_id={}&grant_type=authorization_code&code={}&redirect_uri={}&resource={}&code_verifier={}",
        client_id,
        urlencoded(code),
        urlencoded(redirect_uri),
        ADO_RESOURCE,
        urlencoded(code_verifier),
    );

    let url = token_url(tenant);
    let output = Command::new("curl")
        .args([
            "-s",
            "-X",
            "POST",
            &url,
            "-H",
            "Content-Type: application/x-www-form-urlencoded",
            "-d",
            &body,
            "--max-time",
            "30",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .map_err(|e| format!("Failed to exchange code: {}", e))?;

    if !output.status.success() {
        return Err("Token exchange request failed".to_string());
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| format!("Invalid token response: {}", e))?;

    if let Some(err) = json.get("error").and_then(|v| v.as_str()) {
        let desc = json
            .get("error_description")
            .and_then(|v| v.as_str())
            .unwrap_or(err);
        return Err(desc.to_string());
    }

    let access_token = json
        .get("access_token")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let refresh_token = json
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    if access_token.is_empty() {
        return Err("No access token in response".to_string());
    }

    Ok(TokenResponse {
        access_token,
        refresh_token,
    })
}

/// Extract a single query-string parameter from an HTTP GET request line.
/// e.g. ("GET /?code=abc&state=xyz HTTP/1.1", "code") -> Some("abc")
fn query_param(request_line: &str, name: &str) -> Option<String> {
    let path = request_line.split_whitespace().nth(1)?;
    let query = path.split('?').nth(1)?;
    let prefix = format!("{}=", name);
    for param in query.split('&') {
        if let Some(value) = param.strip_prefix(prefix.as_str()) {
            return Some(urldecoded(value));
        }
    }
    None
}

fn extract_code_from_request(request_line: &str) -> Option<String> {
    query_param(request_line, "code")
}

fn extract_state_from_request(request_line: &str) -> Option<String> {
    query_param(request_line, "state")
}

/// Prefers `error_description` (human-readable) over `error` (code).
fn extract_error_from_request(request_line: &str) -> Option<String> {
    query_param(request_line, "error_description").or_else(|| query_param(request_line, "error"))
}

/// Async browser auth flow state — runs localhost server in a background thread.
pub struct BrowserAuthFlow {
    pub status: String,
    rx: std::sync::mpsc::Receiver<AuthResult>,
    spinner_tick: usize,
}

impl BrowserAuthFlow {
    /// Start the browser auth flow: bind localhost, open browser, wait for redirect.
    /// `tenant` is the Azure AD tenant (e.g. "mycompany.com" or a GUID).
    /// Optionally uses AZURE_DEVOPS_CLIENT_ID env var for client ID.
    pub fn start(tenant: &str) -> Result<Self, String> {
        if tenant.trim().is_empty() {
            return Err("Tenant is required for browser login".to_string());
        }
        let tenant = tenant.trim().to_string();
        let client_id = get_client_id();

        // Bind to a random available port
        let listener =
            TcpListener::bind("127.0.0.1:0").map_err(|e| format!("Failed to bind port: {}", e))?;
        let port = listener
            .local_addr()
            .map_err(|e| format!("Failed to get port: {}", e))?
            .port();

        // Generate PKCE code verifier/challenge and a CSRF `state` nonce.
        let code_verifier = generate_code_verifier();
        let code_challenge =
            compute_code_challenge(&code_verifier).map_err(|e| format!("PKCE error: {}", e))?;
        let state = generate_state();

        let redirect_uri = format!("http://localhost:{}", port);
        let auth_url = format!(
            "{}?client_id={}&response_type=code&redirect_uri={}&resource={}&code_challenge={}&code_challenge_method=S256&state={}",
            authorize_url(&tenant),
            client_id,
            urlencoded(&redirect_uri),
            ADO_RESOURCE,
            urlencoded(&code_challenge),
            urlencoded(&state),
        );

        // Open browser
        crate::panel::github::open_url(&auth_url);

        let (tx, rx) = std::sync::mpsc::channel();

        // Poll for an incoming connection with a 2-minute wall-clock timeout.
        // TcpListener has no native accept timeout, so we use non-blocking + sleep.
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("Failed to set non-blocking: {}", e))?;

        std::thread::spawn(move || {
            let timeout = std::time::Duration::from_secs(120);
            let start = std::time::Instant::now();
            let accepted = loop {
                match listener.accept() {
                    Ok(conn) => break Ok(conn),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        if start.elapsed() > timeout {
                            break Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "Timed out waiting for browser login (2 min)",
                            ));
                        }
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                    Err(e) => break Err(e),
                }
            };
            let result = match accepted {
                Ok((stream, _)) => {
                    handle_redirect(stream, &state, &redirect_uri, &code_verifier, &tenant)
                }
                Err(e) => AuthResult::Error(format!("Failed to accept connection: {}", e)),
            };
            let _ = tx.send(result);
        });

        Ok(Self {
            status: "Waiting for browser login...".to_string(),
            rx,
            spinner_tick: 0,
        })
    }

    /// Poll for completion. Returns Some on terminal state.
    pub fn poll(&mut self) -> Option<AuthResult> {
        self.spinner_tick = self.spinner_tick.wrapping_add(1);
        let c = SPINNER[self.spinner_tick % SPINNER.len()];
        self.status = format!("{} Waiting for browser login...", c);
        self.rx.try_recv().ok()
    }
}

/// Handle a redirect connection: read the request line, write the browser
/// response page, and return the derived `AuthResult`. Runs in the spawned
/// thread; errors must always produce an `AuthResult` so the mpsc send never
/// goes missing (otherwise the dialog would hang on "Waiting...").
fn handle_redirect(
    mut stream: std::net::TcpStream,
    expected_state: &str,
    redirect_uri: &str,
    code_verifier: &str,
    tenant: &str,
) -> AuthResult {
    // `BufReader` needs its own handle so the response write below still
    // uses the original stream. If cloning fails, we can't parse the request
    // but we can still produce a proper AuthResult::Error.
    let cloned = match stream.try_clone() {
        Ok(c) => c,
        Err(e) => {
            return AuthResult::Error(format!("Failed to clone socket: {}", e));
        }
    };
    let mut reader = BufReader::new(cloned);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        return AuthResult::Error("Failed to read browser redirect".to_string());
    }

    let send_page = |stream: &mut std::net::TcpStream, body: &str| {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nConnection: close\r\n\r\n{}",
            body
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    };

    if let Some(code) = extract_code_from_request(&request_line) {
        // Validate CSRF state before trusting the authorization code.
        let received = extract_state_from_request(&request_line).unwrap_or_default();
        if received != expected_state {
            send_page(&mut stream, ERROR_HTML);
            return AuthResult::Error(
                "OAuth state mismatch — possible CSRF, ignoring redirect".to_string(),
            );
        }
        // Flush the success page first so the user sees a fast response.
        send_page(&mut stream, SUCCESS_HTML);
        drop(stream);
        match exchange_code(&code, redirect_uri, code_verifier, tenant) {
            Ok(token) => AuthResult::Success(token),
            Err(e) => AuthResult::Error(e),
        }
    } else if let Some(err) = extract_error_from_request(&request_line) {
        send_page(&mut stream, ERROR_HTML);
        AuthResult::Error(err)
    } else {
        send_page(&mut stream, ERROR_HTML);
        AuthResult::Error("No authorization code in redirect".to_string())
    }
}

// ---------------------------------------------------------------------------
// Token storage (keychain / keyring)
// ---------------------------------------------------------------------------

/// Store a Bearer token for Azure DevOps API calls.
pub fn store_bearer_token(token: &str) -> Result<(), String> {
    keychain_store("azure-bearer", "middle-manager azure bearer", token)
}

/// Get stored Bearer token.
pub fn get_bearer_token() -> Option<String> {
    keychain_lookup("azure-bearer")
}

/// Store refresh token.
pub fn store_refresh_token(token: &str) -> Result<(), String> {
    keychain_store("azure-refresh", "middle-manager azure refresh", token)
}

fn keychain_store(account: &str, label: &str, value: &str) -> Result<(), String> {
    if cfg!(target_os = "macos") {
        // macOS `security add-generic-password` takes the password as argv (-w),
        // which is briefly visible to other local users via `ps`. The alternative
        // (interactive prompt) is unusable from a TUI. We use `-U` to upsert in a
        // single command, keeping the exposure window as short as possible.
        let status = Command::new("security")
            .args([
                "add-generic-password",
                "-U",
                "-s",
                "middle-manager",
                "-a",
                account,
                "-l",
                label,
                "-w",
                value,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|e| format!("Failed to run security: {}", e))?;
        if status.success() {
            Ok(())
        } else {
            Err("Failed to store in macOS Keychain".to_string())
        }
    } else {
        let mut child = Command::new("secret-tool")
            .args([
                "store",
                "--label",
                label,
                "service",
                "middle-manager",
                "account",
                account,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("Failed to run secret-tool: {}", e))?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(value.as_bytes())
                .map_err(|e| format!("Failed to write: {}", e))?;
        }
        let status = child
            .wait()
            .map_err(|e| format!("secret-tool failed: {}", e))?;
        if status.success() {
            Ok(())
        } else {
            Err("Failed to store in keyring".to_string())
        }
    }
}

fn keychain_lookup(account: &str) -> Option<String> {
    if cfg!(target_os = "macos") {
        let output = Command::new("security")
            .args([
                "find-generic-password",
                "-s",
                "middle-manager",
                "-a",
                account,
                "-w",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .ok()?;
        if output.status.success() {
            let val = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !val.is_empty() {
                return Some(val);
            }
        }
    } else {
        let output = Command::new("secret-tool")
            .args(["lookup", "service", "middle-manager", "account", account])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .ok()?;
        if output.status.success() {
            let val = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !val.is_empty() {
                return Some(val);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// PKCE (Proof Key for Code Exchange)
// ---------------------------------------------------------------------------

/// Fill `out` with random bytes.
///
/// Primary source is `/dev/urandom` (present on both macOS and Linux, which are
/// our only targets). Falls back to a time+pid+stack-address seeded xorshift
/// only if the OS source is unreachable — logged so unexpected fallbacks are
/// visible. The fallback is NOT a CSPRNG and only exists to keep the flow
/// functional in degraded environments (e.g. chroot without /dev).
fn fill_random(out: &mut [u8]) {
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        if f.read_exact(out).is_ok() {
            return;
        }
    }
    crate::debug_log::log("azure_auth: /dev/urandom unavailable, falling back to xorshift");
    use std::time::{SystemTime, UNIX_EPOCH};
    let time_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = std::process::id() as u128;
    let ptr = out.as_ptr() as u128;
    let mut state: u64 = (time_ns ^ pid ^ ptr) as u64;
    for b in out.iter_mut() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *b = state as u8;
    }
}

/// Generate a PKCE code verifier (RFC 7636 §4.1): 43–128 URL-safe chars.
fn generate_code_verifier() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
    let mut bytes = [0u8; 64];
    fill_random(&mut bytes);
    let mut verifier = String::with_capacity(64);
    for b in bytes.iter() {
        verifier.push(CHARSET[(*b as usize) % CHARSET.len()] as char);
    }
    verifier
}

/// Generate an OAuth2 `state` parameter (RFC 6749 §10.12) — a 32-byte
/// base64url-encoded random nonce for CSRF protection on the redirect.
fn generate_state() -> String {
    let mut bytes = [0u8; 32];
    fill_random(&mut bytes);
    base64url_encode(&bytes)
}

/// Compute S256 code challenge: base64url(sha256(code_verifier)).
fn compute_code_challenge(verifier: &str) -> Result<String, String> {
    // Use openssl to compute SHA-256
    let output = Command::new("openssl")
        .args(["dgst", "-sha256", "-binary"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .and_then(|mut child| {
            if let Some(mut stdin) = child.stdin.take() {
                use std::io::Write;
                stdin.write_all(verifier.as_bytes())?;
            }
            child.wait_with_output()
        })
        .map_err(|e| format!("openssl failed: {}", e))?;

    if !output.status.success() || output.stdout.is_empty() {
        return Err("SHA-256 computation failed".to_string());
    }

    // Base64url encode (no padding)
    Ok(base64url_encode(&output.stdout))
}

/// Base64url encoding without padding (RFC 4648 §5).
fn base64url_encode(data: &[u8]) -> String {
    let b64 = crate::clipboard::base64_encode(data);
    b64.replace('+', "-")
        .replace('/', "_")
        .trim_end_matches('=')
        .to_string()
}

// ---------------------------------------------------------------------------
// URL encoding / decoding
// ---------------------------------------------------------------------------

/// Simple percent-encoding for URL form data.
fn urlencoded(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => {
                out.push('%');
                out.push(char::from(b"0123456789ABCDEF"[(b >> 4) as usize]));
                out.push(char::from(b"0123456789ABCDEF"[(b & 0x0F) as usize]));
            }
        }
    }
    out
}

/// Simple percent-decoding.
fn urldecoded(s: &str) -> String {
    let mut out = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push(hi << 4 | lo);
                i += 3;
                continue;
            }
        }
        if bytes[i] == b'+' {
            out.push(b' ');
        } else {
            out.push(bytes[i]);
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urlencoded_basic() {
        assert_eq!(urlencoded("hello world"), "hello%20world");
        assert_eq!(urlencoded("a/b"), "a%2Fb");
        assert_eq!(urlencoded("foo@bar.com"), "foo%40bar.com");
    }

    #[test]
    fn urlencoded_passthrough() {
        assert_eq!(urlencoded("abc-123_xyz.~"), "abc-123_xyz.~");
    }

    #[test]
    fn urldecoded_basic() {
        assert_eq!(urldecoded("hello%20world"), "hello world");
        assert_eq!(urldecoded("a%2Fb"), "a/b");
        assert_eq!(urldecoded("foo+bar"), "foo bar");
    }

    #[test]
    fn extract_code_works() {
        let line = "GET /?code=abc123&state=xyz HTTP/1.1";
        assert_eq!(extract_code_from_request(line), Some("abc123".to_string()));
    }

    #[test]
    fn extract_code_missing() {
        let line = "GET /?error=access_denied HTTP/1.1";
        assert_eq!(extract_code_from_request(line), None);
    }

    #[test]
    fn extract_error_works() {
        let line = "GET /?error=access_denied&error_description=User%20cancelled HTTP/1.1";
        assert_eq!(
            extract_error_from_request(line),
            Some("User cancelled".to_string())
        );
    }

    #[test]
    fn code_verifier_is_valid() {
        let v = generate_code_verifier();
        assert_eq!(v.len(), 64);
        assert!(v
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-._~".contains(c)));
        // Two calls produce different values
        let v2 = generate_code_verifier();
        assert_ne!(v, v2);
    }

    #[test]
    fn code_challenge_is_base64url() {
        let v = generate_code_verifier();
        let c = compute_code_challenge(&v).unwrap();
        // SHA-256 = 32 bytes -> base64 = 43 chars (no padding)
        assert_eq!(c.len(), 43);
        assert!(!c.contains('+'));
        assert!(!c.contains('/'));
        assert!(!c.contains('='));
    }
}
