use std::collections::HashSet;
use std::ffi::OsString;
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail};
use bytes::Bytes;
use if_addrs::IfAddr;
use prost_types::Timestamp;
use proto::hackarena::broker::v1::broker_service_client::BrokerServiceClient;
use proto::hackarena::broker::v1::{HeartbeatRequest, RegisterBackendRequest};
use rand::RngCore;
use rand::rngs::OsRng;
use tokio::process::Command;
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;
use tonic::codegen::http::Uri;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Request, Status};

use crate::config::Config;

const BROKER_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const BROKER_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const HEARTBEAT_DEFAULT_INTERVAL: Duration = Duration::from_secs(20);
const HEARTBEAT_MIN_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_MAX_INTERVAL: Duration = Duration::from_secs(60);
const HEARTBEAT_SAFETY_MARGIN: Duration = Duration::from_secs(10);
const HEARTBEAT_RETRY_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct RegisteredBackend {
    pub backend_id: String,
}

#[derive(Clone, Default)]
pub struct BrokerRegistrationState {
    inner: Arc<RwLock<Option<RegisteredBackend>>>,
}

impl BrokerRegistrationState {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn current(&self) -> Option<RegisteredBackend> {
        self.inner.read().await.clone()
    }

    async fn update(&self, backend_id: String) {
        let mut guard = self.inner.write().await;
        *guard = Some(RegisteredBackend { backend_id });
    }
}

#[derive(Clone)]
struct AuthCookieInterceptor {
    token: String,
}

impl Interceptor for AuthCookieInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let cookie = format!("auth_token={}", self.token);
        let value = MetadataValue::try_from(cookie.as_str())
            .map_err(|_| Status::unauthenticated("invalid broker auth token"))?;
        request.metadata_mut().insert("cookie", value);
        Ok(request)
    }
}

#[derive(Clone)]
struct BrokerGrpcClient {
    channel: Channel,
    origin: Uri,
}

impl BrokerGrpcClient {
    fn new(endpoint: &str) -> anyhow::Result<Self> {
        let endpoint_url = endpoint;
        let origin: Uri = endpoint_url
            .parse()
            .map_err(|err| anyhow!("invalid broker origin URI `{endpoint_url}`: {err}"))?;
        let endpoint = Endpoint::from_shared(endpoint_url.to_string())
            .map_err(|err| anyhow!("invalid broker endpoint URI `{endpoint_url}`: {err}"))?;
        let endpoint = if endpoint_url.starts_with("https://") {
            endpoint
                .tls_config(ClientTlsConfig::new().with_enabled_roots())
                .map_err(|err| {
                    anyhow!("invalid TLS config for broker endpoint `{endpoint_url}`: {err}")
                })?
        } else {
            endpoint
        }
        .connect_timeout(BROKER_CONNECT_TIMEOUT);
        Ok(Self {
            channel: endpoint.connect_lazy(),
            origin,
        })
    }

    fn client_with_cookie(
        &self,
        token: &str,
    ) -> BrokerServiceClient<InterceptedService<Channel, AuthCookieInterceptor>> {
        let interceptor = AuthCookieInterceptor {
            token: token.to_string(),
        };
        let svc = InterceptedService::new(self.channel.clone(), interceptor);
        BrokerServiceClient::with_origin(svc, self.origin.clone())
    }

    async fn register_backend(
        &self,
        token: &str,
        request: RegisterBackendRequest,
    ) -> Result<proto::hackarena::broker::v1::RegisterBackendResponse, Status> {
        let mut client = self.client_with_cookie(token);
        let response = tokio::time::timeout(BROKER_RPC_TIMEOUT, client.register_backend(request))
            .await
            .map_err(|_| Status::deadline_exceeded("broker RegisterBackend timed out"))??;
        Ok(response.into_inner())
    }

    async fn heartbeat(
        &self,
        token: &str,
        backend_id: String,
        backend_secret: Bytes,
    ) -> Result<(), Status> {
        let mut client = self.client_with_cookie(token);
        tokio::time::timeout(
            BROKER_RPC_TIMEOUT,
            client.heartbeat(HeartbeatRequest {
                backend_id,
                backend_secret,
            }),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("broker Heartbeat timed out"))??;
        Ok(())
    }
}

struct BrokerSession {
    backend_id: String,
    heartbeat_interval: Duration,
}

pub async fn start_registration_manager(
    cfg: Arc<Config>,
    shutdown_rx: broadcast::Receiver<()>,
) -> anyhow::Result<(BrokerRegistrationState, JoinHandle<()>)> {
    let local_ipv4_endpoints = resolve_local_ipv4_endpoints(cfg.listen_addr);
    if local_ipv4_endpoints.is_empty() {
        bail!("failed to determine local non-loopback IPv4 address");
    }
    let local_ipv6 = resolve_local_ipv6(cfg.listen_addr)
        .map(|ip| ip.to_string())
        .unwrap_or_default();
    let local_port = u32::from(cfg.listen_addr.port());
    let backend_secret = generate_backend_secret();
    let registration_state = BrokerRegistrationState::new();

    let request = RegisterBackendRequest {
        local_ipv4_endpoints,
        local_ipv6,
        local_port,
        backend_secret,
    };

    let client = BrokerGrpcClient::new(&cfg.broker_endpoint)?;
    let mut token = fetch_auth_token().await?;
    let mut session = register_with_auth_retry(&client, &mut token, &request).await?;
    registration_state.update(session.backend_id.clone()).await;

    tracing::info!(
        backend_id = %session.backend_id,
        heartbeat_interval_s = session.heartbeat_interval.as_secs(),
        broker_endpoint = %cfg.broker_endpoint,
        local_ipv4_endpoints = ?request.local_ipv4_endpoints,
        local_ipv6 = %request.local_ipv6,
        local_port = request.local_port,
        "local broker registration established"
    );

    let state = registration_state.clone();
    Ok((
        registration_state,
        tokio::spawn(async move {
            run_heartbeat_loop(
                client,
                request,
                &mut token,
                &mut session,
                state,
                shutdown_rx,
            )
            .await;
        }),
    ))
}

async fn run_heartbeat_loop(
    client: BrokerGrpcClient,
    request: RegisterBackendRequest,
    token: &mut String,
    session: &mut BrokerSession,
    registration_state: BrokerRegistrationState,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut next_delay = session.heartbeat_interval;

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                break;
            }
            _ = tokio::time::sleep(next_delay) => {}
        }

        match heartbeat_with_recovery(&client, token, &request, session, &registration_state).await
        {
            Ok(()) => {
                next_delay = session.heartbeat_interval;
            }
            Err(err) => {
                tracing::warn!(error = %err, "broker heartbeat failed; will retry");
                next_delay = HEARTBEAT_RETRY_BACKOFF;
            }
        }
    }

    tracing::info!("local broker registration manager stopped");
}

async fn heartbeat_with_recovery(
    client: &BrokerGrpcClient,
    token: &mut String,
    request: &RegisterBackendRequest,
    session: &mut BrokerSession,
    registration_state: &BrokerRegistrationState,
) -> anyhow::Result<()> {
    let mut status = match client
        .heartbeat(
            token,
            session.backend_id.clone(),
            request.backend_secret.clone(),
        )
        .await
    {
        Ok(()) => return Ok(()),
        Err(status) => status,
    };

    if is_auth_error(&status) {
        *token = fetch_auth_token().await?;
        status = match client
            .heartbeat(
                token,
                session.backend_id.clone(),
                request.backend_secret.clone(),
            )
            .await
        {
            Ok(()) => return Ok(()),
            Err(status) => status,
        };
    }

    if status.code() == Code::NotFound {
        tracing::warn!(
            backend_id = %session.backend_id,
            "broker backend entry missing; re-registering"
        );
        *session = register_with_auth_retry(client, token, request).await?;
        registration_state.update(session.backend_id.clone()).await;
        return Ok(());
    }

    Err(anyhow!("broker Heartbeat failed: {status}"))
}

async fn register_with_auth_retry(
    client: &BrokerGrpcClient,
    token: &mut String,
    request: &RegisterBackendRequest,
) -> anyhow::Result<BrokerSession> {
    let mut response = match client.register_backend(token, request.clone()).await {
        Ok(response) => response,
        Err(status) if is_auth_error(&status) => {
            *token = fetch_auth_token().await?;
            client
                .register_backend(token, request.clone())
                .await
                .map_err(|err| {
                    anyhow!("broker RegisterBackend failed after token refresh: {err}")
                })?
        }
        Err(status) => bail!("broker RegisterBackend failed: {status}"),
    };

    let backend_id = response.backend_id.trim().to_string();
    if backend_id.is_empty() {
        bail!("broker RegisterBackend returned an empty backend_id");
    }

    let heartbeat_interval =
        heartbeat_interval_from_expires_at(response.expires_at.take().as_ref());
    if let Some(relay) = response.relay.as_ref() {
        tracing::info!(
            relay_host = %relay.relay_host,
            relay_port = relay.relay_port,
            "broker relay tunnel config received"
        );
    }

    Ok(BrokerSession {
        backend_id,
        heartbeat_interval,
    })
}

fn heartbeat_interval_from_expires_at(expires_at: Option<&Timestamp>) -> Duration {
    let ttl = expires_at.and_then(duration_until_timestamp);
    let Some(ttl) = ttl else {
        return HEARTBEAT_DEFAULT_INTERVAL;
    };
    if ttl == Duration::from_secs(0) {
        return HEARTBEAT_MIN_INTERVAL;
    }

    let interval = ttl.saturating_sub(HEARTBEAT_SAFETY_MARGIN);
    clamp_duration(interval, HEARTBEAT_MIN_INTERVAL, HEARTBEAT_MAX_INTERVAL)
}

fn duration_until_timestamp(ts: &Timestamp) -> Option<Duration> {
    if ts.seconds < 0 || ts.nanos < 0 {
        return None;
    }
    let seconds = u64::try_from(ts.seconds).ok()?;
    let nanos = u32::try_from(ts.nanos).ok()?;
    if nanos >= 1_000_000_000 {
        return None;
    }

    let expires_at = Duration::new(seconds, nanos);
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    Some(expires_at.saturating_sub(now))
}

fn clamp_duration(value: Duration, min: Duration, max: Duration) -> Duration {
    if value < min {
        min
    } else if value > max {
        max
    } else {
        value
    }
}

fn is_auth_error(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unauthenticated | Code::PermissionDenied
    )
}

async fn fetch_auth_token() -> anyhow::Result<String> {
    let candidates = ha_auth_candidates();
    let mut tried = Vec::with_capacity(candidates.len());
    let mut output = None;

    for candidate in candidates {
        let candidate_label = candidate_to_display(&candidate);
        tried.push(candidate_label.clone());

        match Command::new(&candidate)
            .arg("token")
            .arg("--raw")
            .output()
            .await
        {
            Ok(result) => {
                tracing::debug!(ha_auth = %candidate_label, "resolved ha-auth command");
                output = Some(result);
                break;
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                continue;
            }
            Err(err) => {
                return Err(anyhow!(
                    "failed to execute `ha-auth token --raw` via `{candidate_label}`: {err}"
                ));
            }
        }
    }

    let output = output.ok_or_else(|| {
        anyhow!(
            "failed to execute `ha-auth token --raw`; no command found (tried: {})",
            tried.join(", ")
        )
    })?;

    let code = output.status.code();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();

    if output.status.success() {
        if stdout.is_empty() {
            bail!("`ha-auth token --raw` returned empty token");
        }
        return Ok(stdout);
    }

    match code {
        Some(2) => bail!("ha-auth login required; run `hackarena auth login`"),
        Some(10) => bail!("ha-auth network/upstream error: {stderr}"),
        Some(11) => bail!("ha-auth internal/local error: {stderr}"),
        Some(code) => bail!("ha-auth failed with exit code {code}: {stderr}"),
        None => bail!("ha-auth process terminated by signal"),
    }
}

fn ha_auth_candidates() -> Vec<OsString> {
    let mut candidates = Vec::new();

    if let Some(env_path) = std::env::var_os("HA_AUTH_BIN") {
        if !env_path.is_empty() {
            candidates.push(env_path);
        }
    }

    #[cfg(target_os = "windows")]
    {
        if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") {
            let base = PathBuf::from(local_app_data);
            candidates.push(
                base.join("HackArena")
                    .join("bin")
                    .join("ha-auth.exe")
                    .into_os_string(),
            );
            candidates.push(base.join("ha-auth").join("ha-auth.exe").into_os_string());
        }
    }

    #[cfg(target_os = "linux")]
    {
        if let Some(xdg_data_home) = std::env::var_os("XDG_DATA_HOME") {
            candidates.push(
                PathBuf::from(xdg_data_home)
                    .join("HackArena")
                    .join("bin")
                    .join("ha-auth")
                    .into_os_string(),
            );
        }
        if let Some(home) = home_dir() {
            candidates.push(
                home.join(".local")
                    .join("share")
                    .join("HackArena")
                    .join("bin")
                    .join("ha-auth")
                    .into_os_string(),
            );
            candidates.push(
                home.join(".local")
                    .join("bin")
                    .join("ha-auth")
                    .into_os_string(),
            );
        }
    }

    #[cfg(target_os = "macos")]
    {
        if let Some(home) = home_dir() {
            candidates.push(
                home.join("Library")
                    .join("Application Support")
                    .join("HackArena")
                    .join("bin")
                    .join("ha-auth")
                    .into_os_string(),
            );
            candidates.push(
                home.join(".local")
                    .join("bin")
                    .join("ha-auth")
                    .into_os_string(),
            );
        }
    }

    #[cfg(target_os = "windows")]
    {
        candidates.push(PathBuf::from("ha-auth.exe").into_os_string());
    }
    #[cfg(not(target_os = "windows"))]
    {
        candidates.push(PathBuf::from("./ha-auth").into_os_string());
    }
    candidates.push(OsString::from("ha-auth"));

    dedup_os_strings(candidates)
}

fn dedup_os_strings(values: Vec<OsString>) -> Vec<OsString> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for value in values {
        if seen.insert(value.clone()) {
            out.push(value);
        }
    }
    out
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn home_dir() -> Option<PathBuf> {
    let path = std::env::var_os("HOME")?;
    if path.is_empty() {
        None
    } else {
        Some(PathBuf::from(path))
    }
}

fn candidate_to_display(candidate: &OsString) -> String {
    let path = Path::new(candidate);
    path.to_string_lossy().to_string()
}

fn resolve_local_ipv4_endpoints(listen_addr: SocketAddr) -> Vec<String> {
    let mut candidates = Vec::new();
    if let IpAddr::V4(ip) = listen_addr.ip() {
        if !ip.is_loopback() && !ip.is_unspecified() {
            candidates.push(ip);
        }
    }
    if let Some(ip) = discover_ipv4() {
        candidates.push(ip);
    }
    candidates.extend(discover_interface_ipv4s());

    let mut seen = HashSet::new();
    let mut endpoints = Vec::new();
    for ip in candidates {
        if seen.insert(ip) {
            endpoints.push(ip.to_string());
        }
    }
    endpoints
}

fn resolve_local_ipv6(listen_addr: SocketAddr) -> Option<Ipv6Addr> {
    match listen_addr.ip() {
        IpAddr::V6(ip) if !ip.is_loopback() && !ip.is_unspecified() => Some(ip),
        _ => discover_ipv6(),
    }
}

fn discover_ipv4() -> Option<Ipv4Addr> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    socket.connect((Ipv4Addr::new(1, 1, 1, 1), 80)).ok()?;
    match socket.local_addr().ok()?.ip() {
        IpAddr::V4(ip) if !ip.is_loopback() && !ip.is_unspecified() => Some(ip),
        _ => None,
    }
}

fn discover_interface_ipv4s() -> Vec<Ipv4Addr> {
    let Ok(ifaces) = if_addrs::get_if_addrs() else {
        return Vec::new();
    };

    let mut addresses = Vec::new();
    for iface in ifaces {
        let IfAddr::V4(v4) = iface.addr else {
            continue;
        };
        let ip = v4.ip;
        if ip.is_loopback() || ip.is_unspecified() || ip.is_link_local() {
            continue;
        }
        addresses.push(ip);
    }
    addresses
}

fn discover_ipv6() -> Option<Ipv6Addr> {
    let socket = UdpSocket::bind((Ipv6Addr::UNSPECIFIED, 0)).ok()?;
    socket
        .connect((
            Ipv6Addr::new(0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111),
            80,
        ))
        .ok()?;
    match socket.local_addr().ok()?.ip() {
        IpAddr::V6(ip) if !ip.is_loopback() && !ip.is_unspecified() => Some(ip),
        _ => None,
    }
}

fn generate_backend_secret() -> Bytes {
    let mut secret = vec![0_u8; 32];
    OsRng.fill_bytes(&mut secret);
    Bytes::from(secret)
}
