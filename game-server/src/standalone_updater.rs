use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow, bail};
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, ETAG, IF_NONE_MATCH, USER_AGENT};
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;
use zip::ZipArchive;

use windows_sys::Win32::Foundation::{CloseHandle, WAIT_OBJECT_0, WAIT_TIMEOUT};
use windows_sys::Win32::System::Threading::{
    OpenProcess, PROCESS_SYNCHRONIZE, WaitForSingleObject,
};

const GITHUB_OWNER: &str = "INIT-SGGW";
const GITHUB_REPO: &str = "HackArena3.0-Backend";
const GITHUB_API_BASE_URL: &str = "https://api.github.com";
const GITHUB_API_TIMEOUT: Duration = Duration::from_secs(15);
const STANDALONE_USER_AGENT: &str = "ha3-standalone-updater";
const SHA256SUMS_ASSET_NAME: &str = "SHA256SUMS.txt";
const MIN_SUPPORTED_TAG: &str = "v0.2.0-beta.9";
const USER_LOG_TARGET: &str = "ha3_standalone::user";
const STANDALONE_UPDATE_BINARY_NAME: &str = "ha3-standalone-update.exe";
pub const DEFAULT_UPDATE_CACHE_TTL_MINUTES: u64 = 30;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StartupArgs {
    pub update_to: Option<String>,
    pub ignore_update_cache: bool,
    pub show_help: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct UpdaterConfig {
    pub metadata_ttl: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyUpdateArgs {
    pub old_pid: u32,
    pub install_dir: PathBuf,
    pub zip_path: PathBuf,
    pub staging_dir: PathBuf,
    pub tag: String,
    pub auth_token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupUpdateOutcome {
    ContinueCurrent,
    ExitForUpdate,
}

#[derive(Debug)]
struct StandaloneUpdater {
    cache_root: PathBuf,
    state_path: PathBuf,
    install_dir: PathBuf,
    metadata_ttl: Duration,
    http_client: reqwest::Client,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct UpdaterState {
    #[serde(default)]
    metadata_etag: Option<String>,
    #[serde(default)]
    metadata_checked_at_unix_secs: Option<u64>,
    #[serde(default)]
    releases: Vec<CachedRelease>,
    #[serde(default)]
    skipped_tags: BTreeSet<String>,
    #[serde(default)]
    rate_limit_reset_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedRelease {
    tag_name: String,
    prerelease: bool,
    assets: Vec<CachedAsset>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CachedAsset {
    name: String,
    browser_download_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ApplyAuthorization {
    install_dir: PathBuf,
    zip_path: PathBuf,
    staging_dir: PathBuf,
    tag: String,
    created_at_unix_secs: u64,
}

#[derive(Debug, Deserialize)]
struct GitHubReleaseResponse {
    tag_name: String,
    draft: bool,
    prerelease: bool,
    #[serde(default)]
    assets: Vec<GitHubReleaseAsset>,
}

#[derive(Debug, Deserialize)]
struct GitHubReleaseAsset {
    name: String,
    browser_download_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReleaseEntry {
    tag_name: String,
    version: Version,
    channel: ReleaseChannel,
    assets: Vec<CachedAsset>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReleaseChannel {
    Stable,
    Prerelease,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AutomaticUpdateCandidates {
    prerelease: Option<ReleaseEntry>,
    stable: Option<ReleaseEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PromptAction {
    Update(String),
    Skip(String),
    StartCurrent,
}

impl StartupArgs {
    pub fn parse_from_env() -> anyhow::Result<Self> {
        parse_startup_args_from_iter(std::env::args())
    }

    pub fn print_help(binary_name: &str) {
        println!("Usage: {binary_name} [--update-to <tag>] [--ignore-update-cache]");
        println!();
        println!("Standalone updater options:");
        println!(
            "  --update-to <tag>         Update to an exact GitHub release tag before startup."
        );
        println!("  --ignore-update-cache     Force a fresh GitHub release metadata check.");
        println!("  -h, --help                Show this help and exit.");
    }
}

pub async fn run_startup_update(
    startup_args: &StartupArgs,
    config: UpdaterConfig,
) -> anyhow::Result<StartupUpdateOutcome> {
    let current_tag = current_release_tag();
    let current_version = parse_release_version(&current_tag)
        .ok_or_else(|| anyhow!("current standalone version `{current_tag}` is not valid semver"))?;

    if let Some(target_tag) = startup_args.update_to.as_deref() {
        let updater = StandaloneUpdater::new(config.metadata_ttl)?;
        return updater
            .perform_manual_update(&current_tag, &current_version, target_tag)
            .await;
    }

    match StandaloneUpdater::new(config.metadata_ttl) {
        Ok(updater) => {
            updater
                .perform_automatic_update(
                    &current_tag,
                    &current_version,
                    startup_args.ignore_update_cache,
                )
                .await
        }
        Err(err) => {
            tracing::warn!(error = %err, "standalone updater initialization failed");
            eprintln!("Could not initialize the standalone updater: {:#}", err);
            eprintln!("Starting the current version.");
            Ok(StartupUpdateOutcome::ContinueCurrent)
        }
    }
}

pub fn updater_binary_name() -> &'static str {
    STANDALONE_UPDATE_BINARY_NAME
}

pub fn parse_apply_update_args_from_iter<I>(args: I) -> anyhow::Result<ApplyUpdateArgs>
where
    I: IntoIterator,
    I::Item: Into<String>,
{
    let mut iter = args.into_iter().map(Into::into);
    let _ = iter.next();
    let Some(command) = iter.next() else {
        bail!("missing updater subcommand")
    };
    if command != "apply" {
        bail!("unknown updater subcommand `{command}`");
    }

    let mut old_pid: Option<u32> = None;
    let mut install_dir: Option<PathBuf> = None;
    let mut zip_path: Option<PathBuf> = None;
    let mut staging_dir: Option<PathBuf> = None;
    let mut tag: Option<String> = None;
    let mut auth_token: Option<String> = None;

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--old-pid" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow!("`--old-pid` requires a numeric value"))?;
                old_pid = Some(
                    value
                        .parse::<u32>()
                        .with_context(|| format!("invalid `--old-pid` value `{value}`"))?,
                );
            }
            "--install-dir" => {
                install_dir = Some(PathBuf::from(
                    iter.next()
                        .ok_or_else(|| anyhow!("`--install-dir` requires a path"))?,
                ));
            }
            "--zip-path" => {
                zip_path = Some(PathBuf::from(
                    iter.next()
                        .ok_or_else(|| anyhow!("`--zip-path` requires a path"))?,
                ));
            }
            "--staging-dir" => {
                staging_dir = Some(PathBuf::from(
                    iter.next()
                        .ok_or_else(|| anyhow!("`--staging-dir` requires a path"))?,
                ));
            }
            "--tag" => {
                tag = Some(normalize_release_tag_input(
                    &iter
                        .next()
                        .ok_or_else(|| anyhow!("`--tag` requires a release tag"))?,
                )?);
            }
            "--auth-token" => {
                auth_token = Some(
                    iter.next()
                        .ok_or_else(|| anyhow!("`--auth-token` requires a token"))?,
                );
            }
            other => bail!("unknown updater argument `{other}`"),
        }
    }

    Ok(ApplyUpdateArgs {
        old_pid: old_pid.ok_or_else(|| anyhow!("`--old-pid` is required"))?,
        install_dir: install_dir.ok_or_else(|| anyhow!("`--install-dir` is required"))?,
        zip_path: zip_path.ok_or_else(|| anyhow!("`--zip-path` is required"))?,
        staging_dir: staging_dir.ok_or_else(|| anyhow!("`--staging-dir` is required"))?,
        tag: tag.ok_or_else(|| anyhow!("`--tag` is required"))?,
        auth_token: auth_token.ok_or_else(|| anyhow!("`--auth-token` is required"))?,
    })
}

pub fn print_apply_update_help(binary_name: &str) {
    println!(
        "Usage: {binary_name} apply --old-pid <pid> --install-dir <path> --zip-path <path> --staging-dir <path> --tag <release-tag> --auth-token <token>"
    );
    println!("This helper is started automatically by ha3-standalone.exe during an update.");
}

pub fn run_apply_update(args: &ApplyUpdateArgs) -> anyhow::Result<()> {
    authorize_apply_request_from_env(args)?;
    tracing::info!(
        target: USER_LOG_TARGET,
        old_pid = args.old_pid,
        install_dir = %display_path(&args.install_dir),
        zip_path = %display_path(&args.zip_path),
        staging_dir = %display_path(&args.staging_dir),
        target_tag = %args.tag,
        "Applying standalone update"
    );

    wait_for_process_exit(args.old_pid, Duration::from_secs(60))?;
    prepare_staging_dir(&args.staging_dir, &args.zip_path)?;
    apply_staged_update(&args.install_dir, &args.staging_dir)?;
    relaunch_standalone(&args.install_dir)
}

impl StandaloneUpdater {
    fn new(metadata_ttl: Duration) -> anyhow::Result<Self> {
        let cache_root = updater_cache_root_from_env()?;
        let install_dir = std::env::current_exe()
            .context("failed to resolve current executable path")?
            .parent()
            .ok_or_else(|| anyhow!("failed to resolve standalone install directory"))?
            .to_path_buf();
        let state_path = cache_root.join("state.json");
        let http_client = reqwest::Client::builder()
            .timeout(GITHUB_API_TIMEOUT)
            .build()
            .context("failed to build GitHub HTTP client")?;

        Ok(Self {
            cache_root,
            state_path,
            install_dir,
            metadata_ttl,
            http_client,
        })
    }

    async fn perform_automatic_update(
        &self,
        current_tag: &str,
        current_version: &Version,
        ignore_update_cache: bool,
    ) -> anyhow::Result<StartupUpdateOutcome> {
        tracing::debug!(
            current_version = current_tag,
            cache_ttl_minutes = self.metadata_ttl.as_secs() / 60,
            "Checking for standalone updates"
        );
        let result = self
            .try_perform_automatic_update(current_tag, current_version, ignore_update_cache)
            .await;
        match result {
            Ok(outcome) => Ok(outcome),
            Err(err) => {
                tracing::warn!(error = %err, "standalone update check failed");
                eprintln!("Could not complete the standalone update check: {:#}", err);
                eprintln!("Starting the current version.");
                Ok(StartupUpdateOutcome::ContinueCurrent)
            }
        }
    }

    async fn try_perform_automatic_update(
        &self,
        current_tag: &str,
        current_version: &Version,
        ignore_update_cache: bool,
    ) -> anyhow::Result<StartupUpdateOutcome> {
        self.ensure_cache_root()?;

        let mut state = self.load_state_or_default();
        let maybe_releases = self
            .load_releases_for_automatic_check(&mut state, ignore_update_cache)
            .await?;
        let Some(releases) = maybe_releases else {
            return Ok(StartupUpdateOutcome::ContinueCurrent);
        };

        let parsed_releases = parse_release_entries(&releases);
        let candidates =
            select_automatic_candidates(current_version, &parsed_releases, &state.skipped_tags);
        if candidates.prerelease.is_none() && candidates.stable.is_none() {
            tracing::info!(current_version = %current_tag, "no newer standalone release available");
            return Ok(StartupUpdateOutcome::ContinueCurrent);
        }

        tracing::info!(
            target: USER_LOG_TARGET,
            prerelease = candidates.prerelease.as_ref().map(|entry| entry.tag_name.as_str()),
            stable = candidates.stable.as_ref().map(|entry| entry.tag_name.as_str()),
            "A newer standalone build is available"
        );

        match prompt_for_automatic_action(current_tag, &candidates)? {
            PromptAction::StartCurrent => Ok(StartupUpdateOutcome::ContinueCurrent),
            PromptAction::Skip(tag) => {
                state.skipped_tags.insert(tag.clone());
                self.save_state(&state)?;
                tracing::info!(
                    target: USER_LOG_TARGET,
                    skipped_tag = %tag,
                    "Skipping standalone update version"
                );
                println!("Skipping {tag} for future automatic prompts.");
                Ok(StartupUpdateOutcome::ContinueCurrent)
            }
            PromptAction::Update(tag) => match self.fetch_release_by_tag(&tag).await {
                Ok(release) => self.prepare_and_launch_update(&release).await,
                Err(err) => {
                    tracing::warn!(
                        target: USER_LOG_TARGET,
                        target_tag = %tag,
                        error = %err,
                        "failed to prepare standalone update"
                    );
                    eprintln!(
                        "Could not prepare the standalone update to {tag}: {:#}",
                        err
                    );
                    eprintln!("Starting the current version.");
                    Ok(StartupUpdateOutcome::ContinueCurrent)
                }
            },
        }
    }

    async fn perform_manual_update(
        &self,
        current_tag: &str,
        current_version: &Version,
        target_tag: &str,
    ) -> anyhow::Result<StartupUpdateOutcome> {
        self.ensure_cache_root()?;

        let normalized_tag = normalize_release_tag_input(target_tag)?;
        tracing::info!(
            target: USER_LOG_TARGET,
            target_tag = %normalized_tag,
            current_version = current_tag,
            "Running manual standalone update"
        );
        let target_version = parse_release_version(&normalized_tag)
            .ok_or_else(|| anyhow!("`{normalized_tag}` is not a valid release tag"))?;
        let min_supported = min_supported_version();
        if target_version < min_supported {
            bail!(
                "manual update target `{normalized_tag}` is below the minimum supported version `{MIN_SUPPORTED_TAG}`"
            );
        }
        if target_version == *current_version {
            println!(
                "Standalone {current_tag} is already the current version. Starting it normally."
            );
            return Ok(StartupUpdateOutcome::ContinueCurrent);
        }
        if target_version < *current_version
            && !prompt_yes_no(&format!(
                "The requested version {normalized_tag} is older than the current version {current_tag}. Continue with this downgrade?"
            ))?
        {
            println!("Manual update cancelled. Starting the current version.");
            return Ok(StartupUpdateOutcome::ContinueCurrent);
        }

        let release = self.fetch_release_by_tag(&normalized_tag).await?;
        self.prepare_and_launch_update(&release).await
    }

    fn ensure_cache_root(&self) -> anyhow::Result<()> {
        fs::create_dir_all(&self.cache_root).with_context(|| {
            format!(
                "failed to create updater cache directory {}",
                display_path(&self.cache_root)
            )
        })
    }

    fn load_state_or_default(&self) -> UpdaterState {
        match self.load_state() {
            Ok(state) => state,
            Err(err) => {
                eprintln!(
                    "Could not read updater state from {}: {:#}",
                    display_path(&self.state_path),
                    err
                );
                UpdaterState::default()
            }
        }
    }

    fn load_state(&self) -> anyhow::Result<UpdaterState> {
        if !self.state_path.is_file() {
            return Ok(UpdaterState::default());
        }
        let raw = fs::read_to_string(&self.state_path).with_context(|| {
            format!(
                "failed to read updater state file {}",
                display_path(&self.state_path)
            )
        })?;
        let state = serde_json::from_str(&raw).with_context(|| {
            format!(
                "failed to decode updater state file {}",
                display_path(&self.state_path)
            )
        })?;
        Ok(state)
    }

    fn save_state(&self, state: &UpdaterState) -> anyhow::Result<()> {
        self.ensure_cache_root()?;
        let raw = serde_json::to_string_pretty(state).context("failed to encode updater state")?;
        fs::write(&self.state_path, raw).with_context(|| {
            format!(
                "failed to write updater state file {}",
                display_path(&self.state_path)
            )
        })
    }

    async fn load_releases_for_automatic_check(
        &self,
        state: &mut UpdaterState,
        ignore_update_cache: bool,
    ) -> anyhow::Result<Option<Vec<CachedRelease>>> {
        if !ignore_update_cache && metadata_cache_is_fresh(state, self.metadata_ttl) {
            tracing::debug!(
                cache_root = %display_path(&self.cache_root),
                checked_at_unix_secs = state.metadata_checked_at_unix_secs,
                release_count = state.releases.len(),
                "using cached standalone update metadata"
            );
            return Ok(Some(state.releases.clone()));
        }

        if !ignore_update_cache && let Some(reset_at) = active_rate_limit_reset(state) {
            tracing::warn!(
                target: USER_LOG_TARGET,
                reset_at_unix_secs = reset_at,
                "GitHub rate limit is active for standalone update checks"
            );
            eprintln!(
                "Could not check for standalone updates because the GitHub rate limit is active until Unix time {reset_at}. Starting the current version."
            );
            return Ok(None);
        }

        let url = format!(
            "{GITHUB_API_BASE_URL}/repos/{GITHUB_OWNER}/{GITHUB_REPO}/releases?per_page=100"
        );
        let mut request = self
            .http_client
            .get(&url)
            .header(USER_AGENT, STANDALONE_USER_AGENT)
            .header(ACCEPT, "application/vnd.github+json");
        if !ignore_update_cache && let Some(etag) = state.metadata_etag.as_deref() {
            request = request.header(IF_NONE_MATCH, etag);
        }

        tracing::debug!(
            url,
            ignore_update_cache,
            has_cached_etag = state.metadata_etag.is_some(),
            "requesting standalone release metadata from GitHub"
        );
        let response = request
            .send()
            .await
            .context("failed to query GitHub releases")?;
        match response.status() {
            StatusCode::OK => {
                let etag = response
                    .headers()
                    .get(ETAG)
                    .and_then(|value| value.to_str().ok())
                    .map(ToOwned::to_owned);
                let releases: Vec<GitHubReleaseResponse> = response
                    .json()
                    .await
                    .context("failed to decode GitHub release list")?;
                state.metadata_etag = etag;
                state.metadata_checked_at_unix_secs = Some(now_unix_secs());
                state.rate_limit_reset_unix_secs = None;
                state.releases = releases
                    .into_iter()
                    .filter(|release| !release.draft)
                    .map(CachedRelease::from)
                    .collect();
                tracing::debug!(
                    release_count = state.releases.len(),
                    "standalone release metadata refreshed from GitHub"
                );
                self.save_state(state)?;
                Ok(Some(state.releases.clone()))
            }
            StatusCode::NOT_MODIFIED => {
                state.metadata_checked_at_unix_secs = Some(now_unix_secs());
                tracing::debug!("standalone release metadata not modified (ETag)");
                self.save_state(state)?;
                Ok(Some(state.releases.clone()))
            }
            status if is_rate_limited_status(status) => {
                let reset_at = response
                    .headers()
                    .get("x-ratelimit-reset")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok());
                state.rate_limit_reset_unix_secs = reset_at;
                tracing::warn!(
                    target: USER_LOG_TARGET,
                    http_status = status.as_u16(),
                    reset_at_unix_secs = reset_at,
                    "GitHub rate limit reached while checking standalone updates"
                );
                self.save_state(state)?;
                if let Some(reset_at) = reset_at {
                    eprintln!(
                        "Could not check for standalone updates because the GitHub rate limit was reached. Automatic checks will retry after Unix time {reset_at}. Starting the current version."
                    );
                } else {
                    eprintln!(
                        "Could not check for standalone updates because the GitHub rate limit was reached. Starting the current version."
                    );
                }
                Ok(None)
            }
            status => {
                bail!("GitHub release list request failed with HTTP {status}");
            }
        }
    }

    async fn fetch_release_by_tag(&self, tag: &str) -> anyhow::Result<ReleaseEntry> {
        let url =
            format!("{GITHUB_API_BASE_URL}/repos/{GITHUB_OWNER}/{GITHUB_REPO}/releases/tags/{tag}");
        tracing::debug!(url, tag, "requesting standalone release by tag");
        let response = self
            .http_client
            .get(&url)
            .header(USER_AGENT, STANDALONE_USER_AGENT)
            .header(ACCEPT, "application/vnd.github+json")
            .send()
            .await
            .with_context(|| format!("failed to query GitHub release `{tag}`"))?;
        match response.status() {
            StatusCode::OK => {
                let release: GitHubReleaseResponse = response
                    .json()
                    .await
                    .with_context(|| format!("failed to decode GitHub release `{tag}`"))?;
                if release.draft {
                    bail!("GitHub release `{tag}` is a draft and cannot be used");
                }
                parse_single_release(CachedRelease::from(release)).ok_or_else(|| {
                    anyhow!("GitHub release `{tag}` is not a supported standalone tag")
                })
            }
            StatusCode::NOT_FOUND => bail!("GitHub release tag `{tag}` was not found"),
            status if is_rate_limited_status(status) => {
                bail!("GitHub rate limit reached while resolving release `{tag}`")
            }
            status => bail!("GitHub release request for `{tag}` failed with HTTP {status}"),
        }
    }

    async fn prepare_and_launch_update(
        &self,
        release: &ReleaseEntry,
    ) -> anyhow::Result<StartupUpdateOutcome> {
        tracing::info!(
            target: USER_LOG_TARGET,
            target_tag = %release.tag_name,
            "Preparing standalone update"
        );
        println!("Preparing standalone update to {}...", release.tag_name);
        let zip_path = self.ensure_verified_download(release).await?;
        self.launch_apply_helper(release, &zip_path)?;
        Ok(StartupUpdateOutcome::ExitForUpdate)
    }

    async fn ensure_verified_download(&self, release: &ReleaseEntry) -> anyhow::Result<PathBuf> {
        let download_dir = self.download_dir(&release.tag_name);
        fs::create_dir_all(&download_dir).with_context(|| {
            format!(
                "failed to create download cache directory {}",
                display_path(&download_dir)
            )
        })?;

        let expected_zip_name = expected_zip_asset_name(&release.tag_name);
        let zip_asset = release
            .assets
            .iter()
            .find(|asset| asset.name == expected_zip_name)
            .ok_or_else(|| {
                anyhow!(
                    "release `{}` does not contain `{expected_zip_name}`",
                    release.tag_name
                )
            })?;
        let checksum_asset = release
            .assets
            .iter()
            .find(|asset| asset.name.eq_ignore_ascii_case(SHA256SUMS_ASSET_NAME))
            .ok_or_else(|| {
                anyhow!(
                    "release `{}` does not contain `{SHA256SUMS_ASSET_NAME}`",
                    release.tag_name
                )
            })?;

        let checksum_path = download_dir.join(SHA256SUMS_ASSET_NAME);
        let checksum_text = self
            .ensure_checksum_manifest(&checksum_path, checksum_asset)
            .await?;
        let checksum_map = parse_checksum_manifest(&checksum_text)?;
        let expected_hash = checksum_map.get(&expected_zip_name).ok_or_else(|| {
            anyhow!(
                "{SHA256SUMS_ASSET_NAME} for `{}` does not contain `{expected_zip_name}`",
                release.tag_name
            )
        })?;

        let zip_path = download_dir.join(&expected_zip_name);
        if zip_path.is_file() && file_sha256_matches(&zip_path, expected_hash)? {
            tracing::debug!(
                target_tag = %release.tag_name,
                path = %display_path(&zip_path),
                "reusing cached standalone package download"
            );
            return Ok(zip_path);
        }

        tracing::debug!(
            target_tag = %release.tag_name,
            asset_name = %expected_zip_name,
            "downloading standalone package from GitHub"
        );
        self.download_binary_asset(&zip_asset.browser_download_url, &zip_path)
            .await
            .with_context(|| format!("failed to download `{expected_zip_name}`"))?;
        if !file_sha256_matches(&zip_path, expected_hash)? {
            let _ = fs::remove_file(&zip_path);
            bail!(
                "downloaded standalone package `{expected_zip_name}` failed SHA-256 verification"
            );
        }
        Ok(zip_path)
    }

    async fn ensure_checksum_manifest(
        &self,
        checksum_path: &Path,
        checksum_asset: &CachedAsset,
    ) -> anyhow::Result<String> {
        if checksum_path.is_file() {
            let cached = fs::read_to_string(checksum_path).with_context(|| {
                format!(
                    "failed to read cached checksum file {}",
                    display_path(checksum_path)
                )
            })?;
            if parse_checksum_manifest(&cached).is_ok() {
                return Ok(cached);
            }
        }

        let bytes = self
            .download_bytes(&checksum_asset.browser_download_url)
            .await
            .with_context(|| format!("failed to download `{}`", checksum_asset.name))?;
        let text = String::from_utf8(bytes).with_context(|| {
            format!(
                "downloaded checksum file `{}` is not valid UTF-8",
                checksum_asset.name
            )
        })?;
        parse_checksum_manifest(&text)?;
        fs::write(checksum_path, &text).with_context(|| {
            format!(
                "failed to write checksum file {}",
                display_path(checksum_path)
            )
        })?;
        Ok(text)
    }

    async fn download_binary_asset(&self, url: &str, destination: &Path) -> anyhow::Result<()> {
        let bytes = self.download_bytes(url).await?;
        let temp_path = destination.with_extension("part");
        fs::write(&temp_path, &bytes).with_context(|| {
            format!(
                "failed to write temporary download file {}",
                display_path(&temp_path)
            )
        })?;
        if destination.exists() {
            fs::remove_file(destination).with_context(|| {
                format!(
                    "failed to replace cached download {}",
                    display_path(destination)
                )
            })?;
        }
        fs::rename(&temp_path, destination).with_context(|| {
            format!(
                "failed to move downloaded file into {}",
                display_path(destination)
            )
        })?;
        Ok(())
    }

    async fn download_bytes(&self, url: &str) -> anyhow::Result<Vec<u8>> {
        let response = self
            .http_client
            .get(url)
            .header(USER_AGENT, STANDALONE_USER_AGENT)
            .send()
            .await
            .with_context(|| format!("failed to download GitHub asset from {url}"))?;
        if !response.status().is_success() {
            bail!(
                "GitHub asset download failed with HTTP {}",
                response.status()
            );
        }
        let bytes = response
            .bytes()
            .await
            .context("failed to read GitHub asset bytes")?;
        Ok(bytes.to_vec())
    }

    fn download_dir(&self, tag: &str) -> PathBuf {
        self.cache_root
            .join("downloads")
            .join(sanitize_path_component(tag))
    }

    fn staging_dir(&self, tag: &str) -> PathBuf {
        self.cache_root
            .join("staging")
            .join(sanitize_path_component(tag))
    }

    fn launch_apply_helper(&self, release: &ReleaseEntry, zip_path: &Path) -> anyhow::Result<()> {
        let installed_updater_path = self.install_dir.join(STANDALONE_UPDATE_BINARY_NAME);
        if !installed_updater_path.is_file() {
            bail!(
                "standalone updater binary is missing from the install directory: {}",
                display_path(&installed_updater_path)
            );
        }

        let runner_dir = self.cache_root.join("runner");
        fs::create_dir_all(&runner_dir).with_context(|| {
            format!(
                "failed to create updater runner directory {}",
                display_path(&runner_dir)
            )
        })?;
        let helper_path = runner_dir.join(format!(
            "ha3-standalone-update-runner-{}.exe",
            sanitize_path_component(&release.tag_name)
        ));
        let staging_dir = self.staging_dir(&release.tag_name);
        let auth_token = Uuid::new_v4().to_string();
        fs::copy(&installed_updater_path, &helper_path).with_context(|| {
            format!(
                "failed to stage updater runner {}",
                display_path(&helper_path)
            )
        })?;
        self.write_apply_authorization(
            &auth_token,
            &ApplyAuthorization {
                install_dir: self.install_dir.clone(),
                zip_path: zip_path.to_path_buf(),
                staging_dir: staging_dir.clone(),
                tag: release.tag_name.clone(),
                created_at_unix_secs: now_unix_secs(),
            },
        )?;

        println!(
            "Applying standalone update to {}. The current version will close now.",
            release.tag_name
        );
        tracing::info!(
            target: USER_LOG_TARGET,
            target_tag = %release.tag_name,
            helper_path = %display_path(&helper_path),
            updater_path = %display_path(&installed_updater_path),
            zip_path = %display_path(zip_path),
            "Launching standalone update helper"
        );

        let mut command = Command::new(&helper_path);
        command
            .arg("apply")
            .arg("--old-pid")
            .arg(std::process::id().to_string())
            .arg("--install-dir")
            .arg(&self.install_dir)
            .arg("--zip-path")
            .arg(zip_path)
            .arg("--staging-dir")
            .arg(&staging_dir)
            .arg("--tag")
            .arg(&release.tag_name)
            .arg("--auth-token")
            .arg(&auth_token)
            .current_dir(&self.install_dir)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());

        if let Err(err) = command.spawn() {
            let _ = self.remove_apply_authorization(&auth_token);
            return Err(anyhow!(err)).with_context(|| {
                format!(
                    "failed to launch update helper {}",
                    display_path(&helper_path)
                )
            });
        }
        Ok(())
    }

    fn write_apply_authorization(
        &self,
        token: &str,
        authorization: &ApplyAuthorization,
    ) -> anyhow::Result<()> {
        let auth_dir = self.cache_root.join("pending-apply");
        fs::create_dir_all(&auth_dir).with_context(|| {
            format!(
                "failed to create apply authorization directory {}",
                display_path(&auth_dir)
            )
        })?;
        let path = apply_authorization_path(&self.cache_root, token);
        let raw = serde_json::to_string_pretty(authorization)
            .context("failed to encode apply authorization")?;
        fs::write(&path, raw).with_context(|| {
            format!(
                "failed to write apply authorization file {}",
                display_path(&path)
            )
        })
    }

    fn remove_apply_authorization(&self, token: &str) -> anyhow::Result<()> {
        let path = apply_authorization_path(&self.cache_root, token);
        if !path.exists() {
            return Ok(());
        }
        fs::remove_file(&path).with_context(|| {
            format!(
                "failed to remove apply authorization file {}",
                display_path(&path)
            )
        })
    }
}

impl From<GitHubReleaseResponse> for CachedRelease {
    fn from(value: GitHubReleaseResponse) -> Self {
        Self {
            tag_name: value.tag_name,
            prerelease: value.prerelease,
            assets: value.assets.into_iter().map(CachedAsset::from).collect(),
        }
    }
}

impl From<GitHubReleaseAsset> for CachedAsset {
    fn from(value: GitHubReleaseAsset) -> Self {
        Self {
            name: value.name,
            browser_download_url: value.browser_download_url,
        }
    }
}

fn parse_startup_args_from_iter<I>(args: I) -> anyhow::Result<StartupArgs>
where
    I: IntoIterator,
    I::Item: Into<String>,
{
    let mut parsed = StartupArgs::default();
    let mut iter = args.into_iter().map(Into::into);
    let _ = iter.next();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--ignore-update-cache" => {
                parsed.ignore_update_cache = true;
            }
            "--update-to" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow!("`--update-to` requires a release tag"))?;
                parsed.update_to = Some(normalize_release_tag_input(&value)?);
            }
            "-h" | "--help" => {
                parsed.show_help = true;
            }
            other if other.starts_with("--update-to=") => {
                let (_, value) = other
                    .split_once('=')
                    .ok_or_else(|| anyhow!("`--update-to` requires a release tag"))?;
                parsed.update_to = Some(normalize_release_tag_input(value)?);
            }
            other => bail!("unknown standalone argument `{other}`"),
        }
    }
    Ok(parsed)
}

fn current_release_tag() -> String {
    format!("v{}", env!("CARGO_PKG_VERSION"))
}

fn normalize_release_tag_input(raw: &str) -> anyhow::Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("release tag must not be empty");
    }
    if trimmed.starts_with('v') {
        Ok(trimmed.to_string())
    } else {
        Ok(format!("v{trimmed}"))
    }
}

fn parse_release_entries(releases: &[CachedRelease]) -> Vec<ReleaseEntry> {
    let mut parsed: Vec<ReleaseEntry> = releases
        .iter()
        .cloned()
        .filter_map(parse_single_release)
        .collect();
    parsed.sort_by(|left, right| right.version.cmp(&left.version));
    parsed
}

fn parse_single_release(release: CachedRelease) -> Option<ReleaseEntry> {
    let version = parse_release_version(&release.tag_name)?;
    if version < min_supported_version() {
        return None;
    }
    let channel = if !version.pre.is_empty() || release.prerelease {
        ReleaseChannel::Prerelease
    } else {
        ReleaseChannel::Stable
    };
    Some(ReleaseEntry {
        tag_name: release.tag_name,
        version,
        channel,
        assets: release.assets,
    })
}

fn parse_release_version(tag: &str) -> Option<Version> {
    let normalized = tag.trim().strip_prefix('v')?;
    Version::parse(normalized).ok()
}

fn min_supported_version() -> Version {
    Version::parse(MIN_SUPPORTED_TAG.trim_start_matches('v'))
        .expect("minimum standalone version must be valid semver")
}

fn select_automatic_candidates(
    current_version: &Version,
    releases: &[ReleaseEntry],
    skipped_tags: &BTreeSet<String>,
) -> AutomaticUpdateCandidates {
    let stable = releases
        .iter()
        .find(|release| {
            release.channel == ReleaseChannel::Stable
                && release.version > *current_version
                && !skipped_tags.contains(&release.tag_name)
        })
        .cloned();

    let prerelease = if current_version.pre.is_empty() {
        None
    } else {
        releases
            .iter()
            .find(|release| {
                release.channel == ReleaseChannel::Prerelease
                    && release.version > *current_version
                    && !skipped_tags.contains(&release.tag_name)
            })
            .cloned()
    };

    AutomaticUpdateCandidates { prerelease, stable }
}

fn prompt_for_automatic_action(
    current_tag: &str,
    candidates: &AutomaticUpdateCandidates,
) -> anyhow::Result<PromptAction> {
    let mut options: Vec<(String, PromptAction)> = Vec::new();
    if let Some(prerelease) = candidates.prerelease.as_ref() {
        options.push((
            format!(
                "Update to latest prerelease ({}) [recommended]",
                prerelease.tag_name
            ),
            PromptAction::Update(prerelease.tag_name.clone()),
        ));
    }
    if let Some(stable) = candidates.stable.as_ref() {
        let label = if candidates.prerelease.is_none() {
            format!("Update to {} [recommended]", stable.tag_name)
        } else {
            format!("Update to latest stable ({})", stable.tag_name)
        };
        options.push((label, PromptAction::Update(stable.tag_name.clone())));
    }
    if let Some(prerelease) = candidates.prerelease.as_ref() {
        options.push((
            format!("Skip {}", prerelease.tag_name),
            PromptAction::Skip(prerelease.tag_name.clone()),
        ));
    }
    if let Some(stable) = candidates.stable.as_ref() {
        options.push((
            format!("Skip {}", stable.tag_name),
            PromptAction::Skip(stable.tag_name.clone()),
        ));
    }
    options.push((
        format!("Start current version ({current_tag})"),
        PromptAction::StartCurrent,
    ));

    println!("A newer standalone build is available.");
    println!("Current version: {current_tag}");
    for (index, (label, _)) in options.iter().enumerate() {
        println!("  {}. {}", index + 1, label);
    }

    loop {
        print!("Select an option [1-{}]: ", options.len());
        io::stdout().flush().context("failed to flush stdout")?;
        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .context("failed to read update selection")?;
        let trimmed = input.trim();
        let Ok(index) = trimmed.parse::<usize>() else {
            println!("Please enter a number between 1 and {}.", options.len());
            continue;
        };
        if !(1..=options.len()).contains(&index) {
            println!("Please enter a number between 1 and {}.", options.len());
            continue;
        }
        return Ok(options[index - 1].1.clone());
    }
}

fn prompt_yes_no(question: &str) -> anyhow::Result<bool> {
    loop {
        print!("{question} [y/N]: ");
        io::stdout().flush().context("failed to flush stdout")?;
        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .context("failed to read confirmation")?;
        match input.trim().to_ascii_lowercase().as_str() {
            "y" | "yes" => return Ok(true),
            "" | "n" | "no" => return Ok(false),
            _ => println!("Please answer with `y` or `n`."),
        }
    }
}

fn metadata_cache_is_fresh(state: &UpdaterState, ttl: Duration) -> bool {
    let Some(last_checked) = state.metadata_checked_at_unix_secs else {
        return false;
    };
    now_unix_secs().saturating_sub(last_checked) <= ttl.as_secs()
}

fn active_rate_limit_reset(state: &UpdaterState) -> Option<u64> {
    let reset_at = state.rate_limit_reset_unix_secs?;
    (reset_at > now_unix_secs()).then_some(reset_at)
}

fn expected_zip_asset_name(tag: &str) -> String {
    format!("ha3-standalone-x86_64-pc-windows-msvc-{tag}.zip")
}

fn parse_checksum_manifest(raw: &str) -> anyhow::Result<HashMap<String, String>> {
    let mut sums = HashMap::new();
    for (index, line) in raw.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some((hash, file_name)) = split_checksum_line(trimmed) else {
            bail!("invalid checksum manifest line {}: `{trimmed}`", index + 1);
        };
        sums.insert(file_name.to_string(), hash.to_ascii_lowercase());
    }
    Ok(sums)
}

fn split_checksum_line(line: &str) -> Option<(&str, &str)> {
    let mut parts = line.split_whitespace();
    let hash = parts.next()?;
    let file_name = parts.next()?;
    if parts.next().is_some() || hash.len() != 64 {
        return None;
    }
    Some((hash, file_name))
}

fn file_sha256_matches(path: &Path, expected_hash: &str) -> anyhow::Result<bool> {
    let actual = compute_file_sha256(path)?;
    Ok(actual.eq_ignore_ascii_case(expected_hash))
}

fn compute_file_sha256(path: &Path) -> anyhow::Result<String> {
    let mut file =
        fs::File::open(path).with_context(|| format!("failed to open {}", display_path(path)))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 16 * 1024];
    loop {
        let bytes_read = file
            .read(&mut buffer)
            .with_context(|| format!("failed to read {}", display_path(path)))?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn wait_for_process_exit(pid: u32, timeout: Duration) -> anyhow::Result<()> {
    // SAFETY: Windows handle API calls are used with values returned by the OS.
    unsafe {
        let handle = OpenProcess(PROCESS_SYNCHRONIZE, 0, pid);
        if handle.is_null() {
            return Ok(());
        }
        let timeout_ms = u32::try_from(timeout.as_millis()).unwrap_or(u32::MAX);
        let result = WaitForSingleObject(handle, timeout_ms);
        let _ = CloseHandle(handle);
        match result {
            WAIT_OBJECT_0 => Ok(()),
            WAIT_TIMEOUT => bail!("timed out while waiting for standalone process {pid} to stop"),
            other => {
                bail!("failed while waiting for standalone process {pid} to stop (code {other})")
            }
        }
    }
}

fn prepare_staging_dir(staging_dir: &Path, zip_path: &Path) -> anyhow::Result<()> {
    if staging_dir.exists() {
        fs::remove_dir_all(staging_dir).with_context(|| {
            format!(
                "failed to clear update staging directory {}",
                display_path(staging_dir)
            )
        })?;
    }
    fs::create_dir_all(staging_dir).with_context(|| {
        format!(
            "failed to create update staging directory {}",
            display_path(staging_dir)
        )
    })?;
    extract_zip_archive(zip_path, staging_dir)?;

    let staged_exe = staging_dir.join("ha3-standalone.exe");
    if !staged_exe.is_file() {
        bail!(
            "the downloaded package does not contain `ha3-standalone.exe` in {}",
            display_path(staging_dir)
        );
    }
    let staged_updater = staging_dir.join(STANDALONE_UPDATE_BINARY_NAME);
    if !staged_updater.is_file() {
        bail!(
            "the downloaded package does not contain `{}` in {}",
            STANDALONE_UPDATE_BINARY_NAME,
            display_path(staging_dir)
        );
    }
    Ok(())
}

fn extract_zip_archive(zip_path: &Path, destination: &Path) -> anyhow::Result<()> {
    let file = fs::File::open(zip_path)
        .with_context(|| format!("failed to open update archive {}", display_path(zip_path)))?;
    let mut archive = ZipArchive::new(file).with_context(|| {
        format!(
            "failed to read update archive structure {}",
            display_path(zip_path)
        )
    })?;

    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).with_context(|| {
            format!(
                "failed to read update archive entry {index} from {}",
                display_path(zip_path)
            )
        })?;
        let Some(enclosed_path) = entry.enclosed_name() else {
            continue;
        };
        let out_path = destination.join(enclosed_path);
        if entry.is_dir() {
            fs::create_dir_all(&out_path).with_context(|| {
                format!(
                    "failed to create extracted directory {}",
                    display_path(&out_path)
                )
            })?;
            continue;
        }

        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create extracted parent directory {}",
                    display_path(parent)
                )
            })?;
        }

        let mut output = fs::File::create(&out_path).with_context(|| {
            format!(
                "failed to create extracted file {}",
                display_path(&out_path)
            )
        })?;
        io::copy(&mut entry, &mut output).with_context(|| {
            format!(
                "failed to extract archive entry into {}",
                display_path(&out_path)
            )
        })?;
    }
    Ok(())
}

fn apply_staged_update(install_dir: &Path, staging_dir: &Path) -> anyhow::Result<()> {
    remove_obsolete_root_dlls(install_dir, staging_dir)?;
    copy_root_file(
        &staging_dir.join("ha3-standalone.exe"),
        &install_dir.join("ha3-standalone.exe"),
    )?;
    copy_root_file(
        &staging_dir.join(STANDALONE_UPDATE_BINARY_NAME),
        &install_dir.join(STANDALONE_UPDATE_BINARY_NAME),
    )?;
    copy_all_matching_root_files(staging_dir, install_dir, "dll")?;
    replace_managed_directory(&staging_dir.join("frontend"), install_dir, "frontend")?;
    let assets_parent = install_dir.join("assets");
    replace_managed_directory(
        &staging_dir.join("assets").join("tracks"),
        &assets_parent,
        "tracks",
    )?;
    replace_managed_directory(
        &staging_dir.join("assets").join("bolids"),
        &assets_parent,
        "bolids",
    )?;

    let package_config = staging_dir.join("standalone.toml");
    let target_config = install_dir.join("standalone.toml");
    if package_config.is_file() && !target_config.exists() {
        copy_root_file(&package_config, &target_config)?;
    }
    Ok(())
}

fn remove_obsolete_root_dlls(install_dir: &Path, staging_dir: &Path) -> anyhow::Result<()> {
    let new_dlls = root_file_names_with_extension(staging_dir, "dll")?;
    for existing in root_file_names_with_extension(install_dir, "dll")? {
        if !new_dlls.contains(&existing) {
            let path = install_dir.join(&existing);
            fs::remove_file(&path).with_context(|| {
                format!("failed to remove obsolete DLL {}", display_path(&path))
            })?;
        }
    }
    Ok(())
}

fn root_file_names_with_extension(dir: &Path, extension: &str) -> anyhow::Result<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    if !dir.is_dir() {
        return Ok(names);
    }
    for entry in fs::read_dir(dir)
        .with_context(|| format!("failed to read directory {}", display_path(dir)))?
    {
        let entry =
            entry.with_context(|| format!("failed to iterate directory {}", display_path(dir)))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|value| value.to_str()) != Some(extension) {
            continue;
        }
        if let Some(name) = path.file_name().and_then(|value| value.to_str()) {
            names.insert(name.to_string());
        }
    }
    Ok(names)
}

fn copy_all_matching_root_files(
    source_dir: &Path,
    target_dir: &Path,
    extension: &str,
) -> anyhow::Result<()> {
    if !source_dir.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(source_dir)
        .with_context(|| format!("failed to read directory {}", display_path(source_dir)))?
    {
        let entry = entry
            .with_context(|| format!("failed to iterate directory {}", display_path(source_dir)))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|value| value.to_str()) != Some(extension) {
            continue;
        }
        let target = target_dir.join(entry.file_name());
        copy_root_file(&path, &target)?;
    }
    Ok(())
}

fn copy_root_file(source: &Path, target: &Path) -> anyhow::Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create target parent directory {}",
                display_path(parent)
            )
        })?;
    }
    fs::copy(source, target).with_context(|| {
        format!(
            "failed to copy {} to {}",
            display_path(source),
            display_path(target)
        )
    })?;
    Ok(())
}

fn replace_managed_directory(
    source: &Path,
    target_parent: &Path,
    target_name: &str,
) -> anyhow::Result<()> {
    if !source.exists() {
        return Ok(());
    }
    let target = target_parent.join(target_name);
    if target.exists() {
        fs::remove_dir_all(&target).with_context(|| {
            format!(
                "failed to remove managed directory {}",
                display_path(&target)
            )
        })?;
    }
    fs::create_dir_all(target_parent).with_context(|| {
        format!(
            "failed to create target parent directory {}",
            display_path(target_parent)
        )
    })?;
    copy_directory_recursive(source, &target)
}

fn copy_directory_recursive(source: &Path, target: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(target)
        .with_context(|| format!("failed to create directory {}", display_path(target)))?;
    for entry in fs::read_dir(source)
        .with_context(|| format!("failed to read directory {}", display_path(source)))?
    {
        let entry = entry
            .with_context(|| format!("failed to iterate directory {}", display_path(source)))?;
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());
        if source_path.is_dir() {
            copy_directory_recursive(&source_path, &target_path)?;
        } else {
            copy_root_file(&source_path, &target_path)?;
        }
    }
    Ok(())
}

fn relaunch_standalone(install_dir: &Path) -> anyhow::Result<()> {
    let standalone_path = install_dir.join("ha3-standalone.exe");
    Command::new(&standalone_path)
        .current_dir(install_dir)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| {
            format!(
                "failed to relaunch standalone executable {}",
                display_path(&standalone_path)
            )
        })?;
    Ok(())
}

fn sanitize_path_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '-' | '_' => ch,
            _ => '_',
        })
        .collect()
}

fn authorize_apply_request_from_env(args: &ApplyUpdateArgs) -> anyhow::Result<()> {
    let cache_root = updater_cache_root_from_env()?;
    authorize_apply_request(args, &cache_root)
}

fn authorize_apply_request(args: &ApplyUpdateArgs, cache_root: &Path) -> anyhow::Result<()> {
    validate_apply_args_paths(args, cache_root)?;
    let path = apply_authorization_path(cache_root, &args.auth_token);
    if !path.is_file() {
        bail!(
            "missing apply authorization for this update request; start updates from ha3-standalone.exe"
        );
    }
    let raw = fs::read_to_string(&path).with_context(|| {
        format!(
            "failed to read apply authorization file {}",
            display_path(&path)
        )
    })?;
    let authorization: ApplyAuthorization = serde_json::from_str(&raw).with_context(|| {
        format!(
            "failed to decode apply authorization file {}",
            display_path(&path)
        )
    })?;
    if authorization.install_dir != args.install_dir
        || authorization.zip_path != args.zip_path
        || authorization.staging_dir != args.staging_dir
        || authorization.tag != args.tag
    {
        bail!("apply authorization does not match the requested update");
    }
    fs::remove_file(&path).with_context(|| {
        format!(
            "failed to consume apply authorization file {}",
            display_path(&path)
        )
    })?;
    Ok(())
}

fn validate_apply_args_paths(args: &ApplyUpdateArgs, cache_root: &Path) -> anyhow::Result<()> {
    if !args.install_dir.is_absolute() {
        bail!("`--install-dir` must be an absolute path");
    }
    if !args.zip_path.is_absolute() {
        bail!("`--zip-path` must be an absolute path");
    }
    if !args.staging_dir.is_absolute() {
        bail!("`--staging-dir` must be an absolute path");
    }
    let downloads_root = cache_root.join("downloads");
    let staging_root = cache_root.join("staging");
    if !args.zip_path.starts_with(&downloads_root) {
        bail!(
            "`--zip-path` must point inside the updater download cache at {}",
            display_path(&downloads_root)
        );
    }
    if !args.staging_dir.starts_with(&staging_root) {
        bail!(
            "`--staging-dir` must point inside the updater staging cache at {}",
            display_path(&staging_root)
        );
    }
    Ok(())
}

fn updater_cache_root_from_env() -> anyhow::Result<PathBuf> {
    let local_app_data = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("LOCALAPPDATA is not set"))?;
    Ok(local_app_data
        .join("HackArena")
        .join("3_0")
        .join("standalone")
        .join("update-cache"))
}

fn apply_authorization_path(cache_root: &Path, token: &str) -> PathBuf {
    cache_root
        .join("pending-apply")
        .join(format!("{}.json", sanitize_path_component(token)))
}

fn is_rate_limited_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS
    )
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn display_path(path: &Path) -> String {
    path.display().to_string().replace("\\\\?\\", "")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn release(tag: &str, prerelease: bool) -> CachedRelease {
        CachedRelease {
            tag_name: tag.to_string(),
            prerelease,
            assets: vec![],
        }
    }

    fn apply_args(cache_root: &Path) -> ApplyUpdateArgs {
        ApplyUpdateArgs {
            old_pid: 123,
            install_dir: PathBuf::from(r"C:\HackArena\standalone"),
            zip_path: cache_root
                .join("downloads")
                .join("v0.2.0-beta.9")
                .join("ha3-standalone-x86_64-pc-windows-msvc-v0.2.0-beta.9.zip"),
            staging_dir: cache_root.join("staging").join("v0.2.0-beta.9"),
            tag: "v0.2.0-beta.9".to_string(),
            auth_token: "test-token".to_string(),
        }
    }

    #[test]
    fn parses_update_to_argument_with_optional_v_prefix() {
        let parsed = parse_startup_args_from_iter([
            "ha3-standalone.exe",
            "--update-to",
            "0.2.0-beta.9",
            "--ignore-update-cache",
        ])
        .expect("args should parse");
        assert_eq!(parsed.update_to.as_deref(), Some("v0.2.0-beta.9"));
        assert!(parsed.ignore_update_cache);
    }

    #[test]
    fn parses_apply_update_args_with_auth_token() {
        let parsed = parse_apply_update_args_from_iter([
            "ha3-standalone-update.exe",
            "apply",
            "--old-pid",
            "123",
            "--install-dir",
            r"C:\HackArena\standalone",
            "--zip-path",
            r"C:\Users\test\AppData\Local\HackArena\3_0\standalone\update-cache\downloads\v0.2.0-beta.9\ha3-standalone-x86_64-pc-windows-msvc-v0.2.0-beta.9.zip",
            "--staging-dir",
            r"C:\Users\test\AppData\Local\HackArena\3_0\standalone\update-cache\staging\v0.2.0-beta.9",
            "--tag",
            "0.2.0-beta.9",
            "--auth-token",
            "abc123",
        ])
        .expect("apply args should parse");
        assert_eq!(parsed.tag, "v0.2.0-beta.9");
        assert_eq!(parsed.auth_token, "abc123");
    }

    #[test]
    fn selects_only_stable_for_stable_current_version() {
        let current = Version::parse("0.2.0").expect("valid current version");
        let releases = parse_release_entries(&[
            release("v0.2.1-beta.1", true),
            release("v0.2.1", false),
            release("v0.2.0-beta.9", true),
        ]);
        let candidates = select_automatic_candidates(&current, &releases, &BTreeSet::new());
        assert!(candidates.prerelease.is_none());
        assert_eq!(
            candidates
                .stable
                .as_ref()
                .map(|entry| entry.tag_name.as_str()),
            Some("v0.2.1")
        );
    }

    #[test]
    fn selects_prerelease_and_stable_for_prerelease_current_version() {
        let current = Version::parse("0.2.0-beta.8").expect("valid current version");
        let releases = parse_release_entries(&[
            release("v0.2.0-beta.9", true),
            release("v0.2.0", false),
            release("v0.1.0", false),
        ]);
        let candidates = select_automatic_candidates(&current, &releases, &BTreeSet::new());
        assert_eq!(
            candidates
                .prerelease
                .as_ref()
                .map(|entry| entry.tag_name.as_str()),
            Some("v0.2.0-beta.9")
        );
        assert_eq!(
            candidates
                .stable
                .as_ref()
                .map(|entry| entry.tag_name.as_str()),
            Some("v0.2.0")
        );
    }

    #[test]
    fn skip_state_is_exact_tag_based() {
        let current = Version::parse("0.2.0-beta.8").expect("valid current version");
        let releases =
            parse_release_entries(&[release("v0.2.0-beta.9", true), release("v0.2.0", false)]);
        let skipped_tags = BTreeSet::from(["v0.2.0-beta.9".to_string()]);
        let candidates = select_automatic_candidates(&current, &releases, &skipped_tags);
        assert!(candidates.prerelease.is_none());
        assert_eq!(
            candidates
                .stable
                .as_ref()
                .map(|entry| entry.tag_name.as_str()),
            Some("v0.2.0")
        );
    }

    #[test]
    fn ignores_invalid_or_too_old_tags() {
        let releases = parse_release_entries(&[
            release("not-a-version", false),
            release("v0.2.0-beta.8", true),
            release("v0.2.0-beta.9", true),
        ]);
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].tag_name, "v0.2.0-beta.9");
    }

    #[test]
    fn parses_sha256sums_manifest() {
        let manifest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef  ha3-standalone.zip\r\n";
        let parsed = parse_checksum_manifest(manifest).expect("manifest should parse");
        assert_eq!(
            parsed.get("ha3-standalone.zip").map(String::as_str),
            Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        );
    }

    #[test]
    fn metadata_freshness_uses_ttl_seconds() {
        let state = UpdaterState {
            metadata_checked_at_unix_secs: Some(now_unix_secs().saturating_sub(60)),
            ..UpdaterState::default()
        };
        assert!(metadata_cache_is_fresh(&state, Duration::from_secs(120)));
        assert!(!metadata_cache_is_fresh(&state, Duration::from_secs(30)));
    }

    #[test]
    fn apply_authorization_consumes_matching_request() {
        let temp = TempDir::new().expect("temp dir");
        let cache_root = temp.path().join("update-cache");
        let args = apply_args(&cache_root);
        fs::create_dir_all(args.zip_path.parent().expect("zip parent")).expect("downloads dir");
        fs::create_dir_all(&args.staging_dir).expect("staging dir");
        fs::create_dir_all(cache_root.join("pending-apply")).expect("auth dir");
        let auth_path = apply_authorization_path(&cache_root, &args.auth_token);
        let auth = ApplyAuthorization {
            install_dir: args.install_dir.clone(),
            zip_path: args.zip_path.clone(),
            staging_dir: args.staging_dir.clone(),
            tag: args.tag.clone(),
            created_at_unix_secs: now_unix_secs(),
        };
        fs::write(
            &auth_path,
            serde_json::to_string(&auth).expect("authorization json"),
        )
        .expect("authorization file");

        authorize_apply_request(&args, &cache_root).expect("authorization should validate");
        assert!(!auth_path.exists(), "authorization should be single-use");
    }

    #[test]
    fn apply_authorization_rejects_missing_token_file() {
        let temp = TempDir::new().expect("temp dir");
        let cache_root = temp.path().join("update-cache");
        let args = apply_args(&cache_root);
        fs::create_dir_all(args.zip_path.parent().expect("zip parent")).expect("downloads dir");
        fs::create_dir_all(&args.staging_dir).expect("staging dir");
        let err =
            authorize_apply_request(&args, &cache_root).expect_err("missing auth file should fail");
        assert!(
            err.to_string().contains("missing apply authorization"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn apply_authorization_rejects_mismatched_request() {
        let temp = TempDir::new().expect("temp dir");
        let cache_root = temp.path().join("update-cache");
        let args = apply_args(&cache_root);
        fs::create_dir_all(args.zip_path.parent().expect("zip parent")).expect("downloads dir");
        fs::create_dir_all(&args.staging_dir).expect("staging dir");
        fs::create_dir_all(cache_root.join("pending-apply")).expect("auth dir");
        let auth_path = apply_authorization_path(&cache_root, &args.auth_token);
        let auth = ApplyAuthorization {
            install_dir: args.install_dir.clone(),
            zip_path: args.zip_path.clone(),
            staging_dir: args.staging_dir.clone(),
            tag: "v0.2.0".to_string(),
            created_at_unix_secs: now_unix_secs(),
        };
        fs::write(
            &auth_path,
            serde_json::to_string(&auth).expect("authorization json"),
        )
        .expect("authorization file");

        let err =
            authorize_apply_request(&args, &cache_root).expect_err("mismatched auth should fail");
        assert!(
            err.to_string()
                .contains("apply authorization does not match the requested update"),
            "unexpected error: {err:#}"
        );
        assert!(
            auth_path.exists(),
            "failed authorization should not consume token"
        );
    }

    #[test]
    fn apply_authorization_rejects_zip_path_outside_cache() {
        let temp = TempDir::new().expect("temp dir");
        let cache_root = temp.path().join("update-cache");
        let mut args = apply_args(&cache_root);
        args.zip_path = temp.path().join("outside.zip");
        let err = authorize_apply_request(&args, &cache_root)
            .expect_err("zip path outside cache should fail");
        assert!(
            err.to_string()
                .contains("`--zip-path` must point inside the updater download cache"),
            "unexpected error: {err:#}"
        );
    }
}
