//! `strata update` — channel-aware self-update (#3038).
//!
//! Updates the curl-installed binary in place: resolve the target release,
//! download the target-triple tarball and `checksums-sha256.txt`, verify the
//! SHA-256 **before** touching anything, then atomically replace the running
//! binary. A Homebrew-managed binary is redirected to `brew upgrade` (it belongs
//! to the tap, exactly as `uninstall` defers to `brew uninstall`).
//!
//! Downloads shell out to the same tools `install.sh` uses (`curl`, `tar`) so the
//! CLI carries no HTTP/TLS stack; the machine already has them, since the binary
//! was curl-installed. The checksum is computed in-process (`sha2`), so a
//! tampered download is caught without trusting an external tool. Explicit-invoke
//! only — there is no update-on-startup.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::uninstall::is_homebrew_install;
use crate::CliError;

const REPO: &str = "stratalab/strata-core";
const CURRENT: &str = env!("CARGO_PKG_VERSION");

pub(crate) fn run_update(
    check_only: bool,
    target_version: Option<String>,
) -> Result<Value, CliError> {
    let exe = std::env::current_exe().map_err(|error| {
        CliError::usage(format!("could not locate the running binary: {error}"))
    })?;
    if is_homebrew_install(&exe) {
        return Err(CliError::usage(
            "this strata binary is managed by Homebrew; run `brew upgrade strata` instead",
        ));
    }
    let triple = target_triple()?;

    // The version we would move to: an explicit --version, else the latest release.
    let explicit = target_version.is_some();
    let wanted = match target_version {
        Some(v) => v.trim_start_matches('v').to_owned(),
        None => resolve_latest_version()?,
    };
    let up_to_date = is_up_to_date(&wanted, CURRENT);

    match decide(check_only, up_to_date, explicit) {
        Action::Report => {
            // What happened is the answer, and `render_report` prints it on
            // stdout from this record (#3339) — not a sentence beside it.
            return Ok(json!({
                "type": "update",
                "data": { "current": CURRENT, "latest": wanted, "update_available": !up_to_date, "changed": false }
            }));
        }
        Action::AlreadyCurrent => {
            return Ok(json!({
                "type": "update",
                "data": { "current": CURRENT, "latest": wanted, "update_available": false, "changed": false }
            }));
        }
        Action::Install => {}
    }

    install_release_asset(&exe, &wanted, &asset_name(&wanted, triple), triple)?;

    Ok(json!({
        "type": "update",
        "data": { "current": CURRENT, "latest": wanted, "update_available": false, "changed": true }
    }))
}

/// The `-local` variant's asset name for a release.
///
/// The release publishes two builds per target: the lean default and this one,
/// which carries the vendored llama.cpp needed to execute GGUF models.
fn local_asset_name(version: &str, triple: &str) -> String {
    format!("strata-v{version}-{triple}-local.tar.gz")
}

/// Whether this binary already executes local models.
const fn has_local_execution() -> bool {
    cfg!(feature = "inference-local")
}

/// `strata inference install-local` (D2) — swap the running binary for the
/// same version's local-capable build.
///
/// The alternative was telling people to `cargo install --features
/// inference-local`, which needs a Rust toolchain. The callers of this surface
/// are mostly coding agents, which have none — so a source build is not a
/// remediation for them, it is a dead end that looks like one.
///
/// It installs the **same version** it is running, not the latest: adding a
/// capability should not also move you across releases. `strata update` is
/// still how you change version.
///
/// Idempotent on purpose. An agent that cannot tell whether it already ran this
/// can run it again and get a clean no-op rather than a redundant download.
pub(crate) fn run_install_local() -> Result<Value, CliError> {
    let exe = std::env::current_exe().map_err(|error| {
        CliError::usage(format!("could not locate the running binary: {error}"))
    })?;

    if has_local_execution() {
        return Ok(json!({
            "type": "inference_install_local",
            "data": { "version": CURRENT, "local_execution": true, "changed": false }
        }));
    }
    if is_homebrew_install(&exe) {
        return Err(CliError::usage(
            "this strata binary is managed by Homebrew; run \
             `brew install stratalab/tap/strata-local` instead",
        ));
    }

    let triple = target_triple()?;
    let asset = local_asset_name(CURRENT, triple);
    install_release_asset(&exe, CURRENT, &asset, triple)?;

    Ok(json!({
        "type": "inference_install_local",
        "data": { "version": CURRENT, "local_execution": true, "changed": true }
    }))
}

/// Fetch one release asset, verify its SHA-256, and atomically replace the
/// running binary with the `strata` inside it.
///
/// Shared by `update` (same build, newer version) and `inference install-local`
/// (same version, local-capable build) so both get the identical guarantee:
/// **nothing is touched until the checksum matches.** A tampered or truncated
/// download cannot reach the binary.
fn install_release_asset(
    exe: &Path,
    version: &str,
    asset: &str,
    triple: &str,
) -> Result<(), CliError> {
    let scratch = TempDir::new()?;
    let base = format!("https://github.com/{REPO}/releases/download/v{version}");
    let tarball = scratch.path().join(asset);
    let sums = scratch.path().join("checksums-sha256.txt");
    eprintln!("downloading {asset} ({triple}) ...");
    download(&format!("{base}/{asset}"), &tarball)?;
    download(&format!("{base}/checksums-sha256.txt"), &sums)?;

    let sums_text = std::fs::read_to_string(&sums)
        .map_err(|e| CliError::usage(format!("could not read checksums: {e}")))?;
    let expected = expected_sha(&sums_text, asset).ok_or_else(|| {
        CliError::usage(format!(
            "release {version} has no checksum entry for {asset}"
        ))
    })?;
    let got = sha256_file(&tarball)?;
    if !got.eq_ignore_ascii_case(expected) {
        return Err(CliError::usage(format!(
            "checksum mismatch for {asset} — refusing to install (expected {expected}, got {got})"
        )));
    }

    extract(&tarball, scratch.path())?;
    let staged = scratch.path().join("strata");
    if !staged.exists() {
        return Err(CliError::usage(
            "downloaded archive did not contain a `strata` binary",
        ));
    }
    replace_binary(exe, &staged)
}

/// The release asset triple for the host, or an error on an unsupported target.
/// The release ships exactly three: the two Linux glibc targets and Apple aarch64.
fn target_triple() -> Result<&'static str, CliError> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Ok("x86_64-unknown-linux-gnu"),
        ("linux", "aarch64") => Ok("aarch64-unknown-linux-gnu"),
        ("macos", "aarch64") => Ok("aarch64-apple-darwin"),
        (os, arch) => Err(CliError::usage(format!(
            "`strata update` has no release build for {os}/{arch}; reinstall from https://stratadb.org/install.sh"
        ))),
    }
}

fn asset_name(version: &str, triple: &str) -> String {
    format!("strata-v{version}-{triple}.tar.gz")
}

/// What `run_update` should do, decided from the flags alone (pure).
#[derive(Debug, PartialEq, Eq)]
enum Action {
    /// `--check`: report status, install nothing.
    Report,
    /// No `--check`, already current, and not an explicit version: nothing to do.
    AlreadyCurrent,
    /// Download and replace (behind, or an explicit `--version` re/install).
    Install,
}

fn decide(check_only: bool, up_to_date: bool, explicit: bool) -> Action {
    if check_only {
        Action::Report
    } else if up_to_date && !explicit {
        Action::AlreadyCurrent
    } else {
        Action::Install
    }
}

/// Whether the installed `current` is already at least `wanted`.
fn is_up_to_date(wanted: &str, current: &str) -> bool {
    !is_newer(wanted, current)
}

/// A host command (update/uninstall) rejects a database target; the three flags
/// are the CLI's `--db`, `--db-path`, and `--cache`.
pub(crate) fn rejects_db_target(has_db: bool, has_db_path: bool, cache: bool) -> bool {
    has_db || has_db_path || cache
}

/// The process exit code for `--check`: nonzero exactly when an update is
/// available, so `if ! strata update --check` gates on it.
pub(crate) fn check_exit_code(check_only: bool, update_available: bool) -> i32 {
    i32::from(check_only && update_available)
}

/// Parse an `X.Y.Z` version, ignoring any pre-release/build suffix.
fn parse_version(s: &str) -> Option<(u64, u64, u64)> {
    let core = s.trim_start_matches('v');
    let core = core.split(['-', '+']).next().unwrap_or(core);
    let mut it = core.split('.');
    let a = it.next()?.parse().ok()?;
    let b = it.next()?.parse().ok()?;
    let c = it.next()?.parse().ok()?;
    if it.next().is_some() {
        return None;
    }
    Some((a, b, c))
}

/// Whether `candidate` is a strictly newer version than `current`. An unparseable
/// candidate is treated as newer (an explicit user request we don't second-guess).
fn is_newer(candidate: &str, current: &str) -> bool {
    match (parse_version(candidate), parse_version(current)) {
        (Some(new), Some(cur)) => new > cur,
        _ => true,
    }
}

/// The SHA-256 for `asset` from a `checksums-sha256.txt` (`<sha>␠␠<name>` lines).
fn expected_sha<'a>(checksums: &'a str, asset: &str) -> Option<&'a str> {
    checksums.lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        let sha = parts.next()?;
        let name = parts.next()?;
        (name == asset).then_some(sha)
    })
}

fn sha256_file(path: &Path) -> Result<String, CliError> {
    let bytes = std::fs::read(path)
        .map_err(|e| CliError::usage(format!("could not read download: {e}")))?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(hex(&hasher.finalize()))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn resolve_latest_version() -> Result<String, CliError> {
    let url = format!("https://api.github.com/repos/{REPO}/releases/latest");
    let out = Command::new("curl")
        .args(["-fsSL", "-A", "strata-update", &url])
        .output()
        .map_err(|e| CliError::usage(format!("could not run curl: {e}")))?;
    if !out.status.success() {
        return Err(CliError::usage(
            "could not reach the release API to resolve the latest version",
        ));
    }
    let body: Value = serde_json::from_slice(&out.stdout)
        .map_err(|e| CliError::usage(format!("could not parse the release API response: {e}")))?;
    let tag = body
        .get("tag_name")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::usage("the release API response had no tag_name"))?;
    Ok(tag.trim_start_matches('v').to_owned())
}

fn download(url: &str, dest: &Path) -> Result<(), CliError> {
    let status = Command::new("curl")
        .args(["-fSL", "--proto", "=https", "-o"])
        .arg(dest)
        .arg(url)
        .status()
        .map_err(|e| CliError::usage(format!("could not run curl: {e}")))?;
    if !status.success() {
        return Err(CliError::usage(format!("download failed: {url}")));
    }
    Ok(())
}

fn extract(tarball: &Path, into: &Path) -> Result<(), CliError> {
    let status = Command::new("tar")
        .arg("xzf")
        .arg(tarball)
        .arg("-C")
        .arg(into)
        .status()
        .map_err(|e| CliError::usage(format!("could not run tar: {e}")))?;
    if !status.success() {
        return Err(CliError::usage("failed to extract the downloaded archive"));
    }
    Ok(())
}

/// Atomically replace `current` with `staged`: copy into a temp file in the SAME
/// directory (so the rename is atomic on one filesystem), make it executable, and
/// rename over the running binary. Unix permits renaming over a live executable.
fn replace_binary(current: &Path, staged: &Path) -> Result<(), CliError> {
    let dir = current.parent().ok_or_else(|| {
        CliError::usage("could not determine the installation directory of the running binary")
    })?;
    let tmp = dir.join(".strata.update.tmp");
    std::fs::copy(staged, &tmp).map_err(|e| {
        CliError::usage(format!(
            "cannot write to {} ({e}) — is this a system install? reinstall via https://stratadb.org/install.sh",
            dir.display()
        ))
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))
            .map_err(|e| CliError::usage(format!("could not set permissions: {e}")))?;
    }
    std::fs::rename(&tmp, current).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        CliError::usage(format!("could not replace the running binary: {e}"))
    })?;
    Ok(())
}

/// A scratch directory removed on drop (best-effort).
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Result<Self, CliError> {
        let base = std::env::temp_dir().join(format!("strata-update-{}", std::process::id()));
        std::fs::create_dir_all(&base)
            .map_err(|e| CliError::usage(format!("could not create a scratch directory: {e}")))?;
        Ok(Self(base))
    }
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        asset_name, check_exit_code, decide, expected_sha, has_local_execution, hex, is_newer,
        is_up_to_date, local_asset_name, parse_version, rejects_db_target, sha256_file,
        target_triple, Action, CURRENT,
    };
    use std::io::Write as _;

    #[test]
    fn is_up_to_date_is_the_inverse_of_newer() {
        assert!(is_up_to_date("1.1.1", "1.1.1"));
        assert!(is_up_to_date("1.0.0", "1.1.1")); // asking for older → already current
        assert!(!is_up_to_date("2.0.0", "1.1.1")); // newer available → not current
    }

    #[test]
    fn rejects_db_target_on_any_target_flag() {
        assert!(rejects_db_target(true, false, false));
        assert!(rejects_db_target(false, true, false));
        assert!(rejects_db_target(false, false, true));
        assert!(!rejects_db_target(false, false, false));
    }

    #[test]
    fn check_exit_code_is_nonzero_only_for_an_available_update_under_check() {
        assert_eq!(check_exit_code(true, true), 1);
        assert_eq!(check_exit_code(true, false), 0);
        assert_eq!(check_exit_code(false, true), 0); // a real update always exits 0
    }

    #[test]
    fn target_triple_maps_the_supported_hosts() {
        let got = target_triple();
        match (std::env::consts::OS, std::env::consts::ARCH) {
            ("linux", "x86_64") => assert_eq!(got.unwrap(), "x86_64-unknown-linux-gnu"),
            ("linux", "aarch64") => assert_eq!(got.unwrap(), "aarch64-unknown-linux-gnu"),
            ("macos", "aarch64") => assert_eq!(got.unwrap(), "aarch64-apple-darwin"),
            _ => assert!(got.is_err()),
        }
    }

    #[test]
    fn hex_encodes_lowercase_two_digits_per_byte() {
        assert_eq!(hex(&[0xab, 0xcd, 0x01]), "abcd01");
        assert_eq!(hex(&[]), "");
    }

    #[test]
    fn sha256_file_matches_the_known_digest() {
        let mut f = tempfile::NamedTempFile::new().expect("temp file");
        f.write_all(b"abc").expect("write");
        assert_eq!(
            sha256_file(f.path()).expect("hash"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn decide_covers_the_flag_truth_table() {
        // --check always reports, regardless of state.
        assert_eq!(decide(true, true, false), Action::Report);
        assert_eq!(decide(true, false, false), Action::Report);
        assert_eq!(decide(true, true, true), Action::Report);
        // Not --check, up to date, no explicit version → nothing to do.
        assert_eq!(decide(false, true, false), Action::AlreadyCurrent);
        // Not --check, behind → install.
        assert_eq!(decide(false, false, false), Action::Install);
        // An explicit --version installs even when "up to date" (reinstall/rollback).
        assert_eq!(decide(false, true, true), Action::Install);
    }

    #[test]
    fn version_parsing_ignores_v_prefix_and_suffix() {
        assert_eq!(parse_version("1.2.0"), Some((1, 2, 0)));
        assert_eq!(parse_version("v1.2.0"), Some((1, 2, 0)));
        assert_eq!(parse_version("1.2.0-rc1"), Some((1, 2, 0)));
        assert_eq!(parse_version("1.2"), None);
        assert_eq!(parse_version("1.2.0.1"), None);
        assert_eq!(parse_version("nonsense"), None);
    }

    #[test]
    fn newer_compares_numerically_and_defaults_to_installing_on_garbage() {
        assert!(is_newer("1.2.0", "1.1.1"));
        assert!(is_newer("1.10.0", "1.9.9")); // numeric, not lexical
        assert!(!is_newer("1.1.1", "1.1.1"));
        assert!(!is_newer("1.1.0", "1.1.1"));
        // An unparseable explicit target is honored (treated as newer).
        assert!(is_newer("weird-tag", "1.1.1"));
    }

    #[test]
    fn asset_name_matches_the_release_convention() {
        assert_eq!(
            asset_name("1.2.0", "aarch64-apple-darwin"),
            "strata-v1.2.0-aarch64-apple-darwin.tar.gz"
        );
    }

    #[test]
    fn expected_sha_finds_the_matching_asset_line() {
        let sums = "\
abc123  strata-v1.2.0-x86_64-unknown-linux-gnu.tar.gz
def456  strata-v1.2.0-aarch64-apple-darwin.tar.gz
";
        assert_eq!(
            expected_sha(sums, "strata-v1.2.0-aarch64-apple-darwin.tar.gz"),
            Some("def456")
        );
        assert_eq!(expected_sha(sums, "strata-v1.2.0-nope.tar.gz"), None);
    }

    /// D2: the asset `install-local` downloads is the default asset's name plus
    /// `-local`, which is exactly what the release matrix publishes. If these
    /// two ever disagree the command fails at the checksum step with "no
    /// checksum entry", which is a confusing way to learn about a typo.
    #[test]
    fn the_local_asset_is_the_default_asset_plus_a_suffix() {
        let triple = "x86_64-unknown-linux-gnu";
        assert_eq!(
            local_asset_name("1.2.1", triple),
            "strata-v1.2.1-x86_64-unknown-linux-gnu-local.tar.gz"
        );
        assert_eq!(
            local_asset_name("1.2.1", triple),
            asset_name("1.2.1", triple).replace(".tar.gz", "-local.tar.gz"),
            "the release workflow appends `-local` to the same stem"
        );
    }

    /// `install-local` fetches the version it is running, not the latest:
    /// adding a capability must not also move you across releases.
    #[test]
    fn the_local_asset_is_pinned_to_the_running_version() {
        let triple = "x86_64-unknown-linux-gnu";
        assert!(
            local_asset_name(CURRENT, triple).contains(CURRENT),
            "install-local installs its own version, leaving `update` to change it"
        );
    }

    /// The lean build is the one that needs `install-local`; the local build
    /// short-circuits to a no-op, which is what makes the command safe for an
    /// agent to run without first checking.
    #[test]
    fn only_a_build_without_local_execution_needs_installing() {
        assert_eq!(has_local_execution(), cfg!(feature = "inference-local"));
    }

    /// Every checksum entry the local asset needs resolves the same way the
    /// default one does — the shared verify path treats them identically.
    #[test]
    fn checksums_resolve_for_both_variants() {
        let sums = "abc123  strata-v1.2.1-x86_64-unknown-linux-gnu.tar.gz\n\
                    def456  strata-v1.2.1-x86_64-unknown-linux-gnu-local.tar.gz\n";
        assert_eq!(
            expected_sha(sums, "strata-v1.2.1-x86_64-unknown-linux-gnu.tar.gz"),
            Some("abc123")
        );
        assert_eq!(
            expected_sha(sums, "strata-v1.2.1-x86_64-unknown-linux-gnu-local.tar.gz"),
            Some("def456")
        );
    }
}
