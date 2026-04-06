//! Interactive first-run wizard for `strata init`.
//!
//! Detects hardware, selects a profile, creates a database with appropriate
//! config, offers model downloads, seeds sample data, and returns a ready
//! Strata handle for the REPL.

use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use strata_executor::Strata;

// =========================================================================
// Hardware detection — engine provides RAM/cores/Profile; CLI adds storage kind
// =========================================================================

use strata_executor::{
    apply_profile_if_defaults, detect_hardware as engine_detect_hardware, HardwareInfo, Profile,
};

#[derive(Debug, Clone, Copy)]
enum StorageKind {
    Nvme,
    Ssd,
    Hdd,
    Unknown,
}

impl std::fmt::Display for StorageKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageKind::Nvme => write!(f, "NVMe SSD"),
            StorageKind::Ssd => write!(f, "SSD"),
            StorageKind::Hdd => write!(f, "HDD"),
            StorageKind::Unknown => write!(f, "unknown storage"),
        }
    }
}

fn detect_storage() -> StorageKind {
    #[cfg(target_os = "linux")]
    {
        // Check common block devices for NVMe or rotational status
        if let Ok(entries) = std::fs::read_dir("/sys/block") {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("nvme") {
                    return StorageKind::Nvme;
                }
            }
            // Check rotational flag on first sd/vd device
            for entry in std::fs::read_dir("/sys/block")
                .into_iter()
                .flatten()
                .flatten()
            {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("sd") || name.starts_with("vd") {
                    let rotational = entry.path().join("queue/rotational");
                    if let Ok(val) = std::fs::read_to_string(rotational) {
                        return if val.trim() == "0" {
                            StorageKind::Ssd
                        } else {
                            StorageKind::Hdd
                        };
                    }
                }
            }
        }
    }

    #[cfg(target_os = "macos")]
    {
        // All modern Macs use SSDs
        return StorageKind::Ssd;
    }

    #[allow(unreachable_code)]
    StorageKind::Unknown
}

// =========================================================================
// Config generation — delegates to engine
// =========================================================================

fn build_config(profile: Profile, hw: HardwareInfo) -> strata_executor::StrataConfig {
    let mut cfg = strata_executor::StrataConfig::default();
    apply_profile_if_defaults(&mut cfg, profile, hw);
    cfg
}

// =========================================================================
// Interactive prompts
// =========================================================================

#[allow(dead_code)] // Used by embed feature gate
fn prompt_yes_no(question: &str, default_yes: bool) -> bool {
    let hint = if default_yes { "[Y/n]" } else { "[y/N]" };
    eprint!("  {} {} ", question, hint);
    io::stderr().flush().unwrap();
    let mut answer = String::new();
    if io::stdin().read_line(&mut answer).is_err() {
        return default_yes;
    }
    let trimmed = answer.trim();
    if trimmed.is_empty() {
        return default_yes;
    }
    trimmed.eq_ignore_ascii_case("y") || trimmed.eq_ignore_ascii_case("yes")
}

fn prompt_path(default: &str) -> String {
    eprint!("  Path [{}]: ", default);
    io::stderr().flush().unwrap();
    let mut answer = String::new();
    if io::stdin().read_line(&mut answer).is_err() || answer.trim().is_empty() {
        return default.to_string();
    }
    answer.trim().to_string()
}

/// Expand `~` to the user's home directory.
fn expand_tilde(path: &str) -> PathBuf {
    if path == "~" {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home);
        }
    } else if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(path)
}

// =========================================================================
// Model downloads (feature-gated)
// =========================================================================

#[cfg(feature = "embed")]
fn pull_with_bar(
    registry: &strata_intelligence::ModelRegistry,
    name: &str,
    display_name: &str,
) -> Result<std::path::PathBuf, strata_intelligence::InferenceError> {
    use std::time::Instant;

    let start = Instant::now();
    let result = registry.pull_with_progress(name, |downloaded, total| {
        if total == 0 {
            eprint!("\r  Downloading {}... ", display_name);
            io::stderr().flush().unwrap();
            return;
        }
        let pct = (downloaded as f64 / total as f64 * 100.0).min(100.0);
        let bar_width = 25;
        let filled = (pct / 100.0 * bar_width as f64) as usize;
        let empty = bar_width - filled;
        let dl_mb = downloaded / 1_000_000;
        let total_mb = total / 1_000_000;
        eprint!(
            "\r  Downloading {}  [{}>{}] {:3.0}% ({}/{} MB)  ",
            display_name,
            "\u{2588}".repeat(filled),
            " ".repeat(empty),
            pct,
            dl_mb,
            total_mb,
        );
        io::stderr().flush().unwrap();
    });

    let elapsed = start.elapsed().as_secs();
    // Clear the progress line (ANSI erase-line + carriage return)
    eprint!("\x1B[2K\r");
    io::stderr().flush().unwrap();

    match &result {
        Ok(_) => {
            if elapsed > 1 {
                eprintln!("  \u{2713} {} downloaded ({}s)", display_name, elapsed);
            } else {
                eprintln!("  \u{2713} {} downloaded", display_name);
            }
        }
        Err(_) => {} // caller handles errors
    }
    result
}

/// Try downloading a model, retrying once on failure.
#[cfg(feature = "embed")]
fn pull_with_retry(
    registry: &strata_intelligence::ModelRegistry,
    name: &str,
    display_name: &str,
) -> Result<std::path::PathBuf, strata_intelligence::InferenceError> {
    match pull_with_bar(registry, name, display_name) {
        Ok(path) => Ok(path),
        Err(first_err) => {
            eprintln!("  \u{26a0} Download failed: {}. Retrying...", first_err);
            match pull_with_bar(registry, name, display_name) {
                Ok(path) => Ok(path),
                Err(second_err) => Err(second_err),
            }
        }
    }
}

#[cfg(feature = "embed")]
fn offer_model_downloads(config_path: &Path, hw: &HardwareInfo, non_interactive: bool) {
    use strata_intelligence::ModelRegistry;

    let registry = ModelRegistry::new();

    // --- Embedding model ---
    eprintln!();
    eprintln!("  Strata can auto-embed your text data for semantic search.");

    let minilm_available = registry.resolve("miniLM").is_ok();
    if minilm_available {
        eprintln!("  \u{2713} MiniLM already downloaded — auto-embedding enabled");
    } else if non_interactive || prompt_yes_no("Download MiniLM-L6-v2? (45 MB)", true) {
        match pull_with_retry(&registry, "miniLM", "MiniLM-L6-v2") {
            Ok(_) => {
                eprintln!("  \u{2713} Auto-embedding enabled");
                // Enable auto_embed in config
                if let Ok(mut cfg) = strata_executor::StrataConfig::from_file(config_path) {
                    cfg.auto_embed = true;
                    let _ = cfg.write_to_file(config_path);
                }
            }
            Err(e) => {
                eprintln!("  \u{2717} Download failed: {}", e);
                eprintln!("  \u{2139} Run 'strata models pull miniLM' to try again later.");
            }
        }
    } else {
        eprintln!("  \u{2139} No problem. Run 'strata models pull miniLM' anytime to enable semantic search.");
    }

    // --- Generation model ---
    let ram_gb = hw.ram_bytes / (1024 * 1024 * 1024);
    if ram_gb < 2 {
        return; // Not enough RAM for any generation model
    }

    eprintln!();
    eprintln!(
        "  Strata can run a local LLM for RAG and text generation \u{2014} no API keys needed."
    );

    // Build list of generation models that fit in RAM budget (size < ram/2)
    let budget = hw.ram_bytes / 2;
    let candidates: Vec<_> = registry
        .list_available()
        .into_iter()
        .filter(|m| {
            m.task == strata_intelligence::ModelTask::Generate
                && m.size_bytes <= budget
                && m.name != "gpt2" // skip gpt2 — too small to be useful
        })
        .collect();

    if candidates.is_empty() {
        return;
    }

    // Check if any are already downloaded
    let already_local: Vec<_> = candidates.iter().filter(|m| m.is_local).collect();
    if !already_local.is_empty() {
        eprintln!("  \u{2713} {} already downloaded", already_local[0].name);
        return;
    }

    if non_interactive {
        // Auto-pick the first (smallest) model
        let pick = &candidates[0];
        match pull_with_retry(&registry, &pick.name, &pick.name) {
            Ok(_) => eprintln!("  \u{2713} {} ready", pick.name),
            Err(e) => {
                eprintln!("  \u{2717} Download failed: {}", e);
                eprintln!(
                    "  \u{2139} Run 'strata models pull {}' to try again later.",
                    pick.name
                );
            }
        }
        return;
    }

    eprintln!("  Choose a model to download, or skip:");
    eprintln!();
    for (i, m) in candidates.iter().enumerate() {
        let size = format_size(m.size_bytes);
        eprintln!("    {}) {:<20} ({})", i + 1, m.name, size);
    }
    eprintln!("    s) Skip for now");
    eprintln!();

    eprint!("  Pick [1-{}, s]: ", candidates.len());
    io::stderr().flush().unwrap();

    let mut answer = String::new();
    if io::stdin().read_line(&mut answer).is_err() {
        return;
    }
    let trimmed = answer.trim();

    if trimmed.eq_ignore_ascii_case("s") || trimmed.is_empty() {
        eprintln!("  \u{2139} No problem. Run 'strata models pull <name>' anytime to add one.");
        return;
    }

    if let Ok(idx) = trimmed.parse::<usize>() {
        if idx >= 1 && idx <= candidates.len() {
            let pick = &candidates[idx - 1];
            match pull_with_retry(&registry, &pick.name, &pick.name) {
                Ok(_) => eprintln!("  \u{2713} {} ready", pick.name),
                Err(e) => {
                    eprintln!("  \u{2717} Download failed: {}", e);
                    eprintln!(
                        "  \u{2139} Run 'strata models pull {}' to try again later.",
                        pick.name
                    );
                }
            }
            return;
        }
    }

    eprintln!("  \u{2139} Skipped. Run 'strata models pull <name>' anytime to add one.");
}

#[cfg(not(feature = "embed"))]
fn offer_model_downloads(_config_path: &Path, _hw: &HardwareInfo, _non_interactive: bool) {
    // No-op when embed feature is not compiled in
}

#[allow(dead_code)] // Used by embed feature gate
fn format_size(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.1} GB", bytes as f64 / 1_000_000_000.0)
    } else {
        format!("{} MB", bytes / 1_000_000)
    }
}

// =========================================================================
// Sample data seeding
// =========================================================================

/// Seed minimal sample data so help examples work out of the box.
fn seed_minimal_data(db: &Strata) {
    use strata_executor::Value;

    // Don't re-seed if the database already has user data
    if db.kv_get("greeting").ok().flatten().is_some() {
        return;
    }

    let _ = db.kv_put("greeting", Value::String("hello world".into()));

    use std::collections::HashMap;

    let mut user_map = HashMap::new();
    user_map.insert("name".to_string(), Value::String("Alice".into()));
    user_map.insert("role".to_string(), Value::String("developer".into()));
    user_map.insert("joined".to_string(), Value::String("2026-03-24".into()));
    let _ = db.json_set("user:1", "$", Value::Object(Box::new(user_map)));

    let mut event_map = HashMap::new();
    event_map.insert(
        "action".to_string(),
        Value::String("database_created".into()),
    );
    let event_payload = Value::Object(Box::new(event_map));
    let _ = db.event_append("system.init", event_payload);
}

const SAMPLE_DATASET_URL: &str =
    "https://raw.githubusercontent.com/strata-ai-labs/sample-data/main/quickstart.json";

/// Offer to download and load a richer sample dataset from GitHub.
fn offer_sample_dataset(db: &Strata, non_interactive: bool) {
    eprintln!();
    eprintln!("  We have a sample dataset to help you explore Strata's features.");

    if non_interactive {
        // Non-interactive: skip, just seed minimal data
        seed_minimal_data(db);
        return;
    }

    if !prompt_yes_no("Load the sample dataset?", true) {
        eprintln!("  \u{2139} Loading minimal sample data instead.");
        seed_minimal_data(db);
        return;
    }

    // Try to download the sample dataset
    match download_sample_dataset() {
        Ok(content) => match load_sample_json(db, &content) {
            Ok(count) => {
                eprintln!("  \u{2713} Loaded {} sample records", count);
            }
            Err(e) => {
                eprintln!(
                    "  \u{2139} Could not parse dataset: {}. Loading minimal data.",
                    e
                );
                seed_minimal_data(db);
            }
        },
        Err(e) => {
            eprintln!(
                "  \u{2139} Could not download dataset: {}. Loading minimal data.",
                e
            );
            seed_minimal_data(db);
        }
    }
}

fn download_sample_dataset() -> Result<String, String> {
    // Try curl first, then wget
    let output = std::process::Command::new("curl")
        .args(["-fsSL", "--max-time", "10", SAMPLE_DATASET_URL])
        .output();

    match output {
        Ok(out) if out.status.success() => {
            String::from_utf8(out.stdout).map_err(|e| format!("invalid UTF-8: {}", e))
        }
        _ => {
            // Fallback to wget
            let output = std::process::Command::new("wget")
                .args(["-qO-", "--timeout=10", SAMPLE_DATASET_URL])
                .output()
                .map_err(|e| format!("neither curl nor wget available: {}", e))?;
            if output.status.success() {
                String::from_utf8(output.stdout).map_err(|e| format!("invalid UTF-8: {}", e))
            } else {
                Err("download failed".into())
            }
        }
    }
}

/// Load a JSON array of records into the database.
/// Expected format: [{"type": "kv"|"json"|"event", "key": "...", "value": ...}, ...]
fn load_sample_json(db: &Strata, content: &str) -> Result<usize, String> {
    use strata_executor::Value;

    let parsed: Value =
        serde_json::from_str(content).map_err(|e| format!("JSON parse error: {}", e))?;

    let records = match &parsed {
        Value::Array(arr) => arr.as_ref(),
        _ => return Err("expected a JSON array".into()),
    };

    let mut count = 0;
    for record in records.iter() {
        if let Value::Object(obj) = record {
            let record_type = obj.get("type").and_then(|v| match v {
                Value::String(s) => Some(s.as_str()),
                _ => None,
            });
            let key = obj.get("key").and_then(|v| match v {
                Value::String(s) => Some(s.as_str()),
                _ => None,
            });

            match (record_type, key) {
                (Some("kv"), Some(k)) => {
                    if let Some(v) = obj.get("value") {
                        let _ = db.kv_put(k, v.clone());
                        count += 1;
                    }
                }
                (Some("json"), Some(k)) => {
                    if let Some(v) = obj.get("value") {
                        let _ = db.json_set(k, "$", v.clone());
                        count += 1;
                    }
                }
                (Some("event"), _) => {
                    let event_type = key.unwrap_or("sample");
                    if let Some(v) = obj.get("value") {
                        let _ = db.event_append(event_type, v.clone());
                        count += 1;
                    }
                }
                _ => {} // skip unknown record types
            }
        }
    }

    Ok(count)
}

// =========================================================================
// Shared init core
// =========================================================================

fn print_welcome_banner() {
    eprintln!();
    eprintln!("  \u{256d}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{256e}");
    eprintln!("  \u{2502}         Welcome to Strata       \u{2502}");
    eprintln!("  \u{2570}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{256f}");
    eprintln!();
}

fn detect_and_print_hardware() -> (HardwareInfo, Profile) {
    eprintln!("  Detecting hardware...");
    let hw = engine_detect_hardware();
    let storage = detect_storage();
    let ram_gb = hw.ram_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    eprintln!(
        "  \u{2713} {} cores \u{00b7} {:.0} GB RAM \u{00b7} {}",
        hw.cores, ram_gb, storage
    );
    let profile = Profile::classify(hw);
    eprintln!("  \u{2713} Profile: {}", profile);
    (hw, profile)
}

fn create_and_open_db(
    db_path: &Path,
    profile: Profile,
    hw: HardwareInfo,
) -> Result<Strata, String> {
    std::fs::create_dir_all(db_path)
        .map_err(|e| format!("Failed to create directory {}: {}", db_path.display(), e))?;

    let config_path = db_path.join("strata.toml");
    if !config_path.exists() {
        let cfg = build_config(profile, hw);
        cfg.write_to_file(&config_path)
            .map_err(|e| format!("Failed to write config: {}", e))?;
    }

    Strata::open(db_path).map_err(|e| format!("Failed to open database: {}", e))
}

// =========================================================================
// Telemetry opt-in
// =========================================================================

fn offer_telemetry_opt_in(config_path: &Path, non_interactive: bool) {
    if non_interactive {
        return; // Telemetry defaults to off; don't enable silently
    }

    eprintln!();
    if prompt_yes_no(
        "Help improve Strata by sending anonymous usage data?",
        false,
    ) {
        eprintln!(
            "  \u{2713} Telemetry enabled \u{2014} thank you! (disable anytime in strata.toml)"
        );
        if let Ok(mut cfg) = strata_executor::StrataConfig::from_file(config_path) {
            cfg.telemetry = true;
            let _ = cfg.write_to_file(config_path);
        }
    } else {
        eprintln!("  \u{2713} No telemetry \u{2014} no data will be sent.");
    }
}

// =========================================================================
// `strata init` — explicit setup, asks for path, does NOT enter REPL
// =========================================================================

/// Run the full init wizard. Creates a database at a user-chosen path.
/// Does NOT return a Strata handle — prints instructions to navigate there.
pub fn run_init(default_path: &str, non_interactive: bool) -> Result<(), String> {
    print_welcome_banner();

    let (hw, profile) = detect_and_print_hardware();

    // Ask where to create the database
    eprintln!();
    eprintln!("  Where should we create your database?");
    let chosen_path = if non_interactive {
        eprintln!("  Path [{}]: {}", default_path, default_path);
        default_path.to_string()
    } else {
        prompt_path(default_path)
    };

    let db_path = expand_tilde(&chosen_path);

    let db = create_and_open_db(&db_path, profile, hw)?;
    eprintln!("  \u{2713} Database ready at {}", db_path.display());

    let config_path = db_path.join("strata.toml");
    offer_model_downloads(&config_path, &hw, non_interactive);
    offer_telemetry_opt_in(&config_path, non_interactive);

    offer_sample_dataset(&db, non_interactive);

    // End with navigation instructions (not a REPL)
    eprintln!();
    eprintln!("  You're all set. To start using your database:");
    eprintln!();
    eprintln!("    cd {}", db_path.display());
    eprintln!("    strata                       Open the interactive REPL");
    eprintln!("    strata kv put key \"value\"     Store data from the command line");
    eprintln!("    strata up                    Start the server for multi-process access");
    eprintln!();

    Ok(())
}

// =========================================================================
// `strata` (bare) — in-place init, creates .strata/, returns handle for REPL
// =========================================================================

/// Quick in-place init when the user runs bare `strata` with no database.
/// Creates `.strata/` in the current directory and returns a handle for the REPL.
pub fn run_init_in_place(non_interactive: bool) -> Result<Strata, String> {
    print_welcome_banner();

    eprintln!("  No database found in this directory.");
    if !non_interactive && !prompt_yes_no("Create one here? (.strata/)", true) {
        return Err("Aborted. Run 'strata init' to create a database elsewhere.".into());
    }
    eprintln!();

    let (hw, profile) = detect_and_print_hardware();

    let db_path = PathBuf::from(".strata");

    let db = create_and_open_db(&db_path, profile, hw)?;
    eprintln!("  \u{2713} Database ready at .strata/");

    let config_path = db_path.join("strata.toml");
    offer_model_downloads(&config_path, &hw, non_interactive);
    offer_telemetry_opt_in(&config_path, non_interactive);

    offer_sample_dataset(&db, non_interactive);

    eprintln!();
    eprintln!("  You're all set. Launching the REPL...");
    eprintln!();

    Ok(db)
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Classification and config tests live in strata_engine::database::profile.
    // CLI tests below verify delegation and storage detection only.

    #[test]
    fn build_config_delegates_to_engine_embedded() {
        let hw = HardwareInfo {
            ram_bytes: 512 * 1024 * 1024,
            cores: 1,
        };
        let cfg = build_config(Profile::Embedded, hw);
        // Engine profile sets these values — CLI just re-exports.
        assert_eq!(cfg.storage.write_buffer_size, 16 * 1024 * 1024);
        assert_eq!(cfg.storage.background_threads, 1);
        assert_eq!(cfg.storage.target_file_size, 4 * 1024 * 1024);
        assert!(cfg.storage.compaction_rate_limit > 0);
    }

    #[test]
    fn build_config_delegates_to_engine_server() {
        let hw = HardwareInfo {
            ram_bytes: 64 * 1024 * 1024 * 1024,
            cores: 32,
        };
        let cfg = build_config(Profile::Server, hw);
        assert_eq!(cfg.storage.write_buffer_size, 256 * 1024 * 1024);
        assert_eq!(cfg.storage.background_threads, 8);
        assert_eq!(cfg.storage.target_file_size, 128 * 1024 * 1024);
    }

    #[test]
    fn engine_hardware_detect_positive() {
        let hw = engine_detect_hardware();
        assert!(hw.ram_bytes > 0);
        assert!(hw.cores >= 1);
    }

    #[test]
    fn expand_tilde_works() {
        if std::env::var("HOME").is_ok() {
            let expanded = expand_tilde("~/Documents/Strata");
            assert!(!expanded.to_string_lossy().starts_with("~"));
            assert!(expanded.to_string_lossy().ends_with("Documents/Strata"));
        }
    }

    #[test]
    fn expand_tilde_bare() {
        if let Ok(home) = std::env::var("HOME") {
            let expanded = expand_tilde("~");
            assert_eq!(expanded, PathBuf::from(home));
        }
    }

    #[test]
    fn expand_tilde_no_tilde() {
        let expanded = expand_tilde("/tmp/test");
        assert_eq!(expanded, PathBuf::from("/tmp/test"));
    }

    #[test]
    fn format_size_mb() {
        assert_eq!(format_size(670_000_000), "670 MB");
    }

    #[test]
    fn format_size_gb() {
        assert_eq!(format_size(2_400_000_000), "2.4 GB");
    }
}
