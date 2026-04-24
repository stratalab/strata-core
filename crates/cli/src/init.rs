use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use strata_executor::Strata;
use strata_executor::{
    apply_profile_if_defaults, detect_hardware as engine_detect_hardware, HardwareInfo, Profile,
    StrataConfig,
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
        if let Ok(entries) = std::fs::read_dir("/sys/block") {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("nvme") {
                    return StorageKind::Nvme;
                }
            }
            for entry in std::fs::read_dir("/sys/block")
                .into_iter()
                .flatten()
                .flatten()
            {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("sd") || name.starts_with("vd") {
                    let rotational = entry.path().join("queue/rotational");
                    if let Ok(value) = std::fs::read_to_string(rotational) {
                        return if value.trim() == "0" {
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
        return StorageKind::Ssd;
    }

    #[allow(unreachable_code)]
    StorageKind::Unknown
}

fn build_config(profile: Profile, hw: HardwareInfo) -> StrataConfig {
    let mut cfg = StrataConfig::default();
    apply_profile_if_defaults(&mut cfg, profile, hw);
    cfg
}

fn prompt_yes_no(question: &str, default_yes: bool) -> bool {
    let hint = if default_yes { "[Y/n]" } else { "[y/N]" };
    eprint!("  {question} {hint} ");
    io::stderr().flush().expect("stderr flush should succeed");
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
    eprint!("  Path [{default}]: ");
    io::stderr().flush().expect("stderr flush should succeed");
    let mut answer = String::new();
    if io::stdin().read_line(&mut answer).is_err() || answer.trim().is_empty() {
        return default.to_string();
    }
    answer.trim().to_string()
}

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

#[cfg(feature = "embed")]
fn pull_with_bar(
    registry: &strata_intelligence::ModelRegistry,
    name: &str,
    display_name: &str,
) -> Result<std::path::PathBuf, strata_intelligence::InferenceError> {
    let start = std::time::Instant::now();
    let result = registry.pull_with_progress(name, |downloaded, total| {
        if total == 0 {
            eprint!("\r  Downloading {display_name}... ");
            io::stderr().flush().expect("stderr flush should succeed");
            return;
        }
        let pct = (downloaded as f64 / total as f64 * 100.0).min(100.0);
        let bar_width = 25;
        let filled = (pct / 100.0 * bar_width as f64) as usize;
        let empty = bar_width - filled;
        let dl_mb = downloaded / 1_000_000;
        let total_mb = total / 1_000_000;
        eprint!(
            "\r  Downloading {display_name}  [{}>{}] {:3.0}% ({dl_mb}/{total_mb} MB)  ",
            "\u{2588}".repeat(filled),
            " ".repeat(empty),
            pct,
        );
        io::stderr().flush().expect("stderr flush should succeed");
    });

    let elapsed = start.elapsed().as_secs();
    eprint!("\x1B[2K\r");
    io::stderr().flush().expect("stderr flush should succeed");

    if result.is_ok() {
        if elapsed > 1 {
            eprintln!("  \u{2713} {display_name} downloaded ({elapsed}s)");
        } else {
            eprintln!("  \u{2713} {display_name} downloaded");
        }
    }
    result
}

#[cfg(feature = "embed")]
fn pull_with_retry(
    registry: &strata_intelligence::ModelRegistry,
    name: &str,
    display_name: &str,
) -> Result<std::path::PathBuf, strata_intelligence::InferenceError> {
    match pull_with_bar(registry, name, display_name) {
        Ok(path) => Ok(path),
        Err(first_err) => {
            eprintln!("  \u{26a0} Download failed: {first_err}. Retrying...");
            pull_with_bar(registry, name, display_name)
        }
    }
}

#[cfg(feature = "embed")]
fn offer_model_downloads(config_path: &Path, hw: &HardwareInfo, non_interactive: bool) {
    use strata_intelligence::{ModelRegistry, ModelTask};

    let registry = ModelRegistry::new();

    eprintln!();
    eprintln!("  Strata can auto-embed your text data for semantic search.");

    let minilm_available = registry.resolve("miniLM").is_ok();
    if minilm_available {
        eprintln!("  \u{2713} MiniLM already downloaded — auto-embedding enabled");
    } else if non_interactive || prompt_yes_no("Download MiniLM-L6-v2? (45 MB)", true) {
        match pull_with_retry(&registry, "miniLM", "MiniLM-L6-v2") {
            Ok(_) => {
                eprintln!("  \u{2713} Auto-embedding enabled");
                if let Ok(mut cfg) = StrataConfig::from_file(config_path) {
                    cfg.auto_embed = true;
                    let _ = cfg.write_to_file(config_path);
                }
            }
            Err(error) => {
                eprintln!("  \u{2717} Download failed: {error}");
                eprintln!("  \u{2139} Run 'strata models pull miniLM' to try again later.");
            }
        }
    } else {
        eprintln!(
            "  \u{2139} No problem. Run 'strata models pull miniLM' anytime to enable semantic search."
        );
    }

    let ram_gb = hw.ram_bytes / (1024 * 1024 * 1024);
    if ram_gb < 2 {
        return;
    }

    eprintln!();
    eprintln!("  Strata can run a local LLM for RAG and text generation — no API keys needed.");

    let budget = hw.ram_bytes / 2;
    let candidates: Vec<_> = registry
        .list_available()
        .into_iter()
        .filter(|model| {
            model.task == ModelTask::Generate && model.size_bytes <= budget && model.name != "gpt2"
        })
        .collect();

    if candidates.is_empty() {
        return;
    }

    let already_local: Vec<_> = candidates.iter().filter(|model| model.is_local).collect();
    if !already_local.is_empty() {
        eprintln!("  \u{2713} {} already downloaded", already_local[0].name);
        return;
    }

    if non_interactive {
        let pick = &candidates[0];
        match pull_with_retry(&registry, &pick.name, &pick.name) {
            Ok(_) => eprintln!("  \u{2713} {} ready", pick.name),
            Err(error) => {
                eprintln!("  \u{2717} Download failed: {error}");
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
    for (i, model) in candidates.iter().enumerate() {
        eprintln!(
            "    {}) {:<20} ({})",
            i + 1,
            model.name,
            format_size(model.size_bytes)
        );
    }
    eprintln!("    s) Skip for now");
    eprintln!();

    eprint!("  Pick [1-{}, s]: ", candidates.len());
    io::stderr().flush().expect("stderr flush should succeed");

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
                Err(error) => {
                    eprintln!("  \u{2717} Download failed: {error}");
                    eprintln!(
                        "  \u{2139} Run 'strata models pull {}' to try again later.",
                        pick.name
                    );
                }
            }
        }
    }
}

#[cfg(not(feature = "embed"))]
fn offer_model_downloads(_config_path: &Path, _hw: &HardwareInfo, _non_interactive: bool) {}

#[cfg(any(feature = "embed", test))]
fn format_size(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.1} GB", bytes as f64 / 1_000_000_000.0)
    } else {
        format!("{} MB", bytes / 1_000_000)
    }
}

fn seed_minimal_data(db: &Strata) {
    use strata_executor::Value;

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
    let _ = db.event_append("system.init", Value::Object(Box::new(event_map)));
}

const SAMPLE_DATASET_URL: &str =
    "https://raw.githubusercontent.com/strata-ai-labs/sample-data/main/quickstart.json";

fn offer_sample_dataset(db: &Strata, non_interactive: bool) {
    eprintln!();
    eprintln!("  We have a sample dataset to help you explore Strata's features.");

    if non_interactive {
        seed_minimal_data(db);
        return;
    }

    if !prompt_yes_no("Load the sample dataset?", true) {
        eprintln!("  \u{2139} Loading minimal sample data instead.");
        seed_minimal_data(db);
        return;
    }

    match download_sample_dataset() {
        Ok(content) => match load_sample_json(db, &content) {
            Ok(count) => eprintln!("  \u{2713} Loaded {count} sample records"),
            Err(error) => {
                eprintln!("  \u{2139} Could not parse dataset: {error}. Loading minimal data.");
                seed_minimal_data(db);
            }
        },
        Err(error) => {
            eprintln!("  \u{2139} Could not download dataset: {error}. Loading minimal data.");
            seed_minimal_data(db);
        }
    }
}

fn download_sample_dataset() -> Result<String, String> {
    let output = std::process::Command::new("curl")
        .args(["-fsSL", "--max-time", "10", SAMPLE_DATASET_URL])
        .output();

    match output {
        Ok(out) if out.status.success() => {
            String::from_utf8(out.stdout).map_err(|error| format!("invalid UTF-8: {error}"))
        }
        _ => {
            let output = std::process::Command::new("wget")
                .args(["-qO-", "--timeout=10", SAMPLE_DATASET_URL])
                .output()
                .map_err(|error| format!("neither curl nor wget available: {error}"))?;
            if output.status.success() {
                String::from_utf8(output.stdout).map_err(|error| format!("invalid UTF-8: {error}"))
            } else {
                Err("download failed".into())
            }
        }
    }
}

fn load_sample_json(db: &Strata, content: &str) -> Result<usize, String> {
    use strata_executor::Value;

    let parsed: Value =
        serde_json::from_str(content).map_err(|error| format!("JSON parse error: {error}"))?;

    let records = match &parsed {
        Value::Array(arr) => arr.as_ref(),
        _ => return Err("expected a JSON array".into()),
    };

    let mut count = 0;
    for record in records {
        if let Value::Object(obj) = record {
            let record_type = obj.get("type").and_then(|value| match value {
                Value::String(text) => Some(text.as_str()),
                _ => None,
            });
            let key = obj.get("key").and_then(|value| match value {
                Value::String(text) => Some(text.as_str()),
                _ => None,
            });

            match (record_type, key) {
                (Some("kv"), Some(key)) => {
                    if let Some(value) = obj.get("value") {
                        let _ = db.kv_put(key, value.clone());
                        count += 1;
                    }
                }
                (Some("json"), Some(key)) => {
                    if let Some(value) = obj.get("value") {
                        let _ = db.json_set(key, "$", value.clone());
                        count += 1;
                    }
                }
                (Some("event"), _) => {
                    let event_type = key.unwrap_or("sample");
                    if let Some(value) = obj.get("value") {
                        let _ = db.event_append(event_type, value.clone());
                        count += 1;
                    }
                }
                _ => {}
            }
        }
    }

    Ok(count)
}

fn print_welcome_banner() {
    eprintln!();
    eprintln!("  ╭─────────────────────────────────╮");
    eprintln!("  │         Welcome to Strata       │");
    eprintln!("  ╰─────────────────────────────────╯");
    eprintln!();
}

fn detect_and_print_hardware() -> (HardwareInfo, Profile) {
    eprintln!("  Detecting hardware...");
    let hw = engine_detect_hardware();
    let storage = detect_storage();
    let ram_gb = hw.ram_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    eprintln!(
        "  ✓ {} cores · {:.0} GB RAM · {}",
        hw.cores, ram_gb, storage
    );
    let profile = Profile::classify(hw);
    eprintln!("  ✓ Profile: {profile}");
    (hw, profile)
}

fn create_and_open_db(
    db_path: &Path,
    profile: Profile,
    hw: HardwareInfo,
) -> Result<Strata, String> {
    std::fs::create_dir_all(db_path).map_err(|error| {
        format!(
            "Failed to create directory {}: {}",
            db_path.display(),
            error
        )
    })?;

    let config_path = db_path.join("strata.toml");
    if !config_path.exists() {
        let cfg = build_config(profile, hw);
        cfg.write_to_file(&config_path)
            .map_err(|error| format!("Failed to write config: {error}"))?;
    }

    Strata::open(db_path).map_err(|error| format!("Failed to open database: {error}"))
}

fn offer_telemetry_opt_in(config_path: &Path, non_interactive: bool) {
    if non_interactive {
        return;
    }

    eprintln!();
    if prompt_yes_no(
        "Help improve Strata by sending anonymous usage data?",
        false,
    ) {
        eprintln!("  ✓ Telemetry enabled — thank you! (disable anytime in strata.toml)");
        if let Ok(mut cfg) = StrataConfig::from_file(config_path) {
            cfg.telemetry = true;
            let _ = cfg.write_to_file(config_path);
        }
    } else {
        eprintln!("  ✓ No telemetry — no data will be sent.");
    }
}

pub(crate) fn run_init(default_path: &str, non_interactive: bool) -> Result<(), String> {
    print_welcome_banner();

    let (hw, profile) = detect_and_print_hardware();

    eprintln!();
    eprintln!("  Where should we create your database?");
    let chosen_path = if non_interactive {
        eprintln!("  Path [{default_path}]: {default_path}");
        default_path.to_string()
    } else {
        prompt_path(default_path)
    };

    let db_path = expand_tilde(&chosen_path);
    let db = create_and_open_db(&db_path, profile, hw)?;
    eprintln!("  ✓ Database ready at {}", db_path.display());

    let config_path = db_path.join("strata.toml");
    offer_model_downloads(&config_path, &hw, non_interactive);
    offer_telemetry_opt_in(&config_path, non_interactive);
    offer_sample_dataset(&db, non_interactive);

    eprintln!();
    eprintln!("  You're all set. To start using your database:");
    eprintln!();
    eprintln!("    cd {}", db_path.display());
    eprintln!("    strata                       Open the interactive REPL");
    eprintln!("    strata kv put key \"value\"     Store data from the command line");
    eprintln!("    strata up                    Start the server for multi-process access");
    eprintln!();

    db.close()
        .map_err(|error| format!("Failed to close initialized database: {error}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_config_delegates_to_engine_embedded() {
        let hw = HardwareInfo {
            ram_bytes: 512 * 1024 * 1024,
            cores: 1,
        };
        let cfg = build_config(Profile::Embedded, hw);
        assert_eq!(cfg.storage.write_buffer_size, 16 * 1024 * 1024);
        assert_eq!(cfg.storage.background_threads, 1);
        assert_eq!(cfg.storage.target_file_size, 4 * 1024 * 1024);
        assert!(cfg.storage.compaction_rate_limit > 0);
    }

    #[test]
    fn expand_tilde_works() {
        if std::env::var("HOME").is_ok() {
            let expanded = expand_tilde("~/Documents/Strata");
            assert!(!expanded.to_string_lossy().starts_with('~'));
            assert!(expanded.to_string_lossy().ends_with("Documents/Strata"));
        }
    }

    #[test]
    #[cfg(any(feature = "embed", test))]
    fn format_size_gb() {
        assert_eq!(format_size(2_400_000_000), "2.4 GB");
    }
}
