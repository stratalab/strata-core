use std::env;
use std::fs;
use std::io::{self, Write as _};
use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;
use std::time::Duration;

use strata_executor::{IpcServer, Strata};

static RUNNING_PTR: AtomicPtr<AtomicBool> = AtomicPtr::new(std::ptr::null_mut());

pub(crate) fn run_up(db_path: &str, foreground: bool) -> i32 {
    let data_dir = PathBuf::from(db_path);

    let pid_path = data_dir.join("strata.pid");
    if pid_path.exists() {
        if let Ok(pid_str) = fs::read_to_string(&pid_path) {
            if let Ok(pid) = pid_str.trim().parse::<i32>() {
                let alive = unsafe { libc::kill(pid, 0) == 0 };
                if alive {
                    eprintln!("Strata server already running (pid {pid})");
                    return 1;
                }
                let _ = fs::remove_file(&pid_path);
            }
        }
    }

    let db = match Strata::open(db_path) {
        Ok(db) => db,
        Err(error) => {
            eprintln!("Failed to open database: {error}");
            return 1;
        }
    };

    let database = db.database();
    let access_mode = db.access_mode();
    drop(db);

    if foreground {
        match IpcServer::start(&data_dir, database, access_mode) {
            Ok(mut server) => {
                eprintln!("Strata server started (pid {})", process::id());
                eprintln!("Listening on {}", server.socket_path().display());
                eprintln!("Press Ctrl+C to stop.");

                let running = Arc::new(AtomicBool::new(true));
                install_signal_handlers(running.clone());

                while running.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(200));
                }

                server.shutdown();
                eprintln!("\nStrata server stopped.");
                0
            }
            Err(error) => {
                eprintln!("Failed to start IPC server: {error}");
                1
            }
        }
    } else {
        let log_path = data_dir.join("strata.log");
        let stdout = match fs::File::create(&log_path) {
            Ok(file) => file,
            Err(error) => {
                eprintln!("Failed to create log file: {error}");
                return 1;
            }
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&log_path, fs::Permissions::from_mode(0o600));
        }
        let stderr = match stdout.try_clone() {
            Ok(file) => file,
            Err(error) => {
                eprintln!("Failed to clone log file handle: {error}");
                return 1;
            }
        };

        let daemonize = daemonize::Daemonize::new()
            .working_directory(&data_dir)
            .stdout(stdout)
            .stderr(stderr);

        eprintln!(
            "Starting Strata server in background...\nPID file: {}",
            data_dir.join("strata.pid").display()
        );

        match daemonize.start() {
            Ok(()) => match IpcServer::start(&data_dir, database, access_mode) {
                Ok(mut server) => {
                    let running = Arc::new(AtomicBool::new(true));
                    install_signal_handlers(running.clone());

                    while running.load(Ordering::SeqCst) {
                        std::thread::sleep(Duration::from_millis(200));
                    }

                    server.shutdown();
                    0
                }
                Err(error) => {
                    eprintln!("Failed to start IPC server: {error}");
                    1
                }
            },
            Err(error) => {
                eprintln!("Failed to daemonize: {error}");
                1
            }
        }
    }
}

pub(crate) fn run_down(db_path: &str) -> i32 {
    let data_dir = PathBuf::from(db_path);

    match IpcServer::stop(&data_dir) {
        Ok(()) => {
            eprintln!("Strata server stopped.");
            0
        }
        Err(error) => {
            eprintln!("Failed to stop server: {error}");
            1
        }
    }
}

pub(crate) fn run_uninstall(skip_confirm: bool) -> i32 {
    let home = match env::var("HOME") {
        Ok(home) => PathBuf::from(home),
        Err(_) => {
            eprintln!("Could not determine home directory.");
            return 1;
        }
    };

    let strata_dir = home.join(".strata");
    let legacy_model_dir = home.join(".stratadb");
    let history_file = home.join(".strata_history");

    eprintln!("This will remove:");
    eprintln!();
    if strata_dir.exists() {
        eprintln!("  ~/.strata/          Binary, models, and configuration");
    }
    if legacy_model_dir.exists() {
        eprintln!("  ~/.stratadb/        Legacy model files");
    }
    if history_file.exists() {
        eprintln!("  ~/.strata_history   REPL history");
    }
    eprintln!("  PATH entries        From shell configuration files");
    eprintln!();
    eprintln!("Note: Per-project database directories (.strata/ in your projects)");
    eprintln!("will NOT be removed. Delete those manually if needed.");
    eprintln!();

    if !skip_confirm {
        eprint!("Continue? [y/N] ");
        io::stderr().flush().expect("stderr flush should succeed");
        let mut answer = String::new();
        if io::stdin().read_line(&mut answer).is_err() || !answer.trim().eq_ignore_ascii_case("y") {
            eprintln!("Aborted.");
            return 0;
        }
    }

    if strata_dir.exists() {
        match fs::remove_dir_all(&strata_dir) {
            Ok(()) => eprintln!("  Removed ~/.strata/"),
            Err(error) => eprintln!("  Warning: could not remove ~/.strata/: {error}"),
        }
    }

    if legacy_model_dir.exists() {
        match fs::remove_dir_all(&legacy_model_dir) {
            Ok(()) => eprintln!("  Removed ~/.stratadb/"),
            Err(error) => eprintln!("  Warning: could not remove ~/.stratadb/: {error}"),
        }
    }

    if history_file.exists() {
        match fs::remove_file(&history_file) {
            Ok(()) => eprintln!("  Removed ~/.strata_history"),
            Err(error) => eprintln!("  Warning: could not remove ~/.strata_history: {error}"),
        }
    }

    let strata_path_marker = home.join(".strata/bin").to_string_lossy().to_string();
    let shell_configs = [
        home.join(".zshrc"),
        home.join(".bashrc"),
        home.join(".bash_profile"),
        home.join(".profile"),
        home.join(".config/fish/config.fish"),
    ];

    for config_path in &shell_configs {
        if !config_path.exists() {
            continue;
        }
        let contents = match fs::read_to_string(config_path) {
            Ok(contents) => contents,
            Err(_) => continue,
        };
        if !contents.contains(&strata_path_marker) {
            continue;
        }

        let filtered: Vec<&str> = contents
            .lines()
            .filter(|line| line.trim() != "# Strata" && !line.contains(&strata_path_marker))
            .collect();

        let mut result = filtered;
        while result.last() == Some(&"") {
            result.pop();
        }
        let mut new_contents = result.join("\n");
        new_contents.push('\n');

        match fs::write(config_path, &new_contents) {
            Ok(()) => eprintln!("  Cleaned PATH from {}", config_path.display()),
            Err(error) => eprintln!(
                "  Warning: could not update {}: {}",
                config_path.display(),
                error
            ),
        }
    }

    eprintln!();
    eprintln!("Strata has been uninstalled. Restart your shell to apply PATH changes.");
    0
}

fn install_signal_handlers(running: Arc<AtomicBool>) {
    let ptr = Arc::into_raw(running) as *mut AtomicBool;
    RUNNING_PTR.store(ptr, Ordering::SeqCst);
    unsafe {
        libc::signal(
            libc::SIGTERM,
            handle_signal as *const () as usize as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGINT,
            handle_signal as *const () as usize as libc::sighandler_t,
        );
    }
}

extern "C" fn handle_signal(_: libc::c_int) {
    let ptr = RUNNING_PTR.load(Ordering::SeqCst);
    if !ptr.is_null() {
        unsafe {
            (*ptr).store(false, Ordering::SeqCst);
        }
    }
}
