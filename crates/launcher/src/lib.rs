//! Safe, single-command composition root for local `InsiderTrader` deployments.
//!
//! The launcher owns process composition only.  The runtime remains the sole
//! owner of the journal, broker connections, and trading state; every terminal
//! is just another IPC client.

#![forbid(unsafe_code)]

use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_CONFIG: &str = "data/insidertrader.cfg";
const DEFAULT_JOURNAL: &str = "data/runtime.journal";
const DEFAULT_SOCKET: &str = "data/runtime.sock";
const NEWS_API_ENV: &str = "IT_NEWSAPI_KEY";
const LLM_API_ENV: &str = "IT_LLM_API_KEY";

/// Run the launcher command line.
///
/// # Errors
///
/// Returns an error when setup, process startup, or IPC attachment fails.
pub fn run(args: &[String]) -> Result<(), String> {
    let command = args.get(1).map_or("run", String::as_str);
    let paths = Paths::from_args(args);
    match command {
        "setup" | "configure" => setup(&paths).map(|_| ()),
        "reset" => reset(&paths, args),
        "run" | "start" => run_with_terminal(&paths),
        "server" => run_server(&paths, &SetupSecrets::default()).map(|_| ()),
        "terminal" | "connect" => run_terminal(&paths),
        "help" | "--help" | "-h" => {
            print_help();
            Ok(())
        }
        other => Err(format!("unknown command '{other}'; try 'insider help'")),
    }
}

#[derive(Clone, Debug)]
struct Paths {
    config: PathBuf,
    journal: PathBuf,
    socket: PathBuf,
}

impl Paths {
    fn from_args(args: &[String]) -> Self {
        let value = |flag: &str, fallback: &str| {
            args.windows(2)
                .find(|pair| pair[0] == flag)
                .map(|pair| PathBuf::from(&pair[1]))
                .or_else(|| std::env::var(flag_env(flag)).ok().map(PathBuf::from))
                .unwrap_or_else(|| PathBuf::from(fallback))
        };
        Self {
            config: value("--config", DEFAULT_CONFIG),
            journal: value("--journal", DEFAULT_JOURNAL),
            socket: value("--socket", DEFAULT_SOCKET),
        }
    }
}

fn flag_env(flag: &str) -> &'static str {
    match flag {
        "--config" => "IT_CONFIG",
        "--journal" => "IT_JOURNAL",
        "--socket" => "IT_SOCKET",
        _ => "IT_UNUSED",
    }
}

#[derive(Clone, Debug, Default)]
struct SetupSecrets {
    newsapi_key: Option<String>,
    llm_api_key: Option<String>,
}

fn setup(paths: &Paths) -> Result<SetupSecrets, String> {
    println!(
        "InsiderTrader setup\n===================\n\n  CONFIG   {}\n  JOURNAL  {}\n  SOCKET   {}\n",
        paths.config.display(),
        paths.journal.display(),
        paths.socket.display()
    );
    println!("The headless runtime owns one authoritative trading state.");
    println!("Additional local renderers can attach with: insider terminal");
    if paths.config.exists() {
        println!(
            "Using existing configuration {}; it was not overwritten.",
            paths.config.display()
        );
    } else {
        let template = Path::new("config/example.cfg");
        if !template.exists() {
            return Err(format!(
                "configuration template not found: {}",
                template.display()
            ));
        }
        if let Some(parent) = paths.config.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("create config directory: {e}"))?;
        }
        fs::copy(template, &paths.config).map_err(|e| format!("initialize config: {e}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&paths.config, fs::Permissions::from_mode(0o600))
                .map_err(|e| format!("secure configuration permissions: {e}"))?;
        }
        println!(
            "Created {} from the example configuration.",
            paths.config.display()
        );
    }
    if io::stdin().is_terminal() {
        let mode = read_setting(
            "Trading mode [MANUAL/HYBRID/AUTONOMOUS] (default MANUAL)",
            "MANUAL",
            &["MANUAL", "HYBRID", "AUTONOMOUS"],
        )?;
        let theme = read_setting(
            "Terminal theme [AMBER/BLUE/GREEN/GRAY/MONO] (default AMBER)",
            "AMBER",
            &["AMBER", "BLUE", "GREEN", "GRAY", "MONO"],
        )?;
        let news_sort = read_setting(
            "News ranking [RELEVANCE/RECENCY/SOURCE] (default RELEVANCE)",
            "RELEVANCE",
            &["RELEVANCE", "RECENCY", "SOURCE"],
        )?;
        let prompt =
            read_optional_text("LLM system prompt (optional; Enter keeps the configured prompt)")?;
        update_cfg_setting(&paths.config, "autonomy.mode", &format!("\"{mode}\""))?;
        update_cfg_setting(&paths.config, "terminal.theme", &format!("\"{theme}\""))?;
        update_cfg_setting(&paths.config, "news.sort", &format!("\"{news_sort}\""))?;
        if let Some(prompt) = prompt {
            let escaped = prompt.replace('\\', "\\\\").replace('"', "\\\"");
            update_cfg_setting(
                &paths.config,
                "llm.system_prompt",
                &format!("\"{escaped}\""),
            )?;
        }
        animate_setup("Applying configuration");
        println!(
            "Configuration saved atomically. Run 'insider setup' to change these preferences."
        );
    }
    let mut secrets = SetupSecrets {
        newsapi_key: std::env::var(NEWS_API_ENV)
            .ok()
            .filter(|value| !value.is_empty()),
        llm_api_key: std::env::var(LLM_API_ENV)
            .ok()
            .filter(|value| !value.is_empty()),
    };
    if secrets.newsapi_key.is_none() {
        secrets.newsapi_key = read_optional_secret("NewsAPI key (optional; Enter to skip)")?;
    } else {
        println!("NewsAPI key: inherited from {NEWS_API_ENV} (not displayed or persisted)");
    }
    if secrets.llm_api_key.is_none() {
        secrets.llm_api_key = read_optional_secret("LLM provider key (optional; Enter to skip)")?;
    } else {
        println!("LLM provider key: inherited from {LLM_API_ENV} (not displayed or persisted)");
    }
    println!(
        "\nNext steps:\n  1. Review the CFG and market symbols.\n  2. Keep credentials in the environment/secret manager.\n  3. Run 'insider' to start the runtime and terminal.\n  4. Type TV for a local browser chart, or run 'insider terminal' for another window."
    );
    Ok(secrets)
}

fn animate_setup(label: &str) {
    // Keep setup pleasant without making it depend on a UI toolkit. The
    // bounded spinner is only used for interactive setup and never touches
    // configuration state while it is running.
    for frame in ['|', '/', '-', '\\'] {
        print!("\r{label} {frame}");
        let _ = io::stdout().flush();
        thread::sleep(Duration::from_millis(70));
    }
    print!("\r{label} done\n");
    let _ = io::stdout().flush();
}

fn read_setting(prompt: &str, default: &str, allowed: &[&str]) -> Result<String, String> {
    loop {
        print!("{prompt}: ");
        io::stdout()
            .flush()
            .map_err(|error| format!("flush setup prompt: {error}"))?;
        let mut value = String::new();
        io::stdin()
            .read_line(&mut value)
            .map_err(|error| format!("read setup prompt: {error}"))?;
        let value = value.trim().to_ascii_uppercase();
        let value = if value.is_empty() {
            default.to_owned()
        } else {
            value
        };
        if allowed.contains(&value.as_str()) {
            return Ok(value);
        }
        println!("Choose one of: {}", allowed.join(", "));
    }
}

fn read_optional_text(prompt: &str) -> Result<Option<String>, String> {
    print!("{prompt}: ");
    io::stdout()
        .flush()
        .map_err(|error| format!("flush setup prompt: {error}"))?;
    let mut value = String::new();
    io::stdin()
        .read_line(&mut value)
        .map_err(|error| format!("read setup prompt: {error}"))?;
    let value = value.trim();
    if value.is_empty() {
        Ok(None)
    } else if value.len() > 16_384 {
        Err("LLM system prompt exceeds 16 KiB".into())
    } else {
        Ok(Some(value.to_owned()))
    }
}

fn update_cfg_setting(path: &Path, key: &str, value: &str) -> Result<(), String> {
    let text = fs::read_to_string(path).map_err(|error| format!("read configuration: {error}"))?;
    let mut found = false;
    let mut output = String::with_capacity(text.len() + key.len() + value.len() + 4);
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with(key) && trimmed[key.len()..].trim_start().starts_with('=') {
            if found {
                continue;
            }
            output.push_str(key);
            output.push_str(" = ");
            output.push_str(value);
            output.push('\n');
            found = true;
        } else {
            output.push_str(line);
            output.push('\n');
        }
    }
    if !found {
        output.push_str(key);
        output.push_str(" = ");
        output.push_str(value);
        output.push('\n');
    }
    let temp = path.with_extension(format!("cfg.tmp.{}", std::process::id()));
    fs::write(&temp, output).map_err(|error| format!("write configuration: {error}"))?;
    fs::rename(&temp, path).map_err(|error| {
        let _ = fs::remove_file(&temp);
        format!("commit configuration: {error}")
    })
}

fn run_with_terminal(paths: &Paths) -> Result<(), String> {
    let secrets = setup(paths)?;
    let mut server = run_server(paths, &secrets)?;
    let result = run_terminal(paths);
    let _ = server.kill();
    let _ = server.wait();
    result
}

fn runtime_command(paths: &Paths, secrets: &SetupSecrets) -> Command {
    let mut command = Command::new("cargo");
    command.args(["run", "--locked", "-p", "insider-runtime", "--", "serve"]);
    command.arg("--config").arg(&paths.config);
    command.arg("--journal").arg(&paths.journal);
    command.arg("--socket").arg(&paths.socket);
    if let Some(value) = &secrets.newsapi_key {
        command.env(NEWS_API_ENV, value);
    }
    if let Some(value) = &secrets.llm_api_key {
        command.env(LLM_API_ENV, value);
    }
    command
}

fn run_server(paths: &Paths, secrets: &SetupSecrets) -> Result<Child, String> {
    if paths.socket.exists() {
        return Err(format!(
            "socket already exists: {} (another runtime may be active)",
            paths.socket.display()
        ));
    }
    if let Some(parent) = paths.journal.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create runtime directory: {e}"))?;
    }
    let mut child = runtime_command(paths, secrets)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| format!("start runtime: {e}"))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if paths.socket.exists() {
            return Ok(child);
        }
        if let Some(status) = child
            .try_wait()
            .map_err(|e| format!("check runtime: {e}"))?
        {
            return Err(format!("runtime exited during startup: {status}"));
        }
        thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    Err("runtime did not create its socket within 30 seconds".into())
}

fn read_optional_secret(prompt: &str) -> Result<Option<String>, String> {
    if !io::stdin().is_terminal() {
        return Ok(None);
    }
    print!("{prompt}: ");
    io::stdout()
        .flush()
        .map_err(|error| format!("flush setup prompt: {error}"))?;
    let mut value = String::new();
    io::stdin()
        .read_line(&mut value)
        .map_err(|error| format!("read setup prompt: {error}"))?;
    let value = value.trim().to_owned();
    Ok((!value.is_empty()).then_some(value))
}

#[allow(clippy::too_many_lines)]
fn reset(paths: &Paths, args: &[String]) -> Result<(), String> {
    let assume_yes = args.iter().any(|value| value == "--yes" || value == "-y");
    let mut stale_socket = false;
    let lock_path = paths.journal.with_extension("lock");
    let mut stale_lock = false;
    if lock_path.exists() {
        let contents = fs::read_to_string(&lock_path)
            .map_err(|error| format!("inspect journal lock: {error}"))?;
        let pid = contents
            .strip_prefix("pid=")
            .and_then(|value| value.lines().next())
            .ok_or_else(|| {
                format!(
                    "refusing reset: malformed journal lock {}",
                    lock_path.display()
                )
            })?
            .parse::<u32>()
            .map_err(|_| {
                format!(
                    "refusing reset: malformed journal lock {}",
                    lock_path.display()
                )
            })?;
        if process_is_alive(pid) {
            return Err(format!(
                "an active runtime process (pid {pid}) owns {}; stop it before reset",
                lock_path.display()
            ));
        }
        stale_lock = true;
    }
    if paths.socket.exists() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileTypeExt;
            let metadata = fs::symlink_metadata(&paths.socket)
                .map_err(|error| format!("inspect runtime socket: {error}"))?;
            if !metadata.file_type().is_socket() {
                return Err(format!(
                    "refusing reset: {} exists but is not a Unix socket",
                    paths.socket.display()
                ));
            }
            match std::os::unix::net::UnixStream::connect(&paths.socket) {
                Ok(_) => {
                    return Err(format!(
                        "an active runtime is using {}; stop it before reset",
                        paths.socket.display()
                    ));
                }
                Err(_) => stale_socket = true,
            }
        }
        #[cfg(not(unix))]
        {
            return Err("reset cannot verify runtime ownership on this platform".into());
        }
    }
    println!(
        "Reset will remove {}{}{} and preserve {} (journal/trading history).",
        paths.config.display(),
        if stale_socket {
            " plus the stale runtime socket"
        } else {
            ""
        },
        if stale_lock {
            " plus the stale journal lock"
        } else {
            ""
        },
        paths.journal.display()
    );
    if !assume_yes {
        if !io::stdin().is_terminal() {
            return Err("reset requires an interactive confirmation or --yes".into());
        }
        print!("Type RESET to continue: ");
        io::stdout()
            .flush()
            .map_err(|error| format!("flush reset prompt: {error}"))?;
        let mut confirmation = String::new();
        io::stdin()
            .read_line(&mut confirmation)
            .map_err(|error| format!("read reset confirmation: {error}"))?;
        if confirmation.trim() != "RESET" {
            return Err("reset cancelled; nothing was removed".into());
        }
    }
    if paths.config.exists() {
        fs::remove_file(&paths.config).map_err(|error| format!("remove configuration: {error}"))?;
    }
    if stale_socket {
        fs::remove_file(&paths.socket).map_err(|error| format!("remove stale socket: {error}"))?;
    }
    if stale_lock {
        fs::remove_file(&lock_path)
            .map_err(|error| format!("remove stale journal lock: {error}"))?;
    }
    println!("Reset complete. Run 'insider setup' to create a fresh configuration.");
    Ok(())
}

fn process_is_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .is_ok_and(|status| status.success())
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

fn run_terminal(paths: &Paths) -> Result<(), String> {
    if !paths.socket.exists() {
        return Err(format!(
            "runtime socket is absent: {}; run 'insider' or 'insider server' first",
            paths.socket.display()
        ));
    }
    let status = Command::new("cargo")
        .args(["run", "--locked", "-p", "insider-terminal"])
        .env("IT_ENGINE_SOCKET", &paths.socket)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|e| format!("start terminal: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("terminal exited with {status}"))
    }
}

fn print_help() {
    let _ = writeln!(
        io::stdout(),
        "usage: insider [setup|reset|run|server|terminal] [--config PATH] [--journal PATH] [--socket PATH]\n\n  run       initialize safely, ask for optional keys, start runtime + terminal (default)\n  setup     create CFG if absent and optionally collect process-only provider keys\n  reset     remove CFG and stale socket/lock; preserve journal; use --yes for automation\n  server    start only the authoritative headless runtime\n  terminal  attach another local terminal to the existing runtime\n\nMultiple terminals may connect to one server; they never duplicate trading state."
    );
}

#[cfg(test)]
mod tests {
    use super::Paths;

    #[test]
    fn defaults_are_deployment_local() {
        let paths = Paths::from_args(&["insider".into()]);
        assert_eq!(paths.socket.to_string_lossy(), "data/runtime.sock");
        assert_eq!(paths.config.to_string_lossy(), "data/insidertrader.cfg");
    }
}
