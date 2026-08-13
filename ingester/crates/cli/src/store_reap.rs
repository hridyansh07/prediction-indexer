//! `indexer-store-reap` — audits or removes expired closed ingest databases.

use std::path::PathBuf;

use indexer_cli::ingest_reaper::{ReapMode, default_report_path, sweep};

const USAGE: &str = "usage: indexer-store-reap <store-root> \
                     [--retention-hours <hours>] [--mode audit|delete] [--report <path>]";

struct Arguments {
    store_root: PathBuf,
    retention_hours: u64,
    mode: ReapMode,
    report: Option<PathBuf>,
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            std::process::ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), String> {
    let arguments = parse_arguments()?;
    let report = sweep(
        &arguments.store_root,
        unix_now_ns()?,
        arguments.retention_hours,
        arguments.mode,
    )?;
    let report_path = arguments
        .report
        .unwrap_or_else(|| default_report_path(&arguments.store_root));
    report.write(&report_path)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report)
            .map_err(|error| format!("encoding reaper report: {error}"))?
    );
    Ok(())
}

fn parse_arguments() -> Result<Arguments, String> {
    let mut positional = Vec::new();
    let mut retention_hours = 24;
    let mut mode = ReapMode::Audit;
    let mut report = None;
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--retention-hours" => {
                let raw = arguments
                    .next()
                    .ok_or_else(|| format!("--retention-hours needs a value\n{USAGE}"))?;
                retention_hours = raw
                    .parse::<u64>()
                    .map_err(|_| format!("invalid --retention-hours value: {raw:?}\n{USAGE}"))?;
            }
            "--mode" => {
                let raw = arguments
                    .next()
                    .ok_or_else(|| format!("--mode needs a value\n{USAGE}"))?;
                mode = ReapMode::parse(&raw)?;
            }
            "--report" => {
                report = Some(PathBuf::from(
                    arguments
                        .next()
                        .ok_or_else(|| format!("--report needs a value\n{USAGE}"))?,
                ));
            }
            "-h" | "--help" => return Err(USAGE.to_owned()),
            other if other.starts_with("--") => {
                return Err(format!("unknown flag: {other}\n{USAGE}"));
            }
            other => positional.push(other.to_owned()),
        }
    }
    if positional.len() != 1 {
        return Err(USAGE.to_owned());
    }
    Ok(Arguments {
        store_root: PathBuf::from(&positional[0]),
        retention_hours,
        mode,
        report,
    })
}

fn unix_now_ns() -> Result<u64, String> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| format!("reading UTC clock: {error}"))?
        .as_nanos();
    u64::try_from(nanos).map_err(|_| "UTC clock is outside the supported range".to_owned())
}
