use std::path::PathBuf;

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), String> {
    let mut arguments = std::env::args().skip(1);
    let root = arguments
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| "usage: indexer-canonical-audit <canonical-root>".to_owned())?;
    if arguments.next().is_some() {
        return Err("usage: indexer-canonical-audit <canonical-root>".to_owned());
    }
    let report = indexer_finalize::audit_canonical_root(&root)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report)
            .map_err(|error| format!("encoding canonical audit report: {error}"))?
    );
    Ok(())
}
