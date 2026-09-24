use replay_transport::{Config, Error, Publisher};
use std::io::Read;
fn main() {
    if let Err(error) = run() {
        eprintln!("replay-publish: {error}");
        // Closed exit contract: never classify risk diagnostics by their text.
        let retryable = matches!(
            error.downcast_ref::<Error>(),
            Some(Error::Transport | Error::Resource)
        );
        std::process::exit(if retryable { 21 } else { 20 });
    }
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: replay-publish CONFIG.json (REDIS_URL environment required)")?;
    let mut bytes = Vec::new();
    if path == "--validate-only" {
        std::io::stdin().take(1_048_577).read_to_end(&mut bytes)?;
    } else {
        std::fs::File::open(&path)?
            .take(1_048_577)
            .read_to_end(&mut bytes)?;
    }
    if bytes.len() > 1_048_576 {
        return Err("config limit".into());
    }
    let config: Config = serde_json::from_slice(&bytes)?;
    if path == "--validate-only" {
        config.validate()?;
        return Ok(());
    }
    let url = std::env::var("REDIS_URL").map_err(|_| "REDIS_URL required")?;
    let mut publisher = Publisher::open(&url, config, replay_risk::RiskLimits::default())?;
    // Optional supervisor handshake. No consumer joins before setup and initial.
    if let Some(path) = std::env::args().nth(2) {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        sync_ready(&file)?;
    }
    while publisher.step()? {}
    println!("terminal published; output remains provisional until all consumers complete");
    Ok(())
}

fn sync_ready(file: &std::fs::File) -> Result<(), Error> {
    file.sync_all().map_err(|_| Error::Resource)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    #[test]
    fn ready_sync_failure_remains_a_resource_error() {
        let file = std::fs::File::open("/dev/null").unwrap();
        assert!(matches!(
            super::sync_ready(&file),
            Err(super::Error::Resource)
        ));
    }
}
