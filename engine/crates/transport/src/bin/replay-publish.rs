use replay_transport::{Config, Error, Publisher};
use std::io::Read;
fn main() {
    if let Err(error) = run() {
        eprintln!("replay-publish: {error}");
        // Closed exit contract: never classify risk diagnostics by their text.
        let retryable = matches!(
            error.downcast_ref::<Error>(),
            Some(Error::Transport | Error::Resource)
        ) || error.downcast_ref::<std::io::Error>().is_some();
        std::process::exit(if retryable { 21 } else { 20 });
    }
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: replay-publish CONFIG.json (REDIS_URL environment required)")?;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(1_048_577)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1_048_576 {
        return Err("config limit".into());
    }
    let config: Config = serde_json::from_slice(&bytes)?;
    let url = std::env::var("REDIS_URL").map_err(|_| "REDIS_URL required")?;
    let mut publisher = Publisher::open(&url, config, replay_risk::RiskLimits::default())?;
    // Optional supervisor handshake. No consumer joins before setup and initial.
    if let Some(path) = std::env::args().nth(2) {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        file.sync_all()?;
    }
    while publisher.step()? {}
    println!("terminal published; output remains provisional until all consumers complete");
    Ok(())
}
