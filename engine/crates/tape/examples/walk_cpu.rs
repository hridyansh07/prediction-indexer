//! Linux walk-only CPU harness; no Redis, normalization, Risk or publishing.
//! Usage: walk_cpu DERIVATIVE_DIR INSTRUMENT[,INSTRUMENT...] LANE[,LANE...]
//! The directory must be an already installed/audited derivative. CPU uses
//! /proc/self/stat user+system ticks, not elapsed time or child/build time.
use std::{env, fs, path::PathBuf, process::Command};

use indexer_types::Sha256;
use replay_domain::{InstrumentId, LaneId};
use replay_materialize::{DerivativeManifest, DerivativePin};
use replay_tape::{
    DerivativeWalker, LowerBoundPolicy, PinnedDerivative, ReadLimits, ScopeFilter, WalkItem,
    WalkRequest,
};

fn cpu_ticks() -> (u64, u64) {
    let stat = fs::read_to_string("/proc/self/stat").unwrap();
    let fields: Vec<_> = stat[stat.rfind(')').unwrap() + 2..]
        .split_whitespace()
        .collect();
    (fields[11].parse().unwrap(), fields[12].parse().unwrap())
}

fn main() -> Result<(), String> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 4 {
        return Err(
            "usage: walk_cpu DERIVATIVE_DIR INSTRUMENT[,INSTRUMENT...] LANE[,LANE...]".into(),
        );
    }
    let hz = Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .map_err(|e| e.to_string())?;
    let hz: f64 = std::str::from_utf8(&hz.stdout)
        .map_err(|e| e.to_string())?
        .trim()
        .parse::<u64>()
        .map_err(|e| e.to_string())? as f64;
    let directory = PathBuf::from(&args[1]);
    let receipt = fs::read(directory.join("receipt.json")).map_err(|e| e.to_string())?;
    let manifest: DerivativeManifest = serde_json::from_slice(
        &fs::read(directory.join("manifest.json")).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    let input = PinnedDerivative {
        directory,
        pin: DerivativePin {
            derivative_address: manifest.derivative_address.clone(),
            receipt_sha256: Sha256::digest(&receipt),
        },
    };
    let request = WalkRequest {
        start_ns: manifest.source_receipt.window_start_ns,
        end_ns: manifest.source_receipt.window_end_ns,
        lower_bound: LowerBoundPolicy::RequireWindowBoundary,
        scope: ScopeFilter {
            instruments: args[2]
                .split(',')
                .map(InstrumentId::new)
                .collect::<Result<_, _>>()
                .map_err(|e| e.to_string())?,
            lanes: args[3]
                .split(',')
                .filter(|s| !s.is_empty())
                .map(LaneId::new)
                .collect::<Result<_, _>>()
                .map_err(|e| e.to_string())?,
        },
    };
    let total_start = cpu_ticks();
    let mut walker = DerivativeWalker::open(vec![input], request, ReadLimits::default())?;
    if !matches!(walker.next_item()?, Some(WalkItem::WindowStatus(_))) {
        return Err("expected initial window status".into());
    }
    // Snapshot copy and decoder open are reported in total, separately from read.
    let read_start = cpu_ticks();
    while let Some(item) = walker.next_item()? {
        std::hint::black_box(item);
    }
    let done = walker.finish()?;
    let end = cpu_ticks();
    println!(
        "read_user_s={:.2} read_sys_s={:.2} read_cpu_s={:.2} total_cpu_s={:.2} counts={:?}",
        (end.0 - read_start.0) as f64 / hz,
        (end.1 - read_start.1) as f64 / hz,
        (end.0 + end.1 - read_start.0 - read_start.1) as f64 / hz,
        (end.0 + end.1 - total_start.0 - total_start.1) as f64 / hz,
        done.counts()
    );
    Ok(())
}
