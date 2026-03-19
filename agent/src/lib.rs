//! Crate to coordinate selection of target probes across multiple processes.
//!
//! Uses an on-disk SQLite database (WAL mode) so that multiple hilbench-agent
//! processes can safely coordinate which probes are in use.

mod db;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProbeConfig {
    pub targets: Vec<TargetConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TargetConfig {
    pub chip: String,
    pub probe: String,
    pub labels: HashMap<String, String>,
}

static SELECTOR: OnceLock<ProbeSelector> = OnceLock::new();

/// Initialize global selector backed by SQLite at `db_path`.
/// Populates the database from `config` if not already initialized.
pub fn init(db_path: &Path, config: ProbeConfig) -> Result<&'static ProbeSelector> {
    // We need to handle the case where OnceLock is already set.
    // Since we can't return a Result from get_or_init, do the init first.
    if let Some(s) = SELECTOR.get() {
        info!("Probe selector already initialized, reusing existing instance");
        return Ok(s);
    }
    let selector = ProbeSelector::new(db_path, config)?;
    Ok(SELECTOR.get_or_init(|| selector))
}

pub struct ProbeSelector {
    db_path: PathBuf,
    owner_id: String,
    stale_timeout: Duration,
}

/// A claimed target. Releases the claim on drop.
pub struct Target {
    config: TargetConfig,
    db_path: PathBuf,
    owner_id: String,
    target_id: i64,
}

impl ProbeSelector {
    fn new(db_path: &Path, config: ProbeConfig) -> Result<Self> {
        let conn = db::open_db(db_path)?;
        let target_count = config.targets.len();
        let added = db::init_db(&conn, &config)?;
        let owner_id = uuid::Uuid::new_v4().to_string();
        if added > 0 {
            info!(
                "Initialized probe selector with {} targets ({} added) at {:?} (owner: {})",
                target_count,
                added,
                db_path,
                &owner_id[..8]
            );
        } else {
            info!(
                "Initialized probe selector with {} targets (already populated) at {:?} (owner: {})",
                target_count,
                db_path,
                &owner_id[..8]
            );
        }
        Ok(Self {
            db_path: db_path.to_owned(),
            owner_id,
            stale_timeout: Duration::from_secs(300),
        })
    }

    /// Set the stale lock timeout. Locks older than this are automatically released.
    pub fn set_stale_timeout(&mut self, timeout: Duration) {
        self.stale_timeout = timeout;
    }

    /// Try to select a target matching `labels`. Returns immediately.
    /// Returns `Ok(None)` if no matching target is available.
    pub fn try_select(&self, labels: &[(&str, &str)]) -> Result<Option<Target>> {
        let conn = db::open_db(&self.db_path)?;
        db::release_stale(&conn, self.stale_timeout)?;
        let result = db::try_claim_one(&conn, &self.owner_id, labels)?;
        match result {
            Some((id, config)) => {
                info!(
                    "Selected target probe={} chip={} for labels [{}]",
                    config.probe,
                    config.chip,
                    fmt_labels(labels)
                );
                Ok(Some(Target {
                    config,
                    db_path: self.db_path.clone(),
                    owner_id: self.owner_id.clone(),
                    target_id: id,
                }))
            }
            None => {
                debug!("No target available for labels [{}]", fmt_labels(labels));
                Ok(None)
            }
        }
    }

    /// Wait until a target matching `labels` becomes available.
    /// Fails immediately if no target in the database matches the labels.
    pub async fn select(&self, labels: &[(&str, &str)]) -> Result<Target> {
        let conn = db::open_db(&self.db_path)?;
        if !db::can_satisfy(&conn, &[labels])? {
            bail!(
                "no target exists matching labels [{}]",
                fmt_labels(labels)
            );
        }
        let start = Instant::now();
        let mut interval = Duration::from_millis(100);
        let max_interval = Duration::from_secs(2);
        let mut logged_waiting = false;
        loop {
            if let Some(target) = self.try_select(labels)? {
                if logged_waiting {
                    info!(
                        "Acquired target probe={} after {:.1}s wait",
                        target.config.probe,
                        start.elapsed().as_secs_f64()
                    );
                }
                return Ok(target);
            }
            if !logged_waiting {
                info!("Waiting for target matching [{}]...", fmt_labels(labels));
                logged_waiting = true;
            }
            tokio::time::sleep(interval).await;
            interval = (interval * 2).min(max_interval);
        }
    }

    /// Atomically try to select one target per label set.
    /// Returns `Ok(None)` if any label set cannot be satisfied — no targets are claimed.
    pub fn try_select_multiple(&self, label_sets: &[&[(&str, &str)]]) -> Result<Option<Vec<Target>>> {
        let conn = db::open_db(&self.db_path)?;
        db::release_stale(&conn, self.stale_timeout)?;
        let results = db::try_claim_multiple(&conn, &self.owner_id, label_sets)?;
        match results {
            Some(pairs) => {
                let probes: Vec<_> = pairs.iter().map(|(_, c)| c.probe.as_str()).collect();
                info!(
                    "Selected {} targets [{}] for {} label sets",
                    pairs.len(),
                    probes.join(", "),
                    label_sets.len()
                );
                Ok(Some(
                    pairs
                        .into_iter()
                        .map(|(id, config)| Target {
                            config,
                            db_path: self.db_path.clone(),
                            owner_id: self.owner_id.clone(),
                            target_id: id,
                        })
                        .collect(),
                ))
            }
            None => {
                debug!("Cannot satisfy all {} label sets simultaneously", label_sets.len());
                Ok(None)
            }
        }
    }

    /// Wait until all label sets can be simultaneously satisfied, then claim them atomically.
    /// Fails immediately if the database doesn't have enough matching targets to ever satisfy all label sets.
    pub async fn select_multiple(&self, label_sets: &[&[(&str, &str)]]) -> Result<Vec<Target>> {
        let conn = db::open_db(&self.db_path)?;
        if !db::can_satisfy(&conn, label_sets)? {
            let descriptions: Vec<_> = label_sets
                .iter()
                .map(|ls| format!("[{}]", fmt_labels(ls)))
                .collect();
            bail!(
                "not enough targets in database to satisfy all label sets: {}",
                descriptions.join(", ")
            );
        }
        let start = Instant::now();
        let mut interval = Duration::from_millis(100);
        let max_interval = Duration::from_secs(2);
        let mut logged_waiting = false;
        loop {
            if let Some(targets) = self.try_select_multiple(label_sets)? {
                if logged_waiting {
                    let probes: Vec<_> = targets.iter().map(|t| t.config.probe.as_str()).collect();
                    info!(
                        "Acquired {} targets [{}] after {:.1}s wait",
                        targets.len(),
                        probes.join(", "),
                        start.elapsed().as_secs_f64()
                    );
                }
                return Ok(targets);
            }
            if !logged_waiting {
                info!(
                    "Waiting for {} target(s) matching all label sets...",
                    label_sets.len()
                );
                logged_waiting = true;
            }
            tokio::time::sleep(interval).await;
            interval = (interval * 2).min(max_interval);
        }
    }
}

impl Target {
    /// Get the configuration of this target.
    pub fn config(&self) -> &TargetConfig {
        &self.config
    }
}

impl Drop for Target {
    fn drop(&mut self) {
        match db::open_db(&self.db_path) {
            Ok(conn) => {
                let _ = db::release_target(&conn, self.target_id, &self.owner_id);
                info!("Released target probe={}", self.config.probe);
            }
            Err(e) => {
                warn!("Failed to release target probe={}: {}", self.config.probe, e);
            }
        }
    }
}

/// Check if all selector labels match the target's labels.
/// A label matches if the target doesn't have the key, or if it has the key with the same value.
fn labels_match(target_labels: &HashMap<String, String>, selector: &[(&str, &str)]) -> bool {
    for (key, value) in selector {
        if let Some(v) = target_labels.get(*key) {
            if v != value {
                return false;
            }
        }
    }
    true
}

fn fmt_labels(labels: &[(&str, &str)]) -> String {
    labels
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests;
