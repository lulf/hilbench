//! Crate to coordinate selection of target probes across multiple processes.
//!
//! Uses an on-disk SQLite database (WAL mode) so that multiple hilbench-agent
//! processes can safely coordinate which probes are in use.

mod db;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Result;
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
        db::init_db(&conn, &config)?;
        Ok(Self {
            db_path: db_path.to_owned(),
            owner_id: uuid::Uuid::new_v4().to_string(),
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
        db::try_claim_one(&conn, &self.owner_id, labels).map(|opt| {
            opt.map(|(id, config)| Target {
                config,
                db_path: self.db_path.clone(),
                owner_id: self.owner_id.clone(),
                target_id: id,
            })
        })
    }

    /// Wait until a target matching `labels` becomes available.
    pub async fn select(&self, labels: &[(&str, &str)]) -> Result<Target> {
        let mut interval = Duration::from_millis(100);
        let max_interval = Duration::from_secs(2);
        loop {
            if let Some(target) = self.try_select(labels)? {
                return Ok(target);
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
            Some(pairs) => Ok(Some(
                pairs
                    .into_iter()
                    .map(|(id, config)| Target {
                        config,
                        db_path: self.db_path.clone(),
                        owner_id: self.owner_id.clone(),
                        target_id: id,
                    })
                    .collect(),
            )),
            None => Ok(None),
        }
    }

    /// Wait until all label sets can be simultaneously satisfied, then claim them atomically.
    pub async fn select_multiple(&self, label_sets: &[&[(&str, &str)]]) -> Result<Vec<Target>> {
        let mut interval = Duration::from_millis(100);
        let max_interval = Duration::from_secs(2);
        loop {
            if let Some(targets) = self.try_select_multiple(label_sets)? {
                return Ok(targets);
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
        if let Ok(conn) = db::open_db(&self.db_path) {
            let _ = db::release_target(&conn, self.target_id, &self.owner_id);
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

#[cfg(test)]
mod tests;
