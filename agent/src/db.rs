//! SQLite database operations for probe selection coordination.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use log::warn;
use rusqlite::{params, Connection};

use crate::{labels_match, ProbeConfig, TargetConfig};

/// Open (or create) the database, enable WAL mode, set busy timeout, and create tables.
pub fn open_db(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path).context("failed to open SQLite database")?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "busy_timeout", 30000)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS targets (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            chip     TEXT NOT NULL,
            probe    TEXT NOT NULL,
            labels   TEXT NOT NULL,
            taken_by TEXT,
            taken_at TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_targets_probe ON targets(probe);",
    )
    .context("failed to create tables")?;
    Ok(conn)
}

/// Populate the database from config if not already initialized.
/// Uses INSERT OR IGNORE so concurrent calls with the same config are safe.
pub fn init_db(conn: &Connection, config: &ProbeConfig) -> Result<usize> {
    let mut stmt = conn.prepare("INSERT OR IGNORE INTO targets (chip, probe, labels) VALUES (?1, ?2, ?3)")?;
    let mut added = 0;
    for t in &config.targets {
        let labels_json = serde_json::to_string(&t.labels)?;
        added += stmt.execute(params![t.chip, t.probe, labels_json])?;
    }
    Ok(added)
}

/// Release targets whose `taken_at` is older than `timeout`.
pub fn release_stale(conn: &Connection, timeout: Duration) -> Result<()> {
    let secs = timeout.as_secs() as f64;
    let released = conn.execute(
        "UPDATE targets SET taken_by = NULL, taken_at = NULL
         WHERE taken_by IS NOT NULL
           AND taken_at IS NOT NULL
           AND julianday('now') - julianday(taken_at) > ?1 / 86400.0",
        params![secs],
    )?;
    if released > 0 {
        warn!(
            "Released {} stale target lock(s) (older than {}s)",
            released,
            timeout.as_secs()
        );
    }
    Ok(())
}

struct AvailableTarget {
    id: i64,
    config: TargetConfig,
}

/// Fetch all targets from the database (regardless of claim status).
fn fetch_all(conn: &Connection) -> Result<Vec<TargetConfig>> {
    let mut stmt = conn.prepare("SELECT chip, probe, labels FROM targets")?;
    let rows = stmt.query_map([], |row| {
        let chip: String = row.get(0)?;
        let probe: String = row.get(1)?;
        let labels_json: String = row.get(2)?;
        Ok((chip, probe, labels_json))
    })?;
    let mut result = Vec::new();
    for row in rows {
        let (chip, probe, labels_json) = row?;
        let labels: HashMap<String, String> = serde_json::from_str(&labels_json).unwrap_or_default();
        result.push(TargetConfig { chip, probe, labels });
    }
    Ok(result)
}

/// Check whether the given label sets can ever be simultaneously satisfied
/// by the targets in the database (ignoring claim status).
pub fn can_satisfy(conn: &Connection, label_sets: &[&[(&str, &str)]]) -> Result<bool> {
    let all_targets = fetch_all(conn)?;
    let mut used_indices: Vec<usize> = Vec::new();

    for labels in label_sets {
        let mut found = false;
        for (i, target) in all_targets.iter().enumerate() {
            if used_indices.contains(&i) {
                continue;
            }
            if labels_match(&target.labels, labels) {
                used_indices.push(i);
                found = true;
                break;
            }
        }
        if !found {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Fetch all available (unclaimed) targets from the database.
fn fetch_available(conn: &Connection) -> Result<Vec<AvailableTarget>> {
    let mut stmt = conn.prepare("SELECT id, chip, probe, labels FROM targets WHERE taken_by IS NULL")?;
    let rows = stmt.query_map([], |row| {
        let id: i64 = row.get(0)?;
        let chip: String = row.get(1)?;
        let probe: String = row.get(2)?;
        let labels_json: String = row.get(3)?;
        Ok((id, chip, probe, labels_json))
    })?;
    let mut result = Vec::new();
    for row in rows {
        let (id, chip, probe, labels_json) = row?;
        let labels: HashMap<String, String> = serde_json::from_str(&labels_json).unwrap_or_default();
        result.push(AvailableTarget {
            id,
            config: TargetConfig { chip, probe, labels },
        });
    }
    Ok(result)
}

/// Try to claim one target matching the given labels within a transaction.
/// Returns the target's row id and config on success.
pub fn try_claim_one(
    conn: &Connection,
    owner_id: &str,
    labels: &[(&str, &str)],
) -> Result<Option<(i64, TargetConfig)>> {
    conn.execute("BEGIN IMMEDIATE", [])?;

    let available = fetch_available(conn)?;
    for target in available {
        if labels_match(&target.config.labels, labels) {
            conn.execute(
                "UPDATE targets SET taken_by = ?1, taken_at = datetime('now')
                 WHERE id = ?2 AND taken_by IS NULL",
                params![owner_id, target.id],
            )?;
            conn.execute("COMMIT", [])?;
            return Ok(Some((target.id, target.config)));
        }
    }

    conn.execute("COMMIT", [])?;
    Ok(None)
}

/// Try to claim one target per label set, atomically. All or nothing.
pub fn try_claim_multiple(
    conn: &Connection,
    owner_id: &str,
    label_sets: &[&[(&str, &str)]],
) -> Result<Option<Vec<(i64, TargetConfig)>>> {
    conn.execute("BEGIN IMMEDIATE", [])?;

    let available = fetch_available(conn)?;
    let mut claimed_ids: Vec<i64> = Vec::new();
    let mut results: Vec<(i64, TargetConfig)> = Vec::new();

    for labels in label_sets {
        let mut found = false;
        for target in &available {
            if claimed_ids.contains(&target.id) {
                continue;
            }
            if labels_match(&target.config.labels, labels) {
                claimed_ids.push(target.id);
                results.push((target.id, target.config.clone()));
                found = true;
                break;
            }
        }
        if !found {
            conn.execute("ROLLBACK", [])?;
            return Ok(None);
        }
    }

    // Claim all matched targets
    for &id in &claimed_ids {
        conn.execute(
            "UPDATE targets SET taken_by = ?1, taken_at = datetime('now')
             WHERE id = ?2 AND taken_by IS NULL",
            params![owner_id, id],
        )?;
    }

    conn.execute("COMMIT", [])?;
    Ok(Some(results))
}

/// Release a specific target by id, only if owned by the given owner.
pub fn release_target(conn: &Connection, target_id: i64, owner_id: &str) -> Result<()> {
    conn.execute(
        "UPDATE targets SET taken_by = NULL, taken_at = NULL
         WHERE id = ?1 AND taken_by = ?2",
        params![target_id, owner_id],
    )?;
    Ok(())
}
