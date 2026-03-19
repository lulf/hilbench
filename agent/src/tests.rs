use std::collections::HashMap;
use std::time::Duration;

use crate::{ProbeConfig, ProbeSelector, TargetConfig};

fn test_config() -> ProbeConfig {
    ProbeConfig {
        targets: vec![
            TargetConfig {
                chip: "nrf52840".into(),
                probe: "jlink-001".into(),
                labels: HashMap::from([("arch".into(), "arm".into()), ("board".into(), "dk".into())]),
            },
            TargetConfig {
                chip: "nrf52840".into(),
                probe: "jlink-002".into(),
                labels: HashMap::from([("arch".into(), "arm".into()), ("board".into(), "ref".into())]),
            },
            TargetConfig {
                chip: "esp32".into(),
                probe: "ftdi-001".into(),
                labels: HashMap::from([("arch".into(), "xtensa".into())]),
            },
        ],
    }
}

fn make_selector(dir: &std::path::Path, config: ProbeConfig) -> ProbeSelector {
    let db_path = dir.join("probes.db");
    ProbeSelector::new(&db_path, config).unwrap()
}

#[test]
fn try_select_matching() {
    let dir = tempfile::tempdir().unwrap();
    let sel = make_selector(dir.path(), test_config());

    let target = sel.try_select(&[("arch", "arm")]).unwrap().unwrap();
    assert_eq!(target.config().chip, "nrf52840");
    assert_eq!(target.config().probe, "jlink-001");
}

#[test]
fn try_select_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let sel = make_selector(dir.path(), test_config());

    let result = sel.try_select(&[("arch", "riscv")]).unwrap();
    assert!(result.is_none());
}

#[test]
fn try_select_claims_exclusively() {
    let dir = tempfile::tempdir().unwrap();
    let sel = make_selector(dir.path(), test_config());

    let t1 = sel.try_select(&[("arch", "arm")]).unwrap().unwrap();
    let t2 = sel.try_select(&[("arch", "arm")]).unwrap().unwrap();
    // Third ARM select should fail — only two ARM targets
    let t3 = sel.try_select(&[("arch", "arm")]).unwrap();
    assert!(t3.is_none());

    // Different probes claimed
    assert_ne!(t1.config().probe, t2.config().probe);
}

#[test]
fn release_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let sel = make_selector(dir.path(), test_config());

    let probe = {
        let t = sel.try_select(&[("arch", "xtensa")]).unwrap().unwrap();
        t.config().probe.clone()
    };
    // After drop, should be selectable again
    let t2 = sel.try_select(&[("arch", "xtensa")]).unwrap().unwrap();
    assert_eq!(t2.config().probe, probe);
}

#[test]
fn try_select_multiple_all_satisfied() {
    let dir = tempfile::tempdir().unwrap();
    let sel = make_selector(dir.path(), test_config());

    let label_sets: &[&[(&str, &str)]] = &[&[("board", "dk")], &[("arch", "xtensa")]];
    let targets = sel.try_select_multiple(label_sets).unwrap().unwrap();
    assert_eq!(targets.len(), 2);
    assert_eq!(targets[0].config().board_label(), "dk");
    assert_eq!(targets[1].config().chip, "esp32");
}

#[test]
fn try_select_multiple_partial_fails() {
    let dir = tempfile::tempdir().unwrap();
    let sel = make_selector(dir.path(), test_config());

    // Claim the only xtensa target first
    let _held = sel.try_select(&[("arch", "xtensa")]).unwrap().unwrap();

    let label_sets: &[&[(&str, &str)]] = &[
        &[("board", "dk")],
        &[("arch", "xtensa")], // not available
    ];
    let result = sel.try_select_multiple(label_sets).unwrap();
    assert!(result.is_none());

    // The dk target should NOT have been claimed either (atomic rollback)
    let dk = sel.try_select(&[("board", "dk")]).unwrap();
    assert!(dk.is_some());
}

#[test]
fn try_select_multiple_no_double_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let sel = make_selector(dir.path(), test_config());

    // Both label sets match ARM targets, but there are only 2
    let label_sets: &[&[(&str, &str)]] = &[&[("arch", "arm")], &[("arch", "arm")]];
    let targets = sel.try_select_multiple(label_sets).unwrap().unwrap();
    assert_eq!(targets.len(), 2);
    assert_ne!(targets[0].config().probe, targets[1].config().probe);

    // Third ARM should fail
    let extra = sel.try_select(&[("arch", "arm")]).unwrap();
    assert!(extra.is_none());
}

#[test]
fn init_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("probes.db");
    let config = test_config();

    let s1 = ProbeSelector::new(&db_path, config.clone()).unwrap();
    let t1 = s1.try_select(&[("arch", "xtensa")]).unwrap().unwrap();

    // Re-init with same config should not wipe state
    let s2 = ProbeSelector::new(&db_path, config).unwrap();
    // xtensa is still taken by s1
    let t2 = s2.try_select(&[("arch", "xtensa")]).unwrap();
    assert!(t2.is_none());

    drop(t1);
    let t3 = s2.try_select(&[("arch", "xtensa")]).unwrap();
    assert!(t3.is_some());
}

#[test]
fn cross_process_coordination() {
    // Simulates two "processes" by using two selectors on the same DB
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("probes.db");
    let config = test_config();

    let s1 = ProbeSelector::new(&db_path, config.clone()).unwrap();
    let s2 = ProbeSelector::new(&db_path, config).unwrap();

    let t1 = s1.try_select(&[("arch", "xtensa")]).unwrap().unwrap();
    let t2 = s2.try_select(&[("arch", "xtensa")]).unwrap();
    assert!(t2.is_none()); // s1 holds it

    drop(t1);
    let t3 = s2.try_select(&[("arch", "xtensa")]).unwrap();
    assert!(t3.is_some());
}

#[tokio::test]
async fn select_waits_for_release() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("probes.db");
    let config = test_config();

    let sel = ProbeSelector::new(&db_path, config).unwrap();
    let held = sel.try_select(&[("arch", "xtensa")]).unwrap().unwrap();

    let db_path2 = db_path.clone();
    let config2 = test_config();
    let handle = tokio::spawn(async move {
        let sel2 = ProbeSelector::new(&db_path2, config2).unwrap();
        // This will poll until the target is released
        let t = tokio::time::timeout(Duration::from_secs(5), sel2.select(&[("arch", "xtensa")]))
            .await
            .expect("select timed out")
            .unwrap();
        assert_eq!(t.config().chip, "esp32");
    });

    // Release after a short delay
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(held);

    handle.await.unwrap();
}

#[tokio::test]
async fn select_multiple_waits() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("probes.db");
    let config = test_config();

    let sel = ProbeSelector::new(&db_path, config).unwrap();
    let held = sel.try_select(&[("arch", "xtensa")]).unwrap().unwrap();

    let db_path2 = db_path.clone();
    let config2 = test_config();
    let handle = tokio::spawn(async move {
        let sel2 = ProbeSelector::new(&db_path2, config2).unwrap();
        let label_sets: &[&[(&str, &str)]] = &[&[("board", "dk")], &[("arch", "xtensa")]];
        let targets = tokio::time::timeout(Duration::from_secs(5), sel2.select_multiple(label_sets))
            .await
            .expect("select_multiple timed out")
            .unwrap();
        assert_eq!(targets.len(), 2);
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(held);

    handle.await.unwrap();
}

#[test]
fn select_multiple_unsatisfiable_fails_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let sel = make_selector(dir.path(), test_config());

    // Request 3 ARM targets but only 2 exist
    let label_sets: &[&[(&str, &str)]] = &[&[("arch", "arm")], &[("arch", "arm")], &[("arch", "arm")]];
    let result = sel.try_select_multiple(label_sets).unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn select_unsatisfiable_labels_fails_early() {
    let dir = tempfile::tempdir().unwrap();
    let sel = make_selector(dir.path(), test_config());

    let result = sel.select(&[("arch", "riscv")]).await;
    let err = result.err().expect("expected error");
    assert!(err.to_string().contains("no target exists"), "got: {}", err);
}

#[tokio::test]
async fn select_multiple_unsatisfiable_labels_fails_early() {
    let dir = tempfile::tempdir().unwrap();
    let sel = make_selector(dir.path(), test_config());

    // 3 ARM targets requested but only 2 exist in db
    let label_sets: &[&[(&str, &str)]] = &[&[("arch", "arm")], &[("arch", "arm")], &[("arch", "arm")]];
    let result = sel.select_multiple(label_sets).await;
    let err = result.err().expect("expected error");
    assert!(err.to_string().contains("not enough targets"), "got: {}", err);
}

// Helper for test readability
impl TargetConfig {
    fn board_label(&self) -> &str {
        self.labels.get("board").map(|s| s.as_str()).unwrap_or("")
    }
}
