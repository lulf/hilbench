//! Crate to coordinate a selection of target probes.
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

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

/// Initialize global selector based on config
pub fn init(config: ProbeConfig) -> &'static ProbeSelector {
    SELECTOR.get_or_init(|| ProbeSelector::new(config))
}

pub struct ProbeSelector {
    targets: Vec<(AtomicBool, TargetConfig)>,
}

#[derive(Debug)]
pub struct Target<'d> {
    config: TargetConfig,
    taken: &'d AtomicBool,
}

impl ProbeSelector {
    fn new(config: ProbeConfig) -> Self {
        let mut targets = Vec::new();
        for t in config.targets {
            targets.push((AtomicBool::new(false), t));
        }
        Self { targets }
    }

    /// Select a target with the provided labels
    pub fn select<'m>(&'m self, labels: &[(&str, &str)]) -> Option<Target<'m>> {
        for (taken, config) in &self.targets {
            let mut matched = true;
            for (key, value) in labels {
                let v = config.labels.get(*key);
                if let Some(v) = v {
                    if v != value {
                        matched = false;
                        break;
                    }
                }
            }
            if matched && taken.swap(true, Ordering::Acquire) == false {
                return Some(Target {
                    config: config.clone(),
                    taken,
                });
            }
        }
        None
    }
}

impl<'d> Target<'d> {
    /// Probe identifier
    pub fn config(&self) -> &TargetConfig {
        &self.config
    }
}

impl<'d> Drop for Target<'d> {
    fn drop(&mut self) {
        self.taken.store(false, Ordering::Release);
    }
}
