//! # leviathan-scheduler
//!
//! Placement scheduler for the Leviathan platform.
//!
//! Provides pluggable scheduling strategies including:
//! - [`FirstFitScheduler`] — O(n) baseline, first node with capacity
//! - [`BestFitScheduler`] — Bin-packing, tightest remaining capacity
//! - [`FirstFitDecreasingScheduler`] — FFD bin-packing on normalized vectors
//! - [`Scorer`] trait — Pluggable affinity/anti-affinity scoring
//!
//! Scheduling decisions are designed to be replicated through Raft consensus
//! via the [`SchedulingDecision`] type.

#![warn(missing_docs)]

use leviathan_core::{
    Container, LeviathanError, Node, NodeId, NodeStatus, ResourceSpec,
};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Scheduler trait
// ---------------------------------------------------------------------------

/// A placement strategy for scheduling containers onto nodes.
///
/// Implementations receive the full set of nodes and a container to place,
/// and must return the ID of the selected node or an error.
pub trait Scheduler {
    /// Select a node for the given container.
    ///
    /// # Errors
    ///
    /// Returns [`LeviathanError::NoSchedulableNode`] if no node has sufficient
    /// resources or meets the scheduling constraints.
    fn select_node(
        &self,
        nodes: &[Node],
        container: &Container,
    ) -> Result<NodeId, LeviathanError>;
}

// ---------------------------------------------------------------------------
// Scorer trait — pluggable affinity/anti-affinity
// ---------------------------------------------------------------------------

/// A pluggable scoring function for scheduling decisions.
///
/// Scorers evaluate how suitable a node is for a given container beyond
/// simple resource capacity. Use cases include:
/// - **Affinity**: prefer nodes that already run related containers
/// - **Anti-affinity**: avoid co-locating competing workloads
/// - **Data locality**: prefer nodes near the data source
/// - **Zone spread**: distribute replicas across failure domains
pub trait Scorer: Send + Sync {
    /// Score a node for a container. Higher scores are preferred.
    ///
    /// The score should be in the range `[0.0, 1.0]` for normalization.
    fn score(&self, node: &Node, container: &Container) -> f64;

    /// Human-readable name of this scorer for logging.
    fn name(&self) -> &str;
}

/// A no-op scorer that gives all nodes equal weight.
#[derive(Debug, Default)]
pub struct DefaultScorer;

impl Scorer for DefaultScorer {
    fn score(&self, _node: &Node, _container: &Container) -> f64 {
        1.0
    }

    fn name(&self) -> &str {
        "default"
    }
}

// ---------------------------------------------------------------------------
// SchedulingDecision — Raft-replicable placement
// ---------------------------------------------------------------------------

/// A scheduling decision that can be replicated through Raft.
///
/// Contains the container ID, selected node, and the term in which
/// the decision was made. Serializable for WAL persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulingDecision {
    /// Container being scheduled.
    pub container_id: String,
    /// Selected node ID.
    pub node_id: String,
    /// Scheduling timestamp (epoch millis).
    pub timestamp_ms: u64,
    /// Resource spec for the container.
    pub resources: ResourceSpec,
}

// ---------------------------------------------------------------------------
// FirstFitScheduler
// ---------------------------------------------------------------------------

/// A simple first-fit scheduler that picks the first `Ready` node with
/// sufficient resources.
///
/// O(n) over the node list, no sorting, no scoring.
#[derive(Debug, Default)]
pub struct FirstFitScheduler;

impl Scheduler for FirstFitScheduler {
    fn select_node(
        &self,
        nodes: &[Node],
        container: &Container,
    ) -> Result<NodeId, LeviathanError> {
        for node in nodes {
            if node.status != NodeStatus::Ready {
                continue;
            }
            if container.resources.fits_within(&node.resources) {
                return Ok(node.id.clone());
            }
        }

        Err(LeviathanError::NoSchedulableNode {
            reason: format!(
                "no Ready node can satisfy {} for container '{}'",
                container.resources, container.name
            ),
        })
    }
}

// ---------------------------------------------------------------------------
// BestFitScheduler
// ---------------------------------------------------------------------------

/// A best-fit scheduler that picks the `Ready` node with the **least**
/// remaining capacity after placement — minimising wasted resources.
#[derive(Debug, Default)]
pub struct BestFitScheduler;

impl Scheduler for BestFitScheduler {
    fn select_node(
        &self,
        nodes: &[Node],
        container: &Container,
    ) -> Result<NodeId, LeviathanError> {
        let mut best: Option<(&Node, u64)> = None;

        for node in nodes {
            if node.status != NodeStatus::Ready {
                continue;
            }
            if !container.resources.fits_within(&node.resources) {
                continue;
            }

            let remaining = ResourceSpec {
                cpu_millicores: node
                    .resources
                    .cpu_millicores
                    .saturating_sub(container.resources.cpu_millicores),
                memory_mib: node
                    .resources
                    .memory_mib
                    .saturating_sub(container.resources.memory_mib),
            };
            let score = remaining.cpu_millicores + remaining.memory_mib;

            match &best {
                Some((_, best_score)) if score >= *best_score => {}
                _ => best = Some((node, score)),
            }
        }

        best.map(|(node, _)| node.id.clone())
            .ok_or_else(|| LeviathanError::NoSchedulableNode {
                reason: format!(
                    "no Ready node can satisfy {} for container '{}'",
                    container.resources, container.name
                ),
            })
    }
}

// ---------------------------------------------------------------------------
// FirstFitDecreasingScheduler — FFD bin-packing
// ---------------------------------------------------------------------------

/// First-Fit Decreasing bin-packing scheduler.
///
/// Sorts candidate nodes by remaining capacity (descending on the normalized
/// CPU+memory vector), then places the container on the first node that fits.
/// Combined with a pluggable [`Scorer`], this supports affinity/anti-affinity.
///
/// # Algorithm
///
/// 1. Filter to `Ready` nodes with sufficient capacity.
/// 2. Score each candidate using the configured [`Scorer`].
/// 3. Sort by `(score descending, remaining capacity ascending)`.
/// 4. Return the first (best-scoring, tightest-fit) node.
pub struct FirstFitDecreasingScheduler {
    /// Pluggable scorer for affinity/anti-affinity rules.
    scorer: Box<dyn Scorer>,
}

impl FirstFitDecreasingScheduler {
    /// Create a new FFD scheduler with the given scorer.
    pub fn new(scorer: Box<dyn Scorer>) -> Self {
        Self { scorer }
    }

    /// Create a new FFD scheduler with the default (uniform) scorer.
    #[must_use]
    pub fn with_default_scorer() -> Self {
        Self {
            scorer: Box::new(DefaultScorer),
        }
    }

    /// Normalize a resource spec to a `[0.0, 1.0]` vector relative to
    /// the maximum capacity in the cluster.
    fn normalize(spec: &ResourceSpec, max_cpu: u64, max_mem: u64) -> f64 {
        let cpu_norm = if max_cpu > 0 {
            spec.cpu_millicores as f64 / max_cpu as f64
        } else {
            0.0
        };
        let mem_norm = if max_mem > 0 {
            spec.memory_mib as f64 / max_mem as f64
        } else {
            0.0
        };
        // Combined score — equal weight to CPU and memory.
        (cpu_norm + mem_norm) / 2.0
    }
}

impl Scheduler for FirstFitDecreasingScheduler {
    fn select_node(
        &self,
        nodes: &[Node],
        container: &Container,
    ) -> Result<NodeId, LeviathanError> {
        // Find max capacity for normalization.
        let max_cpu = nodes.iter().map(|n| n.resources.cpu_millicores).max().unwrap_or(1);
        let max_mem = nodes.iter().map(|n| n.resources.memory_mib).max().unwrap_or(1);

        // Filter and score candidates.
        let mut candidates: Vec<(&Node, f64, f64)> = nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Ready)
            .filter(|n| container.resources.fits_within(&n.resources))
            .map(|n| {
                let remaining = ResourceSpec {
                    cpu_millicores: n.resources.cpu_millicores
                        .saturating_sub(container.resources.cpu_millicores),
                    memory_mib: n.resources.memory_mib
                        .saturating_sub(container.resources.memory_mib),
                };
                let remaining_score = Self::normalize(&remaining, max_cpu, max_mem);
                let affinity_score = self.scorer.score(n, container);
                (n, affinity_score, remaining_score)
            })
            .collect();

        if candidates.is_empty() {
            return Err(LeviathanError::NoSchedulableNode {
                reason: format!(
                    "no Ready node can satisfy {} for container '{}'",
                    container.resources, container.name
                ),
            });
        }

        // Sort: highest affinity score first, then tightest fit (lowest remaining).
        candidates.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal))
        });

        Ok(candidates[0].0.id.clone())
    }
}

impl std::fmt::Debug for FirstFitDecreasingScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FirstFitDecreasingScheduler")
            .field("scorer", &self.scorer.name())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: &str, cpu: u64, mem: u64, status: NodeStatus) -> Node {
        let mut node = Node::new(id, "127.0.0.1:7000", ResourceSpec::new(cpu, mem));
        node.status = status;
        node
    }

    fn make_container(name: &str, cpu: u64, mem: u64) -> Container {
        Container::new(
            format!("id-{name}"),
            name,
            "ubuntu:22.04",
            ResourceSpec::new(cpu, mem),
        )
    }

    // ---- FirstFitScheduler ----

    #[test]
    fn first_fit_selects_first_viable_node() {
        let nodes = vec![
            make_node("node-1", 1000, 1024, NodeStatus::Ready),
            make_node("node-2", 2000, 4096, NodeStatus::Ready),
        ];
        let container = make_container("app", 500, 512);

        let scheduler = FirstFitScheduler;
        let result = scheduler.select_node(&nodes, &container).unwrap();
        assert_eq!(result, NodeId::new("node-1"));
    }

    #[test]
    fn first_fit_skips_not_ready_nodes() {
        let nodes = vec![
            make_node("node-1", 4000, 8192, NodeStatus::NotReady),
            make_node("node-2", 2000, 4096, NodeStatus::Ready),
        ];
        let container = make_container("app", 500, 512);

        let scheduler = FirstFitScheduler;
        let result = scheduler.select_node(&nodes, &container).unwrap();
        assert_eq!(result, NodeId::new("node-2"));
    }

    #[test]
    fn first_fit_returns_error_when_no_capacity() {
        let nodes = vec![make_node("node-1", 100, 128, NodeStatus::Ready)];
        let container = make_container("big-app", 4000, 8192);

        let scheduler = FirstFitScheduler;
        let result = scheduler.select_node(&nodes, &container);
        assert!(result.is_err());
    }

    #[test]
    fn first_fit_returns_error_on_empty_nodes() {
        let scheduler = FirstFitScheduler;
        let container = make_container("app", 500, 512);
        let result = scheduler.select_node(&[], &container);
        assert!(result.is_err());
    }

    // ---- BestFitScheduler ----

    #[test]
    fn best_fit_selects_tightest_node() {
        let nodes = vec![
            make_node("big-node", 8000, 16384, NodeStatus::Ready),
            make_node("small-node", 1000, 1024, NodeStatus::Ready),
            make_node("medium-node", 2000, 2048, NodeStatus::Ready),
        ];
        let container = make_container("app", 500, 512);

        let scheduler = BestFitScheduler;
        let result = scheduler.select_node(&nodes, &container).unwrap();
        assert_eq!(result, NodeId::new("small-node"));
    }

    #[test]
    fn best_fit_skips_insufficient_nodes() {
        let nodes = vec![
            make_node("tiny", 100, 128, NodeStatus::Ready),
            make_node("adequate", 2000, 2048, NodeStatus::Ready),
        ];
        let container = make_container("app", 500, 512);

        let scheduler = BestFitScheduler;
        let result = scheduler.select_node(&nodes, &container).unwrap();
        assert_eq!(result, NodeId::new("adequate"));
    }

    #[test]
    fn best_fit_returns_error_when_all_insufficient() {
        let nodes = vec![
            make_node("tiny-1", 100, 128, NodeStatus::Ready),
            make_node("tiny-2", 200, 256, NodeStatus::Ready),
        ];
        let container = make_container("big-app", 4000, 8192);

        let scheduler = BestFitScheduler;
        let result = scheduler.select_node(&nodes, &container);
        assert!(result.is_err());
    }

    // ---- FirstFitDecreasingScheduler ----

    #[test]
    fn ffd_selects_tightest_fit() {
        let nodes = vec![
            make_node("big-node", 8000, 16384, NodeStatus::Ready),
            make_node("small-node", 1000, 1024, NodeStatus::Ready),
            make_node("medium-node", 2000, 2048, NodeStatus::Ready),
        ];
        let container = make_container("app", 500, 512);

        let scheduler = FirstFitDecreasingScheduler::with_default_scorer();
        let result = scheduler.select_node(&nodes, &container).unwrap();
        // With default scorer (uniform), FFD should pick tightest fit.
        assert_eq!(result, NodeId::new("small-node"));
    }

    #[test]
    fn ffd_with_custom_scorer() {
        struct PreferBigNodes;
        impl Scorer for PreferBigNodes {
            fn score(&self, node: &Node, _container: &Container) -> f64 {
                // Prefer nodes with more CPU.
                node.resources.cpu_millicores as f64 / 10000.0
            }
            fn name(&self) -> &str {
                "prefer-big-nodes"
            }
        }

        let nodes = vec![
            make_node("big-node", 8000, 16384, NodeStatus::Ready),
            make_node("small-node", 1000, 1024, NodeStatus::Ready),
        ];
        let container = make_container("app", 500, 512);

        let scheduler = FirstFitDecreasingScheduler::new(Box::new(PreferBigNodes));
        let result = scheduler.select_node(&nodes, &container).unwrap();
        // Custom scorer prefers big nodes.
        assert_eq!(result, NodeId::new("big-node"));
    }

    #[test]
    fn ffd_empty_nodes() {
        let scheduler = FirstFitDecreasingScheduler::with_default_scorer();
        let container = make_container("app", 500, 512);
        assert!(scheduler.select_node(&[], &container).is_err());
    }

    #[test]
    fn scheduling_decision_serialization() {
        let decision = SchedulingDecision {
            container_id: "c-1".into(),
            node_id: "node-1".into(),
            timestamp_ms: 1234567890,
            resources: ResourceSpec::new(1000, 512),
        };
        let json = serde_json::to_string(&decision).expect("serialize");
        let back: SchedulingDecision = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.container_id, "c-1");
    }
}
