//! Leviathan placement scheduler.
//!
//! The scheduler watches for `Pending` containers and selects the best worker
//! node based on available resources, affinity rules, and cluster topology.
//! It is the direct analogue of the `kube-scheduler` in Kubernetes.
//!
//! # Architecture
//!
//! The scheduler defines a [`Scheduler`] trait and provides a
//! [`FirstFitScheduler`] implementation that uses a simple first-fit
//! placement strategy — iterating nodes and picking the first one with
//! sufficient capacity.
//!
//! More sophisticated strategies (best-fit bin-packing, spread, affinity)
//! will be layered on top of this trait in Day 7.

use leviathan_core::{
    Container, LeviathanError, Node, NodeId, NodeStatus, ResourceSpec,
};

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

/// A simple first-fit scheduler that picks the first `Ready` node with
/// sufficient resources.
///
/// This is the baseline strategy — O(n) over the node list, no sorting,
/// no scoring. Good enough for small clusters and as a correctness baseline
/// that more advanced strategies are validated against.
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

/// A best-fit scheduler that picks the `Ready` node with the **least**
/// remaining capacity after placement — minimising wasted resources
/// (bin-packing heuristic).
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

            // Score: total remaining capacity after placement (lower is tighter fit).
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

fn main() {
    println!("leviathan-scheduler: placement scheduler");
    println!("  Strategies: FirstFitScheduler, BestFitScheduler");
    println!("  Full integration begins on Day 7.");
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
            format!("id-{}", name),
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
        assert_eq!(result, NodeId::new("node-1")); // First fit
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
        let nodes = vec![
            make_node("node-1", 100, 128, NodeStatus::Ready),
        ];
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
        // small-node has the tightest fit (1000-500 + 1024-512 = 1012)
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
}
