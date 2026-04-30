from __future__ import annotations

from .config import DEFAULT_WEIGHTS, TRIGGER_RULES
from .models import ClusterEntity, ClusterExpansion, ClusterScoring, EventCandidate, clamp_score


def branch_average(nodes: list[ClusterEntity], priority_weight: float = 0.6) -> float:
    if not nodes:
        return 0.0
    confidence_weight = 1.0 - priority_weight
    return sum(node.priority_score * priority_weight + node.confidence_score * confidence_weight for node in nodes) / len(nodes)


def score_cluster(event: EventCandidate, expansion: ClusterExpansion) -> ClusterScoring:
    weights = TRIGGER_RULES.get(event.trigger_type, DEFAULT_WEIGHTS)

    peer_score = branch_average(expansion.peer_nodes)
    ownership_score = branch_average(expansion.ownership_nodes)
    governance_score = weights.get("governance_weight", 0.7) * event.event_urgency_score

    event_base = (
        0.30 * event.event_severity_score
        + 0.20 * event.event_urgency_score
        + 0.20 * event.event_confidence_score
        + 0.15 * (100.0 if event.primary_source_urls else 50.0)
        + 0.15 * (85.0 if event.service_hypotheses else 40.0)
    )

    propagation = (
        0.50 * peer_score * weights.get("peer_weight", 0.7)
        + 0.35 * ownership_score * weights.get("ownership_weight", 0.7)
        + 0.15 * governance_score
    )

    priority = clamp_score(0.55 * event_base + 0.45 * propagation)
    confidence = clamp_score(
        0.45 * event.event_confidence_score
        + 0.30 * expansion.cluster_confidence_score
        + 0.15 * peer_score
        + 0.10 * ownership_score
    )

    return ClusterScoring(
        peer_branch_score=clamp_score(peer_score),
        ownership_branch_score=clamp_score(ownership_score),
        governance_branch_score=clamp_score(governance_score),
        cluster_priority_score=priority,
        cluster_confidence_score=confidence,
    )
