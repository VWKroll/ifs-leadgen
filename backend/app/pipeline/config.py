from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ..settings import settings

ALLOWED_TRIGGERS = [
    "regulatory_inquiry",
    "enforcement",
    "insolvency",
    "m_and_a",
    "sanctions",
    "compliance_failure",
    "governance_issue",
    "financing",
]

DEFAULT_WEIGHTS = {"peer_weight": 0.70, "ownership_weight": 0.70, "governance_weight": 0.70}

TRIGGER_RULES = {
    "regulatory_inquiry": {"peer_weight": 0.90, "ownership_weight": 0.80, "governance_weight": 0.80},
    "enforcement": {"peer_weight": 0.95, "ownership_weight": 0.85, "governance_weight": 0.90},
    "insolvency": {"peer_weight": 0.70, "ownership_weight": 0.75, "governance_weight": 0.70},
    "m_and_a": {"peer_weight": 0.45, "ownership_weight": 0.85, "governance_weight": 0.55},
    "sanctions": {"peer_weight": 0.95, "ownership_weight": 0.85, "governance_weight": 0.90},
    "compliance_failure": {"peer_weight": 0.90, "ownership_weight": 0.80, "governance_weight": 0.85},
    "governance_issue": {"peer_weight": 0.60, "ownership_weight": 0.70, "governance_weight": 0.95},
}

PEER_WEIGHTS = {
    "sector_subsector_similarity": 0.25,
    "geography_regulatory_overlap": 0.20,
    "business_model_similarity": 0.20,
    "exposure_similarity": 0.20,
    "size_complexity_similarity": 0.10,
    "ownership_model_similarity": 0.05,
}


@dataclass(slots=True)
class PipelineConfig:
    research_mode: Literal["region", "company"]
    target_region: str
    company_name: str | None
    recency_days: int
    dedup_days: int
    max_peers: int
    max_ownership_nodes: int
    openai_model: str

    @classmethod
    def from_settings_row(cls, row: dict | None) -> "PipelineConfig":
        return cls(
            research_mode=str((row or {}).get("research_mode") or "region"),
            target_region=str((row or {}).get("target_region") or settings.pipeline_target_region),
            company_name=str((row or {}).get("company_name") or "").strip() or None,
            recency_days=int((row or {}).get("recency_days") or settings.pipeline_recency_days),
            dedup_days=int((row or {}).get("dedup_days") or settings.pipeline_dedup_days),
            max_peers=int((row or {}).get("max_peers") or settings.pipeline_max_peers),
            max_ownership_nodes=int((row or {}).get("max_ownership_nodes") or settings.pipeline_max_ownership_nodes),
            openai_model=str((row or {}).get("openai_model") or settings.openai_model),
        )
