from __future__ import annotations

import re
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from .config import ALLOWED_TRIGGERS

VALID_URL_RE = re.compile(r"^https?://[a-zA-Z0-9][a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$")
PLACEHOLDER_DOMAINS = {"example.com", "example.org", "test.com", "localhost"}
LEGAL_SUFFIX_RE = re.compile(
    r"\b(ltd|limited|plc|ag|sa|nv|s\.?p\.?a\.?|spa|srl|gmbh|bv|llc|inc|corp|corporation|group)\b",
    re.IGNORECASE,
)

TRIGGER_ALIASES = {
    "m&a": "m_and_a",
    "m_and_a": "m_and_a",
    "regulatory inquiry": "regulatory_inquiry",
    "compliance failure": "compliance_failure",
    "governance issue": "governance_issue",
}


def clamp_score(value: float) -> float:
    return max(0.0, min(100.0, float(value)))


def normalize_company_name(name: str) -> str:
    if not name:
        return ""
    cleaned = LEGAL_SUFFIX_RE.sub("", name.lower()).strip()
    return " ".join(cleaned.split())


def normalize_trigger_type(value: str) -> str:
    normalized = value.strip().lower().replace("&", "and").replace("-", "_").replace(" ", "_")
    normalized = TRIGGER_ALIASES.get(normalized, normalized)
    return normalized


def validate_source_urls(urls: list[str], field_name: str, min_required: int = 0) -> list[str]:
    if len(urls) < min_required:
        raise ValueError(f"{field_name} must contain at least {min_required} URL(s); got {len(urls)}")
    for url in urls:
        if not VALID_URL_RE.match(url):
            raise ValueError(f"Malformed URL in {field_name}: {url}")
        domain = url.split("//", 1)[-1].split("/", 1)[0].lower()
        if domain in PLACEHOLDER_DOMAINS:
            raise ValueError(f"Placeholder domain not allowed in {field_name}: {url}")
    return urls


class EventCandidate(BaseModel):
    event_id: str
    subject_company_name: str
    subject_company_country: str
    subject_company_region: str = "Europe"
    subject_company_state: str | None = None
    subject_company_city: str | None = None
    subject_company_address: str | None = None
    subject_company_latitude: float | None = None
    subject_company_longitude: float | None = None
    trigger_type: str
    trigger_subtype: str
    event_date: str
    event_headline: str
    headline_source_url: str = ""
    event_summary: str
    sector: Optional[str] = None
    subsector: Optional[str] = None
    service_hypotheses: list[str] = Field(default_factory=list)
    primary_source_urls: list[str] = Field(default_factory=list)
    secondary_source_urls: list[str] = Field(default_factory=list)
    event_confidence_score: float = 0.0
    event_severity_score: float = 0.0
    event_urgency_score: float = 0.0
    dedupe_keys: list[str] = Field(default_factory=list)

    @field_validator("trigger_type")
    @classmethod
    def validate_trigger_type(cls, value: str) -> str:
        normalized = normalize_trigger_type(value)
        if normalized not in ALLOWED_TRIGGERS:
            raise ValueError(f"trigger_type '{value}' not in allowed trigger types")
        return normalized

    @field_validator("headline_source_url")
    @classmethod
    def validate_headline_url(cls, value: str) -> str:
        if value:
            return validate_source_urls([value], "headline_source_url", min_required=1)[0]
        return value

    @field_validator("primary_source_urls")
    @classmethod
    def validate_primary_urls(cls, value: list[str]) -> list[str]:
        return validate_source_urls(value, "primary_source_urls", min_required=1)

    @field_validator("secondary_source_urls")
    @classmethod
    def validate_secondary_urls(cls, value: list[str]) -> list[str]:
        return validate_source_urls(value, "secondary_source_urls")

    @field_validator("event_confidence_score", "event_severity_score", "event_urgency_score")
    @classmethod
    def validate_scores(cls, value: float) -> float:
        return clamp_score(value)


class ClusterEntity(BaseModel):
    entity_name: str
    entity_type: Literal[
        "subject_company",
        "peer_company",
        "pe_sponsor",
        "current_owner",
        "recent_sponsor",
        "deal_counterparty",
    ]
    entity_country: str | None = None
    entity_region: str | None = None
    entity_state: str | None = None
    entity_city: str | None = None
    entity_address: str | None = None
    entity_latitude: float | None = None
    entity_longitude: float | None = None
    relationship_to_subject: str
    commercial_role: Literal["direct_buyer", "peer_candidate", "sponsor_candidate", "influencer"]
    branch_type: Literal["direct", "peer", "ownership"]
    rationale: str
    evidence_type: Literal["direct_evidence", "structured_inference", "commercial_hypothesis"]
    source_urls: list[str] = Field(default_factory=list)
    source_snippets: list[str] = Field(default_factory=list)
    confidence_score: float = 0.0
    priority_score: float = 0.0

    @field_validator("confidence_score", "priority_score")
    @classmethod
    def validate_scores(cls, value: float) -> float:
        return clamp_score(value)


class ClusterExpansion(BaseModel):
    propagation_thesis: str
    best_route_to_market: str
    direct_node: ClusterEntity
    peer_nodes: list[ClusterEntity] = Field(default_factory=list)
    ownership_nodes: list[ClusterEntity] = Field(default_factory=list)
    cluster_confidence_score: float = 0.0

    @field_validator("cluster_confidence_score")
    @classmethod
    def validate_score(cls, value: float) -> float:
        return clamp_score(value)


class ClusterScoring(BaseModel):
    peer_branch_score: float
    ownership_branch_score: float
    governance_branch_score: float
    cluster_priority_score: float
    cluster_confidence_score: float


class RoleRecommendation(BaseModel):
    entity_name: str
    entity_type: str
    role_track_type: Literal["management_execution", "board_oversight", "sponsor_governance"]
    recommended_titles: list[str]
    departments: list[str]
    seniority_levels: list[str]
    hypothesized_services: list[str] = Field(default_factory=list)
    rationale: str
    role_confidence_score: float

    @field_validator("role_confidence_score")
    @classmethod
    def validate_score(cls, value: float) -> float:
        return clamp_score(value)


class GenerationPipelineResult(BaseModel):
    status: Literal["succeeded", "failed", "skipped"]
    created_cluster_id: str | None = None
    duplicate_skipped: bool = False
    error_message: str | None = None
    event: dict[str, Any] | None = None
