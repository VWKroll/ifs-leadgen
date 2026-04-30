"""Mock data matching real ifs_dev.idc_poc table schemas, for local dev without a live connection."""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta

import pandas as pd

random.seed(42)

TRIGGER_TYPES = ["M&A", "Leadership Change", "Regulatory Filing", "Funding Round", "Expansion", "Restructuring"]
TRIGGER_SUBTYPES = ["Acquisition", "IPO", "CEO Departure", "Board Change", "SEC Filing", "Series B", "Layoff"]
COUNTRIES = ["United States", "United Kingdom", "Germany", "Canada", "Australia"]
REGIONS = ["North America", "EMEA", "APAC", "LATAM"]
ENTITY_TYPES = ["Company", "Individual"]
RELATIONSHIP_TYPES = ["Acquirer", "Target", "Advisor", "Lender", "Regulator", "Competitor"]
COMMERCIAL_ROLES = ["Corporate Finance", "Restructuring", "Compliance", "M&A Advisory", "Valuation"]
BRANCH_TYPES = ["direct", "peer", "ownership", "governance"]
ROUTE_TO_MARKET = ["Direct Outreach", "Channel Partner", "Inbound", "Referral"]
SOURCE_TYPES = ["Press Release", "SEC Filing", "News Article", "LinkedIn", "Earnings Call", "Court Filing"]
PUBLISHERS = ["Reuters", "Bloomberg", "WSJ", "Financial Times", "SEC EDGAR", "PRNewswire"]
ROLE_TRACK_TYPES = ["Primary Decision Maker", "Economic Buyer", "Technical Influencer", "Champion"]


def _random_dt(days_back: int = 90) -> datetime:
    return datetime.now() - timedelta(days=random.randint(0, days_back))


def _random_titles() -> str:
    titles = random.sample(["CFO", "CTO", "VP Finance", "Head of Strategy", "Managing Director", "COO", "General Counsel"], k=random.randint(1, 3))
    return json.dumps(titles)


def mock_clusters(n: int = 40) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "cluster_id": f"CLU-{i:04d}",
            "event_id": f"EVT-{i:04d}",
            "run_id": f"RUN-{random.randint(1, 5):03d}",
            "cluster_created_at": _random_dt(90),
            "subject_company_name": f"{random.choice(['Acme', 'GlobalTech', 'Pinnacle', 'Summit', 'Apex'])} {random.choice(['Corp', 'LLC', 'Inc', 'Partners', 'Group'])}",
            "subject_company_normalized": f"company_{i:04d}",
            "subject_country": random.choice(COUNTRIES),
            "subject_region": random.choice(REGIONS),
            "trigger_type": random.choice(TRIGGER_TYPES),
            "trigger_subtype": random.choice(TRIGGER_SUBTYPES),
            "event_date": _random_dt(180).date(),
            "event_headline": f"{random.choice(TRIGGER_TYPES)} event involving key market player #{i}",
            "event_summary": "Significant corporate event with potential advisory opportunity.",
            "service_hypotheses_json": json.dumps(["M&A Advisory", "Valuation", "Due Diligence"]),
            "event_confidence_score": round(random.uniform(0.4, 1.0), 3),
            "event_severity_score": round(random.uniform(0.3, 1.0), 3),
            "event_urgency_score": round(random.uniform(0.2, 1.0), 3),
            "peer_branch_score": round(random.uniform(0.0, 1.0), 3),
            "ownership_branch_score": round(random.uniform(0.0, 1.0), 3),
            "governance_branch_score": round(random.uniform(0.0, 1.0), 3),
            "cluster_priority_score": round(random.uniform(1.0, 10.0), 2),
            "cluster_confidence_score": round(random.uniform(0.4, 1.0), 3),
            "best_route_to_market": random.choice(ROUTE_TO_MARKET),
            "propagation_thesis": "High-value opportunity based on trigger signals and entity network.",
            "dedupe_fingerprint": f"fp_{i:04d}_{random.randint(1000, 9999)}",
            "headline_source_url": f"https://example.com/news/{i}",
        })
    return pd.DataFrame(rows).sort_values("cluster_priority_score", ascending=False).reset_index(drop=True)


def mock_entities(cluster_ids: list[str]) -> pd.DataFrame:
    rows = []
    for cid in cluster_ids:
        for j in range(random.randint(1, 5)):
            rows.append({
                "cluster_entity_id": f"ENT-{cid}-{j:02d}",
                "cluster_id": cid,
                "entity_name": f"{random.choice(['BlackRock', 'KKR', 'Deloitte', 'KPMG', 'Lazard'])} {random.randint(1, 99)}",
                "entity_name_normalized": f"entity_{j:03d}",
                "entity_type": random.choice(ENTITY_TYPES),
                "relationship_to_subject": random.choice(RELATIONSHIP_TYPES),
                "commercial_role": random.choice(COMMERCIAL_ROLES),
                "branch_type": random.choice(BRANCH_TYPES),
                "rationale": "Entity has significant exposure to the triggering event.",
                "evidence_type": random.choice(["direct_evidence", "structured_inference"]),
                "source_urls_json": json.dumps([f"https://example.com/source/{j}"]),
                "source_snippets_json": json.dumps(["Mentioned in connection with the transaction."]),
                "confidence_score": round(random.uniform(0.5, 1.0), 3),
                "priority_score": round(random.uniform(1.0, 10.0), 2),
                "created_at": _random_dt(30),
            })
    return pd.DataFrame(rows)


def mock_recommendations(cluster_ids: list[str]) -> pd.DataFrame:
    rows = []
    for cid in cluster_ids:
        for k in range(random.randint(1, 3)):
            eid = f"ENT-{cid}-{k:02d}"
            rows.append({
                "role_recommendation_id": f"REC-{cid}-{k:02d}",
                "cluster_id": cid,
                "cluster_entity_id": eid,
                "entity_name": f"Entity {k}",
                "entity_type": random.choice(ENTITY_TYPES),
                "role_track_type": random.choice(ROLE_TRACK_TYPES),
                "recommended_titles_json": _random_titles(),
                "departments_json": json.dumps(random.sample(["Finance", "Strategy", "Legal", "Operations"], k=2)),
                "seniority_levels_json": json.dumps(["C-Suite", "VP"]),
                "rationale": "High engagement probability based on trigger type and entity role.",
                "role_confidence_score": round(random.uniform(0.5, 1.0), 3),
                "created_at": _random_dt(30),
            })
    return pd.DataFrame(rows)


def mock_sources(cluster_ids: list[str]) -> pd.DataFrame:
    rows = []
    for cid in cluster_ids:
        for s in range(random.randint(1, 4)):
            rows.append({
                "cluster_source_id": f"SRC-{cid}-{s:02d}",
                "cluster_id": cid,
                "cluster_entity_id": f"ENT-{cid}-00",
                "source_url": f"https://example.com/article/{s}",
                "source_type": random.choice(SOURCE_TYPES),
                "source_title": f"Breaking: {random.choice(TRIGGER_TYPES)} event surfaces new opportunity",
                "publisher": random.choice(PUBLISHERS),
                "published_at": str(_random_dt(60).date()),
                "used_for": random.choice(["Event Detection", "Entity Extraction", "Scoring"]),
                "retrieved_at": _random_dt(10),
            })
    return pd.DataFrame(rows)
