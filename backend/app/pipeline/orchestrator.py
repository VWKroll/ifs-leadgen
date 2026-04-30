from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Callable

from idc_app.config import TABLES
from idc_app.spark import get_spark

from ..store import append_rows, ensure_pipeline_output_tables, recent_distinct_values
from .config import ALLOWED_TRIGGERS, PipelineConfig
from .models import ClusterExpansion, EventCandidate, GenerationPipelineResult, RoleRecommendation
from .persistence import (
    build_cluster_header,
    build_entity_rows,
    build_role_rows,
    build_source_rows,
    make_event_fingerprint,
)
from .prompts import cluster_expansion_prompts, event_discovery_prompts, role_prompts
from .provider import StructuredModelClient, get_provider_client
from .scoring import score_cluster

StepUpdater = Callable[[str, str], None]


@dataclass(slots=True)
class GenerationPipeline:
    provider: StructuredModelClient

    @classmethod
    def create(cls) -> "GenerationPipeline":
        return cls(provider=get_provider_client())

    def discover_event(self, config: PipelineConfig) -> EventCandidate:
        recent_subjects = (
            []
            if config.research_mode == "company" and config.company_name
            else recent_distinct_values(TABLES["event_clusters"], "subject_company_name", "cluster_created_at", config.dedup_days)
        )
        system_prompt, user_prompt = event_discovery_prompts(
            recent_subjects,
            ALLOWED_TRIGGERS,
            config.recency_days,
            target_region=config.target_region,
            company_name=config.company_name,
        )
        return self.provider.call_json_model(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=EventCandidate,
            model=config.openai_model,
            temperature=0.1,
        )

    def expand_cluster(self, event: EventCandidate, config: PipelineConfig) -> ClusterExpansion:
        system_prompt, user_prompt = cluster_expansion_prompts(event, config.max_peers, config.max_ownership_nodes)
        result = self.provider.call_json_model(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=ClusterExpansion,
            model=config.openai_model,
            temperature=0.2,
        )
        if result.direct_node.entity_type != "subject_company":
            raise ValueError("Direct node must be entity_type='subject_company'")
        result.peer_nodes = result.peer_nodes[: config.max_peers]
        result.ownership_nodes = result.ownership_nodes[: config.max_ownership_nodes]
        return result

    def recommend_roles(self, event: EventCandidate, expansion: ClusterExpansion, config: PipelineConfig) -> list[RoleRecommendation]:
        recommendations: list[RoleRecommendation] = []
        for node in [expansion.direct_node, *expansion.peer_nodes, *expansion.ownership_nodes]:
            system_prompt, user_prompt = role_prompts(event, node)
            recommendation = self.provider.call_json_model(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=RoleRecommendation,
                model=config.openai_model,
                temperature=0.1,
            )
            recommendations.append(recommendation)
        return recommendations

    def run_once(self, *, run_id: str, config: PipelineConfig, update_step: StepUpdater | None = None) -> GenerationPipelineResult:
        update = update_step or (lambda _step, _status: None)

        update("discovery", "running")
        event = self.discover_event(config)
        update("discovery", "succeeded")

        fingerprint = make_event_fingerprint(event.subject_company_name, event.trigger_type, event.event_date, event.event_headline)
        recent_fingerprints = set(
            recent_distinct_values(TABLES["event_clusters"], "dedupe_fingerprint", "cluster_created_at", config.dedup_days)
        )
        if fingerprint in recent_fingerprints:
            update("persistence", "skipped")
            return GenerationPipelineResult(
                status="skipped",
                duplicate_skipped=True,
                event=event.model_dump(),
            )

        update("expansion", "running")
        expansion = self.expand_cluster(event, config)
        update("expansion", "succeeded")

        update("scoring", "running")
        scoring = score_cluster(event, expansion)
        update("scoring", "succeeded")

        update("role_recommendation", "running")
        role_recs = self.recommend_roles(event, expansion, config)
        update("role_recommendation", "succeeded")

        update("persistence", "running")
        ensure_pipeline_output_tables()
        cluster_header = build_cluster_header(run_id, event, expansion, scoring)
        cluster_id = cluster_header["cluster_id"]
        entity_rows = build_entity_rows(cluster_id, expansion)
        role_rows = build_role_rows(cluster_id, entity_rows, role_recs)
        source_rows = build_source_rows(entity_rows, expansion)

        spark = get_spark()
        header_schema = spark.table(TABLES["event_clusters"]).schema
        ordered_header = {field.name: cluster_header.get(field.name) for field in header_schema.fields}

        append_rows(TABLES["event_clusters"], [ordered_header])
        append_rows(TABLES["cluster_entities"], entity_rows)
        append_rows(TABLES["cluster_role_recommendations"], role_rows)
        append_rows(TABLES["cluster_sources"], source_rows)
        update("persistence", "succeeded")

        return GenerationPipelineResult(
            status="succeeded",
            created_cluster_id=cluster_id,
            duplicate_skipped=False,
            event=event.model_dump(),
        )


def run_generation_pipeline(run_id: str, config_row: dict, update_step: StepUpdater | None = None) -> GenerationPipelineResult:
    pipeline = GenerationPipeline.create()
    return pipeline.run_once(run_id=run_id, config=PipelineConfig.from_settings_row(config_row), update_step=update_step)


def make_run_id() -> str:
    return str(uuid.uuid4())
