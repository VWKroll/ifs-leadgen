from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from backend.app.knowledge_base import (
    KnowledgeDocument,
    _remote_attributes,
    _sync_remote_document,
    build_cluster_markdown,
    build_country_markdown,
    build_entity_markdown,
    build_region_markdown,
    build_source_markdown,
    sync_knowledge_base,
)
from backend.app.pipeline.provider import AzureOpenAICapabilities
from backend.app.schemas import (
    KnowledgeGraphCountrySummary,
    KnowledgeGraphDistributionItem,
    KnowledgeGraphEventSummary,
    KnowledgeGraphRegionSummary,
    OpportunityDetail,
    OpportunitySummary,
)


def make_detail() -> OpportunityDetail:
    return OpportunityDetail(
        cluster=OpportunitySummary(
            cluster_id="cluster-1",
            subject_company_name="Acme Holdings",
            subject_country="United Kingdom",
            subject_region="Europe",
            trigger_type="insolvency",
            trigger_subtype="administration",
            event_date="2026-04-01",
            event_summary="A distressed event with immediate advisory implications.",
            propagation_thesis="Pressure on lenders and sponsors may widen the opportunity set.",
            cluster_priority_score=88.0,
            cluster_confidence_score=77.0,
            opportunity_score=92.0,
            headline_source_url="https://example.org/headline",
        ),
        graph_nodes=[],
        graph_edges=[],
        entities=[
            {
                "cluster_entity_id": "entity-1",
                "entity_name": "Acme Holdings",
                "entity_type": "subject_company",
                "branch_type": "direct",
                "commercial_role": "direct_buyer",
                "evidence_type": "direct_evidence",
                "priority_score": 91,
                "confidence_score": 84,
                "relationship_to_subject": "The impacted company itself.",
                "rationale": "Direct distress signal.",
                "source_urls_json": '["https://example.org/source-1"]',
                "source_snippets_json": '["A source snippet."]',
            }
        ],
        recommendations=[
            {
                "entity_name": "Acme Holdings",
                "entity_type": "subject_company",
                "role_track_type": "management_execution",
                "role_confidence_score": 82,
                "rationale": "Execution risk is high.",
                "recommended_titles_json": '["CFO"]',
                "departments_json": '["Finance"]',
                "seniority_levels_json": '["C-Level"]',
            }
        ],
        sources=[
            {
                "cluster_source_id": "source-1",
                "cluster_entity_id": "entity-1",
                "source_url": "https://example.org/source-1",
                "source_type": "web",
                "source_title": "Headline",
                "publisher": "Example",
                "used_for": "direct_inference",
                "published_at": "2026-04-01",
            }
        ],
    )


class KnowledgeBaseTests(unittest.TestCase):
    def test_remote_attributes_compact_oversized_json_list_string(self) -> None:
        value = '["' + '","'.join(f"source-{index:03d}" for index in range(80)) + '"]'

        compacted = _remote_attributes({"source_ids": value, "cluster_id": "cluster-1"})

        self.assertLessEqual(len(str(compacted["source_ids"])), 512)
        self.assertEqual(compacted["cluster_id"], "cluster-1")
        self.assertIn("truncated", str(compacted["source_ids"]))

    def test_build_cluster_markdown_contains_expected_sections(self) -> None:
        with patch("backend.app.knowledge_base.get_opportunity_detail", return_value=make_detail()), patch(
            "backend.app.knowledge_base._cluster_run_metadata",
            return_value={"cluster_id": "cluster-1", "run_id": "run-123"},
        ), patch("backend.app.knowledge_base.list_chat_note_records", return_value=[]):
            document = build_cluster_markdown("cluster-1")

        self.assertEqual(document.cluster_id, "cluster-1")
        self.assertIn("# Opportunity Cluster: Acme Holdings", document.content)
        self.assertIn("## Event Summary", document.content)
        self.assertIn("## Entities", document.content)
        self.assertIn("## Role Recommendations", document.content)
        self.assertIn("## Sources", document.content)
        self.assertIn("## Analyst Notes", document.content)
        self.assertIn("Source Run ID: run-123", document.content)

    def test_build_focus_documents_include_entity_and_source_metadata(self) -> None:
        detail = make_detail()
        with patch("backend.app.knowledge_base.get_opportunity_detail", return_value=detail):
            entity_document = build_entity_markdown("cluster-1", "entity-1", detail=detail)
            source_document = build_source_markdown("cluster-1", "source-1", detail=detail)

        self.assertEqual(entity_document.document_kind, "entity")
        self.assertEqual(entity_document.entity_id, "entity-1")
        self.assertEqual(entity_document.attributes["entity_id"], "entity-1")
        self.assertIn("## Role Recommendations", entity_document.content)
        self.assertEqual(source_document.document_kind, "source")
        self.assertEqual(source_document.source_id, "source-1")
        self.assertEqual(source_document.linked_entity_id, "entity-1")
        self.assertEqual(source_document.attributes["source_id"], "source-1")
        self.assertIn("## Source Assessment Context", source_document.content)

    def test_build_cluster_markdown_includes_committed_chat_notes(self) -> None:
        with patch("backend.app.knowledge_base.get_opportunity_detail", return_value=make_detail()), patch(
            "backend.app.knowledge_base._cluster_run_metadata",
            return_value={"cluster_id": "cluster-1", "run_id": "run-123"},
        ), patch(
            "backend.app.knowledge_base.list_chat_note_records",
            return_value=[
                {
                    "title": "Distilled account note",
                    "summary_markdown": "## What We Learned\n- Sponsor relationship may matter.",
                    "committed_at": datetime(2026, 4, 7, tzinfo=timezone.utc),
                    "committed_by": "app",
                }
            ],
        ):
            document = build_cluster_markdown("cluster-1")

        self.assertIn("Distilled account note", document.content)
        self.assertIn("Sponsor relationship may matter.", document.content)

    def test_build_geography_documents_include_region_and_country_narratives(self) -> None:
        country = KnowledgeGraphCountrySummary(
            country_id="United Kingdom",
            label="United Kingdom",
            region_id="EMEA",
            narrative="Country-level restructuring pressure is rising.",
            event_count=2,
            company_count=2,
            average_opportunity=88.0,
            average_confidence=73.0,
            dominant_triggers=[
                KnowledgeGraphDistributionItem(label="Insolvency", count=2, trigger_type="insolvency", tone="red")
            ],
            top_companies=["Acme Holdings", "Bravo Group"],
            events=[
                KnowledgeGraphEventSummary(
                    cluster_id="cluster-1",
                    subject_company_name="Acme Holdings",
                    subject_country="United Kingdom",
                    subject_region="EMEA",
                    trigger_type="insolvency",
                    event_date="2026-04-01",
                    event_summary="A distressed event with immediate advisory implications.",
                    opportunity_score=92.0,
                    cluster_confidence_score=77.0,
                )
            ],
        )
        region = KnowledgeGraphRegionSummary(
            region_id="EMEA",
            label="EMEA",
            narrative="Regional restructuring activity is clustering in a few core markets.",
            event_count=2,
            country_count=1,
            company_count=2,
            average_opportunity=88.0,
            average_confidence=73.0,
            dominant_triggers=[
                KnowledgeGraphDistributionItem(label="Insolvency", count=2, trigger_type="insolvency", tone="red")
            ],
            countries=[country],
        )

        region_document = build_region_markdown(region)
        country_document = build_country_markdown(country)

        self.assertEqual(region_document.document_kind, "region")
        self.assertIn("Regional Narrative: EMEA", region_document.content)
        self.assertIn("## Countries", region_document.content)
        self.assertEqual(country_document.document_kind, "country")
        self.assertIn("Country Narrative: United Kingdom", country_document.content)
        self.assertIn("## Priority Events", country_document.content)

    def test_sync_knowledge_base_skips_remote_upload_for_unchanged_documents(self) -> None:
        cluster_document = KnowledgeDocument(
            document_id="cluster-1:cluster",
            cluster_id="cluster-1",
            document_kind="cluster",
            title="Cluster",
            file_path="clusters/cluster-1.md",
            content="cluster markdown",
            content_sha="sha-cluster",
            source_run_id="run-1",
            attributes={"cluster_id": "cluster-1", "document_kind": "cluster"},
        )
        manifest_document = KnowledgeDocument(
            document_id="__manifest__:manifest",
            cluster_id="__manifest__",
            document_kind="manifest",
            title="Manifest",
            file_path="manifests/latest.md",
            content="manifest markdown",
            content_sha="sha-manifest",
            source_run_id=None,
            attributes={"document_kind": "manifest"},
        )
        existing_cluster = {
            "cluster_id": "cluster-1",
            "document_kind": "cluster",
            "file_path": "/tmp/cluster-1.md",
            "content_sha": "sha-cluster",
            "source_run_id": "run-1",
            "vector_store_id": "vs_1",
            "uploaded_file_id": "file_1",
            "vector_store_file_id": "vsfile_1",
            "sync_status": "synced",
            "synced_at": datetime.now(timezone.utc),
            "error_message": None,
        }
        existing_manifest = {
            **existing_cluster,
            "cluster_id": "__manifest__",
            "document_kind": "manifest",
            "content_sha": "sha-manifest",
            "file_path": "/tmp/latest.md",
        }

        with patch("backend.app.knowledge_base._documents_to_sync", return_value=[cluster_document, manifest_document]), patch(
            "backend.app.knowledge_base.get_azure_capabilities",
            return_value=AzureOpenAICapabilities(True, True, True, "ok"),
        ), patch("backend.app.knowledge_base._ensure_vector_store_id", return_value="vs_1"), patch(
            "backend.app.knowledge_base._write_markdown"
        ), patch(
            "backend.app.knowledge_base.get_kb_document_record",
            side_effect=[existing_cluster, existing_manifest],
        ), patch(
            "backend.app.knowledge_base._sync_remote_document"
        ) as sync_remote, patch(
            "backend.app.knowledge_base.save_kb_document_record"
        ), patch(
            "backend.app.knowledge_base.save_pipeline_settings_record"
        ), patch(
            "backend.app.knowledge_base.count_kb_document_records",
            return_value=1,
        ):
            result = sync_knowledge_base(cluster_id="cluster-1")

        sync_remote.assert_not_called()
        self.assertEqual(result.status, "ready")

    def test_sync_remote_document_uses_compacted_remote_attributes(self) -> None:
        document = KnowledgeDocument(
            document_id="cluster-1:cluster",
            cluster_id="cluster-1",
            document_kind="cluster",
            title="Cluster",
            file_path="clusters/cluster-1.md",
            content="cluster markdown",
            content_sha="sha-cluster",
            source_run_id="run-1",
            attributes={
                "cluster_id": "cluster-1",
                "document_kind": "cluster",
                "source_ids": '["' + '","'.join(f"source-{index:03d}" for index in range(80)) + '"]',
            },
        )

        class FakeFiles:
            def create(self, *, file, purpose):
                self.seen_name = getattr(file, "name", None)
                self.seen_purpose = purpose
                return type("UploadedFile", (), {"id": "file_1"})()

        class FakeVectorStoreFiles:
            def __init__(self):
                self.attributes = None

            def create_and_poll(self, *, file_id, vector_store_id, attributes, poll_interval_ms):
                self.attributes = attributes
                return type("VectorStoreFile", (), {"id": "vsfile_1", "status": "completed", "last_error": None})()

        fake_files = FakeFiles()
        fake_vs_files = FakeVectorStoreFiles()
        fake_client = type("Client", (), {"files": fake_files, "vector_stores": type("VS", (), {"files": fake_vs_files})()})()

        from pathlib import Path

        test_path = Path("/tmp/idc-kb-test-cluster.md")
        test_path.write_text("cluster markdown", encoding="utf-8")

        with patch("backend.app.knowledge_base.get_azure_client", return_value=fake_client), patch(
            "backend.app.knowledge_base._resolve_storage_path"
        ) as resolve_path:
            resolve_path.return_value = test_path
            record = _sync_remote_document(
                document,
                existing=None,
                vector_store_id="vs_1",
            )

        self.assertEqual(record["sync_status"], "synced")
        self.assertIsNotNone(fake_vs_files.attributes)
        self.assertLessEqual(len(str(fake_vs_files.attributes["source_ids"])), 512)

    def test_sync_knowledge_base_surfaces_remote_failures(self) -> None:
        cluster_document = KnowledgeDocument(
            document_id="cluster-1:cluster",
            cluster_id="cluster-1",
            document_kind="cluster",
            title="Cluster",
            file_path="clusters/cluster-1.md",
            content="cluster markdown",
            content_sha="sha-cluster",
            source_run_id="run-1",
            attributes={"cluster_id": "cluster-1", "document_kind": "cluster"},
        )
        failed_record = {
            "cluster_id": "cluster-1",
            "document_kind": "cluster",
            "file_path": "/tmp/cluster-1.md",
            "content_sha": "sha-cluster",
            "source_run_id": "run-1",
            "vector_store_id": "vs_1",
            "uploaded_file_id": "file_1",
            "vector_store_file_id": "vsfile_1",
            "sync_status": "failed",
            "synced_at": datetime.now(timezone.utc),
            "error_message": "Upload failed",
        }

        with patch("backend.app.knowledge_base._documents_to_sync", return_value=[cluster_document]), patch(
            "backend.app.knowledge_base.get_azure_capabilities",
            return_value=AzureOpenAICapabilities(True, True, True, "ok"),
        ), patch("backend.app.knowledge_base._ensure_vector_store_id", return_value="vs_1"), patch(
            "backend.app.knowledge_base._write_markdown"
        ), patch(
            "backend.app.knowledge_base.get_kb_document_record",
            return_value=None,
        ), patch(
            "backend.app.knowledge_base._sync_remote_document",
            return_value=failed_record,
        ), patch(
            "backend.app.knowledge_base.save_kb_document_record"
        ), patch(
            "backend.app.knowledge_base.save_pipeline_settings_record"
        ), patch(
            "backend.app.knowledge_base.count_kb_document_records",
            return_value=1,
        ):
            result = sync_knowledge_base(cluster_id="cluster-1")

        self.assertEqual(result.status, "fallback")
        self.assertEqual(result.last_error, "Upload failed")


if __name__ == "__main__":
    unittest.main()
