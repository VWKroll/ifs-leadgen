from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routes import router
from backend.app.chat_service import build_chat_payload, stream_chat_events
from backend.app.schemas import (
    ChatCommitResponse,
    ChatRequest,
    KnowledgeBaseStatus,
    KnowledgeGraphCountrySummary,
    KnowledgeGraphDistributionItem,
    KnowledgeGraphEventSummary,
    KnowledgeGraphRegionSummary,
    KnowledgeGraphResponse,
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
        graph_nodes=[
            {
                "id": "entity:entity-1",
                "type": "company",
                "subtype": "subject_company",
                "label": "Acme Holdings",
                "entity_id": "entity-1",
                "branch_type": "direct",
                "detail": {
                    "relationship_to_subject": "The impacted company itself.",
                    "rationale": "Direct distress signal.",
                },
            }
        ],
        graph_edges=[],
        entities=[
            {
                "cluster_entity_id": "entity-1",
                "entity_name": "Acme Holdings",
                "entity_type": "subject_company",
                "branch_type": "direct",
                "commercial_role": "direct_buyer",
                "relationship_to_subject": "The impacted company itself.",
                "rationale": "Direct distress signal.",
                "source_urls_json": '["https://example.org/source-1"]',
                "source_snippets_json": '["A source snippet."]',
            }
        ],
        recommendations=[],
        sources=[
            {
                "cluster_source_id": "source-1",
                "cluster_entity_id": "entity-1",
                "source_url": "https://example.org/source-1",
                "source_title": "Headline",
                "publisher": "Example",
                "used_for": "direct_inference",
                "published_at": "2026-04-01",
            }
        ],
    )


class ChatRouteTests(unittest.TestCase):
    def test_build_chat_payload_adds_selected_cluster_filter(self) -> None:
        payload = build_chat_payload(
            ChatRequest(
                message="Summarize this cluster",
                selected_cluster_id="cluster-1",
                scope="selected_cluster",
                previous_response_id="resp_123",
                active_tab="sources",
                entity_id="entity-1",
                source_id="source-1",
                graph_node_id="entity:entity-1",
            ),
            vector_store_id="vs_1",
            focused_context="Focused source context:\n- Title: Headline",
        )

        self.assertEqual(payload["previous_response_id"], "resp_123")
        self.assertEqual(payload["tools"][0]["type"], "file_search")
        self.assertEqual(payload["tools"][0]["filters"]["type"], "and")
        self.assertEqual(payload["tools"][0]["filters"]["filters"][0]["key"], "cluster_id")
        self.assertEqual(payload["tools"][0]["filters"]["filters"][0]["value"], "cluster-1")
        self.assertEqual(payload["tools"][0]["filters"]["filters"][1]["key"], "document_kind")
        self.assertEqual(payload["tools"][0]["filters"]["filters"][1]["value"], "source")
        self.assertEqual(payload["tools"][0]["filters"]["filters"][2]["key"], "source_id")
        self.assertEqual(payload["tools"][0]["filters"]["filters"][2]["value"], "source-1")
        text = payload["input"][0]["content"][0]["text"]
        self.assertIn("Active tab: sources.", text)
        self.assertIn("Focused entity ID: entity-1.", text)
        self.assertIn("Focused source ID: source-1.", text)
        self.assertIn("Focused graph node ID: entity:entity-1.", text)
        self.assertIn("Focused source context:", text)

    def test_build_chat_payload_adds_geography_filters_for_global_graph(self) -> None:
        payload = build_chat_payload(
            ChatRequest(
                message="Compare this country to its region",
                scope="all",
                active_tab="global_graph",
                region_id="EMEA",
                country_id="United Kingdom",
            ),
            vector_store_id="vs_1",
            focused_context="Focused country context:\n- Country ID: United Kingdom",
        )

        self.assertEqual(payload["tools"][0]["type"], "file_search")
        self.assertEqual(payload["tools"][0]["filters"]["type"], "or")
        country_filter = payload["tools"][0]["filters"]["filters"][0]
        self.assertEqual(country_filter["filters"][0]["value"], "country")
        self.assertEqual(country_filter["filters"][1]["value"], "United Kingdom")
        self.assertEqual(country_filter["filters"][2]["value"], "EMEA")
        region_filter = payload["tools"][0]["filters"]["filters"][1]
        self.assertEqual(region_filter["filters"][0]["value"], "region")
        self.assertEqual(region_filter["filters"][1]["value"], "EMEA")
        text = payload["input"][0]["content"][0]["text"]
        self.assertIn("Focused region ID: EMEA.", text)
        self.assertIn("Focused country ID: United Kingdom.", text)
        self.assertIn("Focused country context:", text)

    def test_chat_stream_route_returns_sse_payload(self) -> None:
        app = FastAPI()
        app.include_router(router, prefix="/api")
        client = TestClient(app)

        with patch(
            "backend.app.api.routes.stream_chat_events",
            return_value=iter(
                [
                    "event: delta\ndata: {\"text\":\"Hello\"}\n\n",
                    "event: response\ndata: {\"response_id\":\"resp_1\",\"message\":{\"role\":\"assistant\",\"content\":\"Hello\",\"citations\":[]}}\n\n",
                ]
            ),
        ):
            response = client.post(
                "/api/chat/responses/stream",
                json={"message": "Hello", "scope": "all"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("event: delta", response.text)
        self.assertIn("\"response_id\":\"resp_1\"", response.text)

    def test_knowledge_base_sync_route_returns_status_payload(self) -> None:
        app = FastAPI()
        app.include_router(router, prefix="/api")
        client = TestClient(app)

        with patch(
            "backend.app.api.routes.trigger_knowledge_base_sync",
            return_value={
                "status": "ready",
                "last_synced_at": datetime.now(timezone.utc),
                "document_count": 4,
                "vector_store_id": "vs_123",
                "last_error": None,
            },
        ):
            response = client.post("/api/admin/knowledge-base/sync", json={"full_refresh": True})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ready")
        self.assertEqual(response.json()["vector_store_id"], "vs_123")

    def test_knowledge_base_cleanup_route_returns_status_payload(self) -> None:
        app = FastAPI()
        app.include_router(router, prefix="/api")
        client = TestClient(app)

        with patch(
            "backend.app.api.routes.run_knowledge_base_cleanup",
            return_value={
                "status": "ready",
                "last_synced_at": datetime.now(timezone.utc),
                "document_count": 10,
                "cluster_document_count": 2,
                "entity_document_count": 4,
                "source_document_count": 4,
                "duplicate_candidate_count": 0,
                "stale_local_file_count": 1,
                "cleanup_removed_documents": 3,
                "cleanup_removed_files": 1,
                "vector_store_id": "vs_123",
                "last_error": None,
            },
        ):
            response = client.post("/api/admin/knowledge-base/cleanup", json={"mode": "dedupe"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["cleanup_removed_documents"], 3)
        self.assertEqual(response.json()["cleanup_removed_files"], 1)

    def test_knowledge_graph_route_returns_backend_summary(self) -> None:
        app = FastAPI()
        app.include_router(router, prefix="/api")
        client = TestClient(app)
        generated_at = datetime.now(timezone.utc)
        payload = KnowledgeGraphResponse(
            generated_at=generated_at,
            region_count=1,
            country_count=1,
            event_count=1,
            regions=[
                KnowledgeGraphRegionSummary(
                    region_id="EMEA",
                    label="EMEA",
                    narrative="Regional outlook",
                    event_count=1,
                    country_count=1,
                    company_count=1,
                    average_opportunity=91.0,
                    average_confidence=78.0,
                    dominant_triggers=[
                        KnowledgeGraphDistributionItem(label="Insolvency", count=1, trigger_type="insolvency", tone="red")
                    ],
                    countries=[
                        KnowledgeGraphCountrySummary(
                            country_id="United Kingdom",
                            label="United Kingdom",
                            region_id="EMEA",
                            narrative="Country outlook",
                            event_count=1,
                            company_count=1,
                            average_opportunity=91.0,
                            average_confidence=78.0,
                            dominant_triggers=[
                                KnowledgeGraphDistributionItem(label="Insolvency", count=1, trigger_type="insolvency", tone="red")
                            ],
                            top_companies=["Acme Holdings"],
                            events=[],
                        )
                    ],
                )
            ],
        )

        with patch("backend.app.api.routes.build_knowledge_graph", return_value=payload):
            response = client.get("/api/knowledge-graph")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["region_count"], 1)
        self.assertEqual(response.json()["regions"][0]["region_id"], "EMEA")

    def test_chat_commit_route_returns_commit_payload(self) -> None:
        app = FastAPI()
        app.include_router(router, prefix="/api")
        client = TestClient(app)
        committed_at = datetime.now(timezone.utc)

        with patch(
            "backend.app.api.routes.commit_chat_to_knowledge_base",
            return_value=ChatCommitResponse(
                note_id="note-1",
                cluster_id="cluster-1",
                title="Committed analyst note",
                summary_markdown="## What We Learned\n- Example",
                committed_at=committed_at,
                committed_by="app",
                knowledge_base=KnowledgeBaseStatus(
                    status="ready",
                    document_count=4,
                    vector_store_id="vs_123",
                    last_synced_at=committed_at,
                    last_error=None,
                ),
            ),
        ):
            response = client.post(
                "/api/chat/commit",
                json={
                    "selected_cluster_id": "cluster-1",
                    "selected_cluster_name": "Acme Holdings",
                    "messages": [
                        {"role": "user", "content": "What do we know?", "citations": []},
                        {"role": "assistant", "content": "Here are the key points.", "citations": []},
                    ],
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["note_id"], "note-1")
        self.assertEqual(response.json()["knowledge_base"]["status"], "ready")

    def test_selected_cluster_chat_uses_local_fallback_without_full_sync(self) -> None:
        with patch("backend.app.chat_service.list_kb_document_records", return_value=[]), patch(
            "backend.app.chat_service.count_kb_document_records",
            return_value=0,
        ), patch(
            "backend.app.chat_service.ensure_cluster_markdown",
            return_value=("cluster markdown", "/tmp/cluster-1.md"),
        ) as ensure_markdown, patch(
            "backend.app.chat_service.get_knowledge_base_status",
            return_value={"status": "fallback", "vector_store_id": None},
        ), patch(
            "backend.app.chat_service.get_azure_capabilities"
        ) as get_caps, patch(
            "backend.app.chat_service._stream_once",
            return_value=iter(["event: response\ndata: {}\n\n"]),
        ) as stream_once:
            get_caps.return_value.file_search_supported = False
            output = list(
                stream_chat_events(
                    ChatRequest(
                        message="Summarize this cluster",
                        selected_cluster_id="cluster-1",
                        scope="selected_cluster",
                    )
                )
            )

        ensure_markdown.assert_called_once_with("cluster-1")
        stream_once.assert_called_once()
        self.assertEqual(output, ["event: response\ndata: {}\n\n"])

    def test_selected_cluster_chat_adds_structured_focus_context_and_citations(self) -> None:
        with patch("backend.app.chat_service.list_kb_document_records", return_value=[]), patch(
            "backend.app.chat_service.count_kb_document_records",
            return_value=0,
        ), patch(
            "backend.app.chat_service.ensure_cluster_markdown",
            return_value=("cluster markdown", "/tmp/cluster-1.md"),
        ), patch(
            "backend.app.chat_service.get_knowledge_base_status",
            return_value={"status": "fallback", "vector_store_id": None},
        ), patch(
            "backend.app.chat_service.get_azure_capabilities"
        ) as get_caps, patch(
            "backend.app.chat_service.get_opportunity_detail",
            return_value=make_detail(),
        ), patch(
            "backend.app.chat_service._stream_once",
            return_value=iter(["event: response\ndata: {}\n\n"]),
        ) as stream_once:
            get_caps.return_value.file_search_supported = False
            list(
                stream_chat_events(
                    ChatRequest(
                        message="Assess this source",
                        selected_cluster_id="cluster-1",
                        scope="selected_cluster",
                        active_tab="sources",
                        entity_id="entity-1",
                        source_id="source-1",
                        graph_node_id="entity:entity-1",
                    )
                )
            )

        payload = stream_once.call_args.args[0]
        self.assertIn("Focused source context:", payload["input"][0]["content"][0]["text"])
        self.assertIn("Title: Headline", payload["input"][0]["content"][0]["text"])
        focused_citations = stream_once.call_args.kwargs["focused_citations"]
        source_citation = next((citation for citation in focused_citations if citation.source_id == "source-1"), None)
        self.assertIsNotNone(source_citation)
        self.assertEqual(source_citation.url, "https://example.org/source-1")

    def test_global_graph_chat_adds_geography_focus_context_and_filters(self) -> None:
        graph = KnowledgeGraphResponse(
            generated_at=datetime.now(timezone.utc),
            region_count=1,
            country_count=1,
            event_count=1,
            regions=[
                KnowledgeGraphRegionSummary(
                    region_id="EMEA",
                    label="EMEA",
                    narrative="Regional outlook",
                    event_count=1,
                    country_count=1,
                    company_count=1,
                    average_opportunity=88.0,
                    average_confidence=74.0,
                    dominant_triggers=[
                        KnowledgeGraphDistributionItem(label="Insolvency", count=1, trigger_type="insolvency", tone="red")
                    ],
                    countries=[
                        KnowledgeGraphCountrySummary(
                            country_id="United Kingdom",
                            label="United Kingdom",
                            region_id="EMEA",
                            narrative="Country outlook",
                            event_count=1,
                            company_count=1,
                            average_opportunity=88.0,
                            average_confidence=74.0,
                            dominant_triggers=[
                                KnowledgeGraphDistributionItem(label="Insolvency", count=1, trigger_type="insolvency", tone="red")
                            ],
                            top_companies=["Acme Holdings"],
                            events=[
                                KnowledgeGraphEventSummary(
                                    cluster_id="cluster-1",
                                    subject_company_name="Acme Holdings",
                                    subject_country="United Kingdom",
                                    subject_region="EMEA",
                                    trigger_type="insolvency",
                                    event_date="2026-04-01",
                                    event_summary="Event summary",
                                    opportunity_score=88.0,
                                    cluster_confidence_score=74.0,
                                )
                            ],
                        )
                    ],
                )
            ],
        )

        with patch("backend.app.chat_service.list_kb_document_records", return_value=[]), patch(
            "backend.app.chat_service.count_kb_document_records",
            return_value=2,
        ), patch(
            "backend.app.chat_service.get_knowledge_base_status",
            return_value={"status": "ready", "vector_store_id": "vs_1"},
        ), patch(
            "backend.app.chat_service.get_azure_capabilities"
        ) as get_caps, patch(
            "backend.app.chat_service.build_knowledge_graph",
            return_value=graph,
        ), patch(
            "backend.app.chat_service._stream_once",
            return_value=iter(["event: response\ndata: {}\n\n"]),
        ) as stream_once:
            get_caps.return_value.file_search_supported = True
            list(
                stream_chat_events(
                    ChatRequest(
                        message="How does the UK compare with the region?",
                        scope="all",
                        active_tab="global_graph",
                        region_id="EMEA",
                        country_id="United Kingdom",
                    )
                )
            )

        payload = stream_once.call_args.args[0]
        self.assertEqual(payload["tools"][0]["filters"]["type"], "or")
        self.assertIn("Focused country context:", payload["input"][0]["content"][0]["text"])
        focused_citations = stream_once.call_args.kwargs["focused_citations"]
        self.assertTrue(any(citation.region_id == "EMEA" for citation in focused_citations))
        self.assertTrue(any(citation.country_id == "United Kingdom" for citation in focused_citations))


if __name__ == "__main__":
    unittest.main()
