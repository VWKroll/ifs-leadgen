from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from backend.app.sales_workspace import (
    _confidence_label,
    _first_text,
    _json_list,
    _json_payload,
    _priority_label,
)


class PriorityLabelTests(unittest.TestCase):
    def test_high(self) -> None:
        self.assertEqual(_priority_label(95.0), "High")

    def test_medium(self) -> None:
        self.assertEqual(_priority_label(65.0), "Medium")

    def test_low(self) -> None:
        self.assertEqual(_priority_label(30.0), "Watch")

    def test_none(self) -> None:
        self.assertEqual(_priority_label(None), "Watch")


class ConfidenceLabelTests(unittest.TestCase):
    def test_high(self) -> None:
        self.assertEqual(_confidence_label(85.0), "High confidence")

    def test_medium(self) -> None:
        self.assertEqual(_confidence_label(70.0), "Medium confidence")

    def test_low(self) -> None:
        self.assertEqual(_confidence_label(40.0), "Needs review")


class FirstTextTests(unittest.TestCase):
    def test_returns_value_when_present(self) -> None:
        self.assertEqual(_first_text("hello", "fallback"), "hello")

    def test_returns_fallback_when_empty(self) -> None:
        self.assertEqual(_first_text("", "fallback"), "fallback")
        self.assertEqual(_first_text(None, "fallback"), "fallback")


class JsonPayloadTests(unittest.TestCase):
    def test_dict_passthrough(self) -> None:
        self.assertEqual(_json_payload({"a": 1}), {"a": 1})

    def test_json_string(self) -> None:
        self.assertEqual(_json_payload('{"b": 2}'), {"b": 2})

    def test_empty(self) -> None:
        self.assertEqual(_json_payload(None), {})
        self.assertEqual(_json_payload(""), {})

    def test_invalid_json(self) -> None:
        self.assertEqual(_json_payload("not-json"), {})


class JsonListTests(unittest.TestCase):
    def test_json_array_string(self) -> None:
        self.assertEqual(_json_list('["a","b"]'), ["a", "b"])

    def test_empty(self) -> None:
        self.assertEqual(_json_list(None), [])
        self.assertEqual(_json_list(""), [])

    def test_filters_empty_strings(self) -> None:
        self.assertEqual(_json_list('["x", ""]'), ["x"])


class ClaimOpportunityTests(unittest.TestCase):
    """Test claim_opportunity with mocked store and detail."""

    @patch("backend.app.sales_workspace.threading")
    @patch("backend.app.sales_workspace.get_sales_workspace")
    @patch("backend.app.sales_workspace.save_sales_draft_record")
    @patch("backend.app.sales_workspace.claim_sales_claim_record")
    @patch("backend.app.sales_workspace.get_sales_claim_record", return_value=None)
    @patch("backend.app.sales_workspace.get_opportunity_detail")
    def test_claim_creates_record(
        self,
        mock_detail: MagicMock,
        mock_get_claim: MagicMock,
        mock_claim: MagicMock,
        mock_save_draft: MagicMock,
        mock_get_workspace: MagicMock,
        mock_threading: MagicMock,
    ) -> None:
        from backend.app.schemas import (
            ClaimOpportunityRequest,
            OpportunityDetail,
            OpportunitySummary,
        )

        cluster = OpportunitySummary(
            cluster_id="c1",
            subject_company_name="Acme",
            subject_country="UK",
            subject_region="Europe",
            trigger_type="insolvency",
            event_date="2026-04-01",
            event_summary="Distress signal",
            opportunity_score=90.0,
        )
        mock_detail.return_value = OpportunityDetail(
            cluster=cluster,
            graph_nodes=[],
            graph_edges=[],
            entities=[{
                "cluster_entity_id": "e1",
                "entity_name": "Acme",
                "entity_type": "company",
                "branch_type": "direct",
                "commercial_role": "subject",
            }],
            sources=[],
            recommendations=[],
        )
        mock_claim.return_value = {
            "claim_id": "claim-1",
            "cluster_id": "c1",
            "sales_item_id": "e1",
            "status": "claimed",
            "draft_id": "draft-1",
            "claimed_by_user_id": "user@test.com",
            "claimed_by_name": "Test User",
        }
        mock_save_draft.return_value = {}
        workspace_mock = MagicMock()
        workspace_mock.cluster_id = "c1"
        workspace_mock.sales_item_id = "e1"
        # First call = not yet claimed; second call = after claiming
        mock_get_workspace.side_effect = [None, workspace_mock]

        from backend.app.sales_workspace import claim_opportunity

        request = ClaimOpportunityRequest(
            sales_item_id="e1",
            claimed_by_user_id="user@test.com",
            claimed_by_name="Test User",
        )
        result = claim_opportunity("c1", request)
        self.assertEqual(result.cluster_id, "c1")
        mock_claim.assert_called_once()


if __name__ == "__main__":
    unittest.main()
