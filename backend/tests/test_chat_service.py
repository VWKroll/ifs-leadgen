from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch

from backend.app.chat_tools import SHERLOCK_TOOLS, execute_tool


class SherlockToolsSchemaTests(unittest.TestCase):
    """Validate the SHERLOCK_TOOLS list structure."""

    def test_tools_list_not_empty(self) -> None:
        self.assertGreater(len(SHERLOCK_TOOLS), 0)

    def test_each_tool_has_required_fields(self) -> None:
        for tool in SHERLOCK_TOOLS:
            with self.subTest(tool=tool.get("name")):
                self.assertEqual(tool["type"], "function")
                self.assertIn("name", tool)
                self.assertIn("description", tool)
                self.assertIn("parameters", tool)
                self.assertEqual(tool["parameters"]["type"], "object")

    def test_tool_names(self) -> None:
        names = {t["name"] for t in SHERLOCK_TOOLS}
        self.assertIn("search_leads", names)
        self.assertIn("get_lead_detail", names)
        self.assertIn("save_deduction", names)


class ExecuteToolSearchLeadsTests(unittest.TestCase):
    """Test _execute_tool for the search_leads branch."""

    @patch("backend.app.chat_tools.search_opportunities")
    def test_search_leads_returns_results(self, mock_search: MagicMock) -> None:
        lead = MagicMock()
        lead.cluster_id = "c1"
        lead.subject_company_name = "Acme"
        lead.event_headline = "Distress event"
        lead.opportunity_score = 90.0
        lead.subject_region = "Europe"
        lead.subject_country = "UK"
        lead.trigger_type = "insolvency"
        mock_search.return_value = [lead]

        result = json.loads(execute_tool("search_leads", {"query": "acme"}, "user@test.com"))
        self.assertEqual(result["total_matches"], 1)
        self.assertEqual(result["results"][0]["cluster_id"], "c1")
        mock_search.assert_called_once_with("acme", limit=10)

    @patch("backend.app.chat_tools.search_opportunities", return_value=[])
    def test_search_leads_no_results(self, mock_search: MagicMock) -> None:
        result = json.loads(execute_tool("search_leads", {"query": "zzz"}, None))
        self.assertEqual(result["results"], [])
        self.assertIn("No leads found", result["message"])


class ExecuteToolGetLeadDetailTests(unittest.TestCase):

    @patch("backend.app.chat_tools.get_opportunity_detail")
    def test_get_lead_detail(self, mock_detail: MagicMock) -> None:
        cluster_mock = MagicMock()
        cluster_mock.model_dump.return_value = {"cluster_id": "c1", "score": 88}
        mock_detail.return_value = MagicMock(
            cluster=cluster_mock,
            entities=[{"entity_name": "Acme", "entity_type": "company", "commercial_role": "subject"}],
            sources=[],
            recommendations=[],
        )
        result = json.loads(execute_tool("get_lead_detail", {"cluster_id": "c1"}, None))
        self.assertEqual(result["entity_count"], 1)
        self.assertEqual(result["source_count"], 0)


class ExecuteToolSaveDeductionTests(unittest.TestCase):

    def test_save_deduction_with_user(self) -> None:
        with patch("backend.app.store.get_user_memory", return_value={"profile": {"key_deductions": []}}) as mock_get, \
             patch("backend.app.store.upsert_user_memory") as mock_upsert:
            result = json.loads(execute_tool("save_deduction", {"deduction": "Important insight"}, "user@test.com"))
            self.assertEqual(result["status"], "saved")
            mock_upsert.assert_called_once()

    def test_save_deduction_no_user(self) -> None:
        result = json.loads(execute_tool("save_deduction", {"deduction": "test"}, None))
        self.assertEqual(result["status"], "no_user_id")

    def test_unknown_tool(self) -> None:
        result = json.loads(execute_tool("nonexistent_tool", {}, None))
        self.assertIn("error", result)


class BuildUserContextTests(unittest.TestCase):
    """Test _build_user_context from chat_service."""

    @patch("backend.app.chat_service.get_user_memory")
    def test_returns_none_for_no_user(self, mock_mem: MagicMock) -> None:
        from backend.app.chat_service import _build_user_context
        self.assertIsNone(_build_user_context(None))
        mock_mem.assert_not_called()

    @patch("backend.app.chat_service.get_user_memory", return_value={})
    def test_returns_none_for_empty_memory(self, mock_mem: MagicMock) -> None:
        from backend.app.chat_service import _build_user_context
        self.assertIsNone(_build_user_context("user@test.com"))

    @patch("backend.app.chat_service.get_user_memory", return_value={
        "profile": {"name": "Alice", "role": "VP", "sector": "TMT", "region": "EMEA"}
    })
    def test_returns_context_string(self, mock_mem: MagicMock) -> None:
        from backend.app.chat_service import _build_user_context
        ctx = _build_user_context("user@test.com")
        self.assertIsNotNone(ctx)
        self.assertIn("Alice", ctx)
        self.assertIn("TMT", ctx)
        self.assertIn("EMEA", ctx)


class ChatServiceHelperTests(unittest.TestCase):
    """Test small helper functions in chat_service."""

    def test_sse_formatting(self) -> None:
        from backend.app.chat_service import _sse
        result = _sse("delta", {"text": "hello"})
        self.assertTrue(result.startswith("event: delta\n"))
        self.assertIn('"text": "hello"', result)
        self.assertTrue(result.endswith("\n\n"))

    def test_format_chat_transcript(self) -> None:
        from backend.app.chat_service import _format_chat_transcript, ChatMessage
        msgs = [
            ChatMessage(role="user", content="Hello", citations=[]),
            ChatMessage(role="assistant", content="The data reveals...", citations=[]),
        ]
        transcript = _format_chat_transcript(msgs)
        self.assertIn("User: Hello", transcript)
        self.assertIn("Assistant: The data reveals", transcript)

    def test_parse_string_list_from_json(self) -> None:
        from backend.app.chat_service import _parse_string_list
        self.assertEqual(_parse_string_list('["a","b"]'), ["a", "b"])
        self.assertEqual(_parse_string_list(None), [])
        self.assertEqual(_parse_string_list(["x", "y"]), ["x", "y"])
        self.assertEqual(_parse_string_list("plain"), ["plain"])


if __name__ == "__main__":
    unittest.main()
