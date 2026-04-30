from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from backend.app.store._core import (
    _clean_value,
    _normalize_timestamp,
    _quote_identifier,
    _quote_sql,
    _quote_table,
    utcnow,
)


class UtcnowTests(unittest.TestCase):
    def test_returns_aware_datetime(self) -> None:
        now = utcnow()
        self.assertIsInstance(now, datetime)
        self.assertEqual(now.tzinfo, timezone.utc)


class CleanValueTests(unittest.TestCase):
    def test_none(self) -> None:
        self.assertIsNone(_clean_value(None))

    def test_nan_float(self) -> None:
        self.assertIsNone(_clean_value(float("nan")))

    def test_regular_value(self) -> None:
        self.assertEqual(_clean_value("hello"), "hello")
        self.assertEqual(_clean_value(42), 42)


class NormalizeTimestampTests(unittest.TestCase):
    def test_none(self) -> None:
        self.assertIsNone(_normalize_timestamp(None))

    def test_naive_datetime_gets_utc(self) -> None:
        naive = datetime(2026, 1, 1, 12, 0, 0)
        result = _normalize_timestamp(naive)
        self.assertEqual(result.tzinfo, timezone.utc)

    def test_aware_datetime_passthrough(self) -> None:
        aware = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = _normalize_timestamp(aware)
        self.assertEqual(result, aware)


class QuoteSqlTests(unittest.TestCase):
    def test_escapes_single_quotes(self) -> None:
        self.assertEqual(_quote_sql("it's"), "it''s")


class QuoteIdentifierTests(unittest.TestCase):
    def test_wraps_in_backticks(self) -> None:
        self.assertEqual(_quote_identifier("col"), "`col`")

    def test_escapes_backtick(self) -> None:
        self.assertEqual(_quote_identifier("co`l"), "`co``l`")


class QuoteTableTests(unittest.TestCase):
    def test_quotes_each_part(self) -> None:
        result = _quote_table("catalog.schema.table")
        self.assertEqual(result, "`catalog`.`schema`.`table`")


class UserMemoryTests(unittest.TestCase):
    """Test user memory get/upsert with mocked Spark."""

    @patch("backend.app.store.memory.get_spark")
    @patch("backend.app.store.memory.ensure_control_plane_tables")
    def test_get_user_memory_empty(self, mock_ensure: MagicMock, mock_spark: MagicMock) -> None:
        import pandas as pd
        from backend.app.store.memory import get_user_memory

        mock_table = MagicMock()
        mock_table.filter.return_value.toPandas.return_value = pd.DataFrame(
            columns=["user_id", "memory_key", "memory_value"]
        )
        mock_spark.return_value.table.return_value = mock_table

        result = get_user_memory("user@test.com")
        self.assertEqual(result, {})

    @patch("backend.app.store.memory.get_spark")
    @patch("backend.app.store.memory.ensure_control_plane_tables")
    def test_get_user_memory_parses_json(self, mock_ensure: MagicMock, mock_spark: MagicMock) -> None:
        import pandas as pd
        from backend.app.store.memory import get_user_memory

        mock_table = MagicMock()
        mock_table.filter.return_value.toPandas.return_value = pd.DataFrame([
            {"user_id": "u1", "memory_key": "profile", "memory_value": json.dumps({"name": "Alice"})},
            {"user_id": "u1", "memory_key": "prefs", "memory_value": "raw-string"},
        ])
        mock_spark.return_value.table.return_value = mock_table

        result = get_user_memory("u1")
        self.assertEqual(result["profile"], {"name": "Alice"})
        self.assertEqual(result["prefs"], "raw-string")

    @patch("backend.app.store.memory._upsert_rows")
    @patch("backend.app.store.memory.ensure_control_plane_tables")
    def test_upsert_user_memory(self, mock_ensure: MagicMock, mock_upsert: MagicMock) -> None:
        from backend.app.store.memory import upsert_user_memory

        result = upsert_user_memory("u1", "profile", {"name": "Bob"})
        self.assertEqual(result["user_id"], "u1")
        self.assertEqual(result["memory_key"], "profile")
        mock_upsert.assert_called_once()


if __name__ == "__main__":
    unittest.main()
