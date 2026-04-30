from __future__ import annotations

import unittest

from backend.app.pipeline.provider import parse_json_output


class ProviderParsingTests(unittest.TestCase):
    def test_parse_json_output_accepts_clean_json(self) -> None:
        payload = parse_json_output('{"status":"ok","count":2}')
        self.assertEqual(payload, {"status": "ok", "count": 2})

    def test_parse_json_output_ignores_trailing_text_after_valid_json(self) -> None:
        payload = parse_json_output('{"status":"ok","count":2}\n\nAdditional trailing text')
        self.assertEqual(payload, {"status": "ok", "count": 2})

    def test_parse_json_output_raises_useful_error_for_invalid_payload(self) -> None:
        with self.assertRaises(ValueError):
            parse_json_output("not-json-at-all")
