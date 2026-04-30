from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.main import app, _run_startup_reconciliation


class MainTests(unittest.TestCase):
    def test_startup_keeps_api_alive_when_control_plane_reconciliation_fails(self) -> None:
        with patch("backend.app.main.reconcile_orphaned_local_runs", side_effect=RuntimeError("scope mismatch")):
            _run_startup_reconciliation(app)

        self.assertEqual(app.state.control_plane_startup_error, "scope mismatch")

        client = TestClient(app)
        response = client.get("/api/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "degraded")
        self.assertEqual(response.json()["detail"], "scope mismatch")


if __name__ == "__main__":
    unittest.main()
