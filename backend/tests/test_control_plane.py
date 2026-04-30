from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from backend.app.control_plane import _sync_run_record, start_manual_generation_run, trigger_knowledge_base_sync


class ControlPlaneTests(unittest.TestCase):
    def test_trigger_knowledge_base_sync_preserves_existing_status_while_already_running(self) -> None:
        class PendingFuture:
            def done(self) -> bool:
                return False

        with patch("backend.app.control_plane._KB_SYNC_FUTURE", new=PendingFuture()), patch(
            "backend.app.control_plane.get_knowledge_base_status",
            return_value={
                "status": "ready",
                "last_synced_at": datetime.now(timezone.utc),
                "document_count": 12,
                "cluster_document_count": 3,
                "entity_document_count": 5,
                "source_document_count": 2,
                "region_document_count": 1,
                "country_document_count": 1,
                "duplicate_candidate_count": 0,
                "stale_local_file_count": 0,
                "vector_store_id": "vs_123",
                "last_error": None,
            },
        ), patch("backend.app.control_plane.save_pipeline_settings_record") as save_settings:
            status = trigger_knowledge_base_sync(cluster_id="cluster-1", full_refresh=False)

        save_settings.assert_not_called()
        self.assertEqual(status["status"], "syncing")
        self.assertEqual(status["document_count"], 12)
        self.assertEqual(status["vector_store_id"], "vs_123")

    def test_sync_run_record_does_not_persist_failure_for_transient_sync_error(self) -> None:
        record = {
            "app_run_id": "run-1",
            "status": "running",
            "requested_at": None,
            "started_at": None,
            "error_message": None,
        }

        with patch("backend.app.control_plane.get_generation_runner") as get_runner, patch(
            "backend.app.control_plane.update_generation_run_record"
        ) as update_record:
            get_runner.return_value.sync.side_effect = RuntimeError("temporary auth failure")

            synced = _sync_run_record(record)

        update_record.assert_not_called()
        self.assertEqual(synced["status"], "running")
        self.assertEqual(synced["error_message"], "Unable to sync generation run status: temporary auth failure")

    def test_sync_run_record_handles_naive_datetime_anchor(self) -> None:
        record = {
            "app_run_id": "run-naive",
            "status": "running",
            "requested_at": (datetime.now(timezone.utc) - timedelta(minutes=5)).replace(tzinfo=None),
            "started_at": None,
            "error_message": None,
        }

        with patch("backend.app.control_plane.get_generation_runner") as get_runner:
            get_runner.return_value.sync.return_value = record

            synced = _sync_run_record(record)

        self.assertEqual(synced["status"], "running")

    def test_start_manual_generation_run_uses_region_override(self) -> None:
        base_settings = {
            "target_region": "EMEA",
            "generation_runner": "local",
        }
        saved_payloads: list[dict] = []

        with patch("backend.app.control_plane.get_pipeline_settings_record", return_value=base_settings), patch(
            "backend.app.control_plane.save_pipeline_settings_record"
        ) as save_settings, patch("backend.app.control_plane.make_run_id", return_value="run-1"), patch(
            "backend.app.control_plane.save_generation_run_record",
            side_effect=lambda payload: saved_payloads.append(payload) or payload,
        ), patch("backend.app.control_plane.get_generation_runner") as get_runner:
            get_runner.return_value.runner_type = "local"
            get_runner.return_value.submit.return_value = {"app_run_id": "run-1", "status": "queued"}

            start_manual_generation_run(requested_by="app", research_mode="region", target_region="APAC")

        save_settings.assert_called_once_with({"target_region": "APAC"}, updated_by="research-request")
        submitted_settings = get_runner.return_value.submit.call_args.args[1]
        self.assertEqual(submitted_settings["research_mode"], "region")
        self.assertEqual(submitted_settings["target_region"], "APAC")
        self.assertIsNone(submitted_settings["company_name"])
        self.assertEqual(saved_payloads[0]["research_target"], "APAC")
        self.assertEqual(saved_payloads[0]["trigger_source"], "manual_region")

    def test_start_manual_generation_run_uses_company_override(self) -> None:
        base_settings = {
            "target_region": "North America",
            "generation_runner": "local",
        }
        saved_payloads: list[dict] = []

        with patch("backend.app.control_plane.get_pipeline_settings_record", return_value=base_settings), patch(
            "backend.app.control_plane.save_pipeline_settings_record"
        ) as save_settings, patch("backend.app.control_plane.make_run_id", return_value="run-2"), patch(
            "backend.app.control_plane.save_generation_run_record",
            side_effect=lambda payload: saved_payloads.append(payload) or payload,
        ), patch("backend.app.control_plane.get_generation_runner") as get_runner:
            get_runner.return_value.runner_type = "local"
            get_runner.return_value.submit.return_value = {"app_run_id": "run-2", "status": "queued"}

            start_manual_generation_run(requested_by="app", research_mode="company", company_name="Acme plc")

        save_settings.assert_called_once_with({"target_region": "North America"}, updated_by="research-request")
        submitted_settings = get_runner.return_value.submit.call_args.args[1]
        self.assertEqual(submitted_settings["research_mode"], "company")
        self.assertEqual(submitted_settings["company_name"], "Acme plc")
        self.assertEqual(submitted_settings["target_region"], "North America")
        self.assertEqual(saved_payloads[0]["research_target"], "Acme plc")
        self.assertEqual(saved_payloads[0]["trigger_source"], "manual_company")


if __name__ == "__main__":
    unittest.main()
