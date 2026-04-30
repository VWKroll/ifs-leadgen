from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from backend.app.generation_runtime import execute_generation_run
from backend.app.knowledge_base import KnowledgeBaseSyncResult
from backend.app.pipeline.models import GenerationPipelineResult


class GenerationRuntimeTests(unittest.TestCase):
    def test_execute_generation_run_updates_knowledge_base_step(self) -> None:
        captured_updates: list[dict] = []

        def capture_update(_run_id: str, patch: dict):
            captured_updates.append(patch)
            return patch

        def fake_sync_knowledge_base(*, on_lock_acquired=None, **_kwargs):
            if on_lock_acquired:
                on_lock_acquired()
            return KnowledgeBaseSyncResult(
                status="ready",
                last_synced_at=datetime.now(timezone.utc),
                document_count=1,
                vector_store_id="vs_1",
            )

        with patch(
            "backend.app.generation_runtime.run_generation_pipeline",
            return_value=GenerationPipelineResult(status="succeeded", created_cluster_id="cluster-1"),
        ), patch(
            "backend.app.generation_runtime.sync_knowledge_base",
            side_effect=fake_sync_knowledge_base,
        ), patch(
            "backend.app.generation_runtime.update_generation_run_record",
            side_effect=capture_update,
        ), patch(
            "backend.app.generation_runtime.invalidate_read_caches"
        ):
            execute_generation_run("run-1", settings_record={})

        final_update = captured_updates[-1]
        self.assertEqual(final_update["status"], "succeeded")
        self.assertEqual(final_update["created_cluster_id"], "cluster-1")
        self.assertEqual(final_update["step_statuses"]["knowledge_base_sync"], "succeeded")
        kb_states = [
            patch["step_statuses"]["knowledge_base_sync"]
            for patch in captured_updates
            if "step_statuses" in patch and "knowledge_base_sync" in patch["step_statuses"]
        ]
        self.assertIn("queued", kb_states)
        self.assertIn("running", kb_states)


if __name__ == "__main__":
    unittest.main()
