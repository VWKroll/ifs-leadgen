from __future__ import annotations

import argparse

from ..generation_runtime import execute_generation_run
from ..store import get_generation_run_record, get_pipeline_settings_record, save_generation_run_record


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one IDC Event Intelligence generation pipeline execution.")
    parser.add_argument("--app-run-id", required=True)
    parser.add_argument("--trigger-source", default="schedule")
    parser.add_argument("--requested-by", default="databricks-job")
    args = parser.parse_args()

    settings_record = get_pipeline_settings_record()
    existing_run = get_generation_run_record(args.app_run_id) or {}
    effective_settings = {
        **settings_record,
        "research_mode": existing_run.get("research_mode") or "region",
        "target_region": existing_run.get("target_region") or settings_record.get("target_region"),
        "company_name": existing_run.get("company_name"),
    }
    save_generation_run_record(
        {
            "app_run_id": args.app_run_id,
            "trigger_source": args.trigger_source,
            "requested_by": args.requested_by,
            "research_mode": existing_run.get("research_mode"),
            "research_target": existing_run.get("research_target"),
            "target_region": existing_run.get("target_region"),
            "company_name": existing_run.get("company_name"),
            "runner_type": "job",
            "status": "queued",
            "step_statuses": {
                "discovery": "pending",
                "expansion": "pending",
                "scoring": "pending",
                "role_recommendation": "pending",
                "persistence": "pending",
            },
        }
    )
    execute_generation_run(args.app_run_id, effective_settings)


if __name__ == "__main__":
    main()
