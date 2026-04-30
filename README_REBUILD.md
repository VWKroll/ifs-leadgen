# IDC Event Intelligence Rebuild

This rebuild keeps Databricks as the source of truth while moving the product into:

- `backend/`: FastAPI service
- `frontend/`: Next.js + React + TypeScript app
- repo root: Databricks Apps entrypoint that starts both with one `app.yaml` command
- `backend/app/pipeline/`: shared generation pipeline package used by the API and the Databricks job path

## Databricks Apps Shape

Databricks Apps builds from the repo root. That means:

- root `package.json` is the Node entrypoint Databricks will detect
- root `package.json` declares `frontend/` as an npm workspace so Databricks installs Next.js dependencies during root build
- root `app.yaml` starts both the Next.js frontend and FastAPI backend
- Next.js proxies `/api/*` to the internal FastAPI process, so the browser only talks to one app origin
- Databricks remains the system of record for all opportunity, graph, and map data

## Local development

```bash
cd /home/vishalyeddanapalli/idc-event-driven-opp
cp backend/.env.example backend/.env
.venv/bin/pip install -r backend/requirements.txt
npm install
npm run dev
```

Edit `backend/.env` before starting the app so the API has your local Databricks and Azure OpenAI settings.

This starts:

- FastAPI on `127.0.0.1:8001`
- Next.js on `127.0.0.1:3000`

The root scripts auto-detect `.venv/bin/python` locally and fall back to plain `python` in Databricks Apps.

For local secrets and backend config, prefer `backend/.env`.
The backend now reads `backend/.env` first and only falls back to root `.env` for legacy compatibility.
Local Databricks auth now prefers OAuth/CLI by default. Set `IDC_DB_AUTH_TYPE=pat` only if you explicitly want PAT-based Databricks Connect auth.

Optional:

- copy `frontend/.env.local.example` to `frontend/.env.local` and set `NEXT_PUBLIC_MAPBOX_TOKEN` for the live map
- keep Databricks auth configured on the machine for backend data access

For Databricks Apps deployment, add `NEXT_PUBLIC_MAPBOX_TOKEN` as an app environment variable so Next.js can render the live map in the deployed app.

See [SMOKE_TEST.md](/home/vishalyeddanapalli/idc-event-driven-opp/SMOKE_TEST.md) for a short manual verification checklist after startup.

To prepare corpus-wide chat locally, run:

```bash
PYTHONPATH=. ./.venv/bin/python scripts/sync-kb.py
```

For a faster first check, you can sync a single cluster and confirm the vector store path works:

```bash
PYTHONPATH=. ./.venv/bin/python scripts/sync-kb.py --cluster-id <cluster-id>
```

## Native pipeline control plane

The app now includes a native backend workflow layer for the event-generation pipeline:

- `POST /api/admin/generation-runs`
- `GET /api/admin/generation-runs`
- `GET /api/admin/generation-runs/{app_run_id}`
- `POST /api/admin/generation-runs/{app_run_id}/cancel`
- `GET /api/admin/settings/pipeline`
- `PATCH /api/admin/settings/pipeline`

The frontend exposes these through the new `Settings` section in the left rail.

Run metadata is stored in two backend-owned Delta tables:

- `pipeline_settings`
- `generation_runs`

The existing opportunity tables remain the canonical generated outputs:

- `event_clusters`
- `cluster_entities`
- `cluster_role_recommendations`
- `cluster_sources`

## Provider and runner configuration

The pipeline package supports two execution modes:

- `local`: the FastAPI process launches the pipeline in a background worker
- `job`: the FastAPI process triggers a Databricks Lakeflow Job and tracks the handoff in run history

Key backend environment variables:

- `IDC_GENERATION_RUNNER=local|job`
- `IDC_GENERATION_JOB_ID=<job id>` when using the Databricks job runner
- `IDC_DB_AUTH_TYPE=oauth|pat|auto`
- `IDC_DB_PROFILE=<databricks profile>` when using CLI/OAuth auth
- `IDC_AZURE_OPENAI_ENDPOINT=<azure/openai endpoint>`
- `IDC_AZURE_OPENAI_API_KEY=<azure/openai key>`
- `IDC_AZURE_OPENAI_API_VERSION=<api version>`
- `IDC_OPENAI_MODEL=<model deployment or model id>`
- `IDC_DB_HOST=<workspace host>` and `IDC_PAT_TOKEN=<token>` for local Databricks Connect access when using `IDC_DB_AUTH_TYPE=pat`

See [backend/.env.example](/home/vishalyeddanapalli/idc-event-driven-opp/backend/.env.example) for a local starting point.

## Databricks job entrypoint

The production Lakeflow Job should execute:

```bash
PYTHONPATH=. python -m backend.app.pipeline.job_entrypoint --app-run-id <run-id> --trigger-source schedule --requested-by databricks-job
```

The API will submit `app_run_id`, `trigger_source`, `requested_by`, and `mode=single` when the runner is set to `job`.

## Databricks Apps deployment notes

Per the Databricks Apps docs, production secrets and managed resources should be surfaced into `app.yaml` through `valueFrom` references rather than hardcoded into the repo.

Recommended app resources to add before enabling production scheduling:

- a secret resource for the Azure OpenAI key
- a Lakeflow Job resource for the generation pipeline

Once those resources exist, wire them into the app environment with names such as:

- `IDC_AZURE_OPENAI_API_KEY`
- `IDC_GENERATION_JOB_ID`

I did not hardcode those `valueFrom` references in `app.yaml` yet because the actual Databricks resource keys have to match what you create in the workspace, and adding guessed keys would break deployment before the resources exist.
