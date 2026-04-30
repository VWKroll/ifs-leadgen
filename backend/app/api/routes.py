from __future__ import annotations

import json
import logging
import time
from collections import deque
from threading import Lock

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from ..settings import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Admin API key guard
# ---------------------------------------------------------------------------

def require_admin_api_key(request: Request) -> None:
    """Dependency that enforces an API key on admin routes when configured."""
    if not settings.admin_api_key:
        return  # No key configured — allow (dev mode)
    provided = request.headers.get("X-API-Key") or ""
    if provided != settings.admin_api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing admin API key.")


def _get_authenticated_user_id(request: Request) -> str | None:
    """Extract the authenticated user identity from Databricks proxy headers.

    Returns the email or preferred-username, or ``None`` in local dev
    (where these headers are absent).
    """
    return (
        request.headers.get("X-Forwarded-Email")
        or request.headers.get("X-Forwarded-Preferred-Username")
        or None
    )


def require_user_match(request: Request, user_id: str) -> None:
    """Ensure the path *user_id* matches the authenticated user.

    In local dev (no proxy headers) this is a no-op so developers can
    test freely.
    """
    caller = _get_authenticated_user_id(request)
    if caller is None:
        return  # dev mode — no proxy headers present
    if caller != user_id:
        raise HTTPException(status_code=403, detail="Forbidden: cannot access another user's data.")


# ---------------------------------------------------------------------------
# Simple in-memory rate limiter for generation runs
# ---------------------------------------------------------------------------

_GENERATION_RUN_TIMESTAMPS: deque[float] = deque()
_GENERATION_RUN_LOCK = Lock()


def enforce_generation_rate_limit() -> None:
    now = time.monotonic()
    window = 60.0
    with _GENERATION_RUN_LOCK:
        while _GENERATION_RUN_TIMESTAMPS and now - _GENERATION_RUN_TIMESTAMPS[0] > window:
            _GENERATION_RUN_TIMESTAMPS.popleft()
        if len(_GENERATION_RUN_TIMESTAMPS) >= settings.generation_rate_limit_per_minute:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded — max {settings.generation_rate_limit_per_minute} generation runs per minute.",
            )
        _GENERATION_RUN_TIMESTAMPS.append(now)

from ..chat_service import commit_chat_to_knowledge_base, stream_chat_events
from ..control_plane import (
    cancel_generation_run,
    get_generation_run,
    get_pipeline_settings_response,
    list_generation_runs,
    run_knowledge_base_cleanup,
    start_manual_generation_run,
    trigger_knowledge_base_sync,
    update_pipeline_settings,
)
from ..sales_workspace import (
    claim_opportunity,
    get_sales_dashboard,
    get_sales_leads,
    get_sales_workspace,
    push_sales_draft,
    send_sales_draft_message,
    update_sales_claim_status,
    update_sales_draft,
)
from ..store import get_user_memory, upsert_user_memory
from ..schemas import (
    ChatCommitRequest,
    ChatCommitResponse,
    ClaimOpportunityRequest,
    CreateGenerationRunRequest,
    ChatRequest,
    GenerationRunResponse,
    GenerationRunsResponse,
    KnowledgeBaseCleanupRequest,
    KnowledgeBaseStatus,
    KnowledgeBaseSyncRequest,
    KnowledgeGraphResponse,
    MapResponse,
    OpportunitiesResponse,
    OpportunityDetail,
    PipelineSettingsPatchRequest,
    PipelineSettingsResponse,
    SalesDashboardResponse,
    SalesLeadsResponse,
    SalesWorkspaceActorRequest,
    SalesDraftConversationRequest,
    SalesDraftPatchRequest,
    SalesWorkspaceResponse,
    SalesWorkspaceStatusPatchRequest,
    UserMemoryResponse,
    UserMemoryUpsertRequest,
    UserProfile,
)
from ..services import build_knowledge_graph, build_map_markers, get_opportunity_detail, list_opportunities

router = APIRouter()


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@router.get("/healthz")
def healthz(request: Request) -> dict:
    """Structured health probe with Spark connectivity check."""
    checks: dict[str, str] = {}

    # Spark / Delta Lake connectivity
    try:
        from idc_app.spark import get_spark
        spark = get_spark()
        spark.sql("SELECT 1").collect()
        checks["spark"] = "ok"
    except Exception as exc:
        checks["spark"] = f"error: {exc}"

    # Control-plane startup
    startup_err = getattr(request.app.state, "control_plane_startup_error", None)
    checks["control_plane"] = "ok" if not startup_err else f"degraded: {startup_err}"

    overall = "healthy" if all(v == "ok" for v in checks.values()) else "degraded"
    return {"status": overall, "checks": checks}


@router.get("/me")
def current_user(request: Request) -> dict[str, str | None]:
    """Return the identity of the logged-in Databricks App user.

    Databricks Apps proxies set ``X-Forwarded-Email`` and
    ``X-Forwarded-Preferred-Username`` headers for the authenticated user.
    """
    email = (
        request.headers.get("X-Forwarded-Email")
        or request.headers.get("X-Forwarded-Preferred-Username")
        or None
    )
    raw_user = request.headers.get("X-Forwarded-User") or ""
    # X-Forwarded-User sometimes returns an internal numeric Databricks user ID
    # (e.g. "1339117837173721@3247602525969716") instead of a display name.
    # Detect this: if the local part before @ is all digits, it is an internal ID.
    local_part = raw_user.split("@")[0] if "@" in raw_user else raw_user
    is_numeric_id = bool(local_part) and local_part.replace("-", "").replace("_", "").isdigit()
    if raw_user and not is_numeric_id:
        name: str | None = raw_user
    elif email:
        name = " ".join(
            w.capitalize()
            for w in email.split("@")[0].replace(".", " ").replace("-", " ").replace("_", " ").split()
        )
    else:
        name = None
    return {"user_id": email, "name": name, "email": email}


@router.get("/health")
def healthcheck(request: Request) -> dict[str, str | None]:
    startup_error = getattr(request.app.state, "control_plane_startup_error", None)
    return {
        "status": "degraded" if startup_error else "ok",
        "detail": startup_error,
    }


@router.get("/opportunities", response_model=OpportunitiesResponse)
def opportunities() -> OpportunitiesResponse:
    return OpportunitiesResponse(items=list_opportunities())


@router.get("/opportunities/map", response_model=MapResponse)
def opportunities_map() -> MapResponse:
    return MapResponse(items=build_map_markers())


@router.get("/knowledge-graph", response_model=KnowledgeGraphResponse)
def knowledge_graph() -> KnowledgeGraphResponse:
    return build_knowledge_graph()


@router.get("/opportunities/{cluster_id}", response_model=OpportunityDetail)
def opportunity_detail(cluster_id: str) -> OpportunityDetail:
    try:
        return get_opportunity_detail(cluster_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Opportunity not found") from exc


@router.get("/admin/settings/pipeline", response_model=PipelineSettingsResponse, dependencies=[Depends(require_admin_api_key)])
def pipeline_settings() -> PipelineSettingsResponse:
    return PipelineSettingsResponse(**get_pipeline_settings_response())


@router.post("/admin/knowledge-base/sync", response_model=KnowledgeBaseStatus, dependencies=[Depends(require_admin_api_key)])
def sync_knowledge_base_route(request: KnowledgeBaseSyncRequest) -> KnowledgeBaseStatus:
    return KnowledgeBaseStatus(**trigger_knowledge_base_sync(cluster_id=request.cluster_id, full_refresh=request.full_refresh))


@router.post("/admin/knowledge-base/cleanup", response_model=KnowledgeBaseStatus, dependencies=[Depends(require_admin_api_key)])
def cleanup_knowledge_base_route(request: KnowledgeBaseCleanupRequest) -> KnowledgeBaseStatus:
    return KnowledgeBaseStatus(**run_knowledge_base_cleanup(mode=request.mode))


@router.patch("/admin/settings/pipeline", response_model=PipelineSettingsResponse, dependencies=[Depends(require_admin_api_key)])
def patch_pipeline_settings(request: PipelineSettingsPatchRequest) -> PipelineSettingsResponse:
    return PipelineSettingsResponse(**update_pipeline_settings(request.model_dump(exclude_none=True)))


@router.post("/admin/generation-runs", response_model=GenerationRunResponse, dependencies=[Depends(require_admin_api_key)])
def create_generation_run(request: CreateGenerationRunRequest) -> GenerationRunResponse:
    enforce_generation_rate_limit()
    try:
        return GenerationRunResponse(
            **start_manual_generation_run(
                requested_by=request.requested_by or "app",
                research_mode=request.research_mode,
                target_region=request.target_region,
                company_name=request.company_name,
            )
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/admin/generation-runs", response_model=GenerationRunsResponse, dependencies=[Depends(require_admin_api_key)])
def generation_runs() -> GenerationRunsResponse:
    return GenerationRunsResponse(items=[GenerationRunResponse(**item) for item in list_generation_runs()])


@router.get("/admin/generation-runs/{app_run_id}", response_model=GenerationRunResponse, dependencies=[Depends(require_admin_api_key)])
def generation_run(app_run_id: str) -> GenerationRunResponse:
    try:
        return GenerationRunResponse(**get_generation_run(app_run_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Generation run not found") from exc


@router.post("/admin/generation-runs/{app_run_id}/cancel", response_model=GenerationRunResponse, dependencies=[Depends(require_admin_api_key)])
def cancel_run(app_run_id: str) -> GenerationRunResponse:
    try:
        return GenerationRunResponse(**cancel_generation_run(app_run_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Generation run not found") from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/chat/responses/stream")
def chat_response_stream(request: ChatRequest, http_request: Request) -> StreamingResponse:
    # If the request carries a user_id, enforce it matches the authenticated
    # caller so one user cannot impersonate another in chat tool-calls.
    if request.user_id:
        caller = _get_authenticated_user_id(http_request)
        if caller is not None and caller != request.user_id:
            raise HTTPException(status_code=403, detail="Forbidden: user_id does not match authenticated user.")

    def event_stream():
        try:
            yield from stream_chat_events(request)
        except KeyError as exc:
            payload = {"message": f"Opportunity not found: {exc.args[0]}"}
            yield f"event: error\ndata: {json.dumps(payload)}\n\n"
        except Exception:
            logger.exception("Unhandled error in chat stream")
            payload = {"message": "An internal error occurred while processing the chat request."}
            yield f"event: error\ndata: {json.dumps(payload)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/chat/commit", response_model=ChatCommitResponse)
def chat_commit(request: ChatCommitRequest) -> ChatCommitResponse:
    return commit_chat_to_knowledge_base(request)


# ---------------------------------------------------------------------------
# User memory (Sherlock AI)
# ---------------------------------------------------------------------------

@router.get("/user/{user_id}/memory", response_model=UserMemoryResponse)
def get_memory(user_id: str, http_request: Request) -> UserMemoryResponse:
    require_user_match(http_request, user_id)
    entries = get_user_memory(user_id)
    profile_raw = entries.pop("profile", {})
    profile = UserProfile.model_validate(profile_raw) if isinstance(profile_raw, dict) else UserProfile()
    return UserMemoryResponse(user_id=user_id, profile=profile, entries=entries)


@router.put("/user/{user_id}/memory")
def put_memory(user_id: str, request: UserMemoryUpsertRequest, http_request: Request) -> dict[str, str]:
    require_user_match(http_request, user_id)
    upsert_user_memory(user_id, request.memory_key, request.memory_value)
    return {"status": "ok"}


@router.get("/sales/opportunities/{cluster_id}/items/{sales_item_id}", response_model=SalesWorkspaceResponse)
def sales_workspace(cluster_id: str, sales_item_id: str) -> SalesWorkspaceResponse:
    workspace = get_sales_workspace(cluster_id, sales_item_id)
    if workspace is None:
        raise HTTPException(status_code=404, detail="Sales workspace not found")
    return workspace


@router.post("/sales/opportunities/{cluster_id}/claim", response_model=SalesWorkspaceResponse, dependencies=[Depends(require_admin_api_key)])
def claim_sales_workspace(cluster_id: str, request: ClaimOpportunityRequest) -> SalesWorkspaceResponse:
    try:
        return claim_opportunity(cluster_id, request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Opportunity not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.patch("/sales/opportunities/{cluster_id}/items/{sales_item_id}/draft", response_model=SalesWorkspaceResponse, dependencies=[Depends(require_admin_api_key)])
def patch_sales_draft(cluster_id: str, sales_item_id: str, request: SalesDraftPatchRequest) -> SalesWorkspaceResponse:
    try:
        return update_sales_draft(cluster_id, sales_item_id, request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Sales workspace not found") from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@router.post("/sales/opportunities/{cluster_id}/items/{sales_item_id}/draft/chat", response_model=SalesWorkspaceResponse, dependencies=[Depends(require_admin_api_key)])
def chat_sales_draft(cluster_id: str, sales_item_id: str, request: SalesDraftConversationRequest) -> SalesWorkspaceResponse:
    try:
        return send_sales_draft_message(cluster_id, sales_item_id, request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Sales workspace not found") from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@router.post("/sales/opportunities/{cluster_id}/items/{sales_item_id}/push", response_model=SalesWorkspaceResponse, dependencies=[Depends(require_admin_api_key)])
def push_sales_workspace(cluster_id: str, sales_item_id: str, request: SalesWorkspaceActorRequest) -> SalesWorkspaceResponse:
    try:
        return push_sales_draft(cluster_id, sales_item_id, request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Sales workspace not found") from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@router.patch("/sales/opportunities/{cluster_id}/items/{sales_item_id}/status", response_model=SalesWorkspaceResponse, dependencies=[Depends(require_admin_api_key)])
def patch_sales_workspace_status(cluster_id: str, sales_item_id: str, request: SalesWorkspaceStatusPatchRequest) -> SalesWorkspaceResponse:
    try:
        return update_sales_claim_status(cluster_id, sales_item_id, request)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Sales workspace not found") from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@router.get("/sales/dashboard", response_model=SalesDashboardResponse)
def sales_dashboard() -> SalesDashboardResponse:
    return get_sales_dashboard()


@router.get("/sales/leads", response_model=SalesLeadsResponse)
def sales_leads(
    page: int = 1,
    page_size: int = 100,
    sort_by: str = "newest_event",
) -> SalesLeadsResponse:
    allowed_sorts = {"newest_event", "highest_priority", "best_confidence"}
    if sort_by not in allowed_sorts:
        raise HTTPException(status_code=400, detail="Unsupported sales lead sort.")
    return get_sales_leads(page=page, page_size=page_size, sort_by=sort_by)
