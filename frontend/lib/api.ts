import {
  ChatCommitRequest,
  ChatCommitResponse,
  ChatRequest,
  ChatResponse,
  ClaimOpportunityRequest,
  CreateGenerationRunRequest,
  GenerationRun,
  KnowledgeGraphResponse,
  KnowledgeBaseStatus,
  KnowledgeBaseSyncRequest,
  MapMarker,
  OpportunityDetail,
  OpportunitySummary,
  PipelineSettings,
  PipelineSettingsPatch,
  SalesDashboard,
  SalesLeadCatalog,
  SalesWorkspaceActorRequest,
  SalesDraftConversationRequest,
  SalesDraftPatchRequest,
  SalesWorkspace,
  SalesWorkspaceStatusPatchRequest,
} from "@/types/api";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "/api";

export class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

function isAbortError(error: unknown): error is Error {
  return error instanceof Error && error.name === "AbortError";
}

function describeNetworkError(path: string, error: unknown): Error {
  const target = `${API_BASE}${path}`;
  const suffix = error instanceof Error && error.message ? ` (${error.message})` : "";
  return new Error(
    `Unable to reach the API at ${target}. Make sure the FastAPI server is running and the frontend proxy is pointed at the correct port.${suffix}`,
  );
}

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${API_BASE}${path}`, {
      cache: "no-store",
      headers: {
        "Content-Type": "application/json",
        ...(init?.headers ?? {}),
      },
      ...init,
    });
  } catch (error) {
    if (isAbortError(error)) throw error;
    throw describeNetworkError(path, error);
  }
  if (!response.ok) {
    let detail = `Request failed for ${path}: ${response.status}`;
    try {
      const payload = await response.json();
      if (payload?.detail) detail = String(payload.detail);
    } catch {}
    throw new ApiError(response.status, detail);
  }
  return response.json();
}

export type CurrentUser = { user_id: string | null; name: string | null; email: string | null };

export async function getCurrentUser(): Promise<CurrentUser> {
  return fetchJson<CurrentUser>("/me");
}

export async function getOpportunities(): Promise<OpportunitySummary[]> {
  const data = await fetchJson<{ items: OpportunitySummary[] }>("/opportunities");
  return data.items;
}

export async function getOpportunityDetail(clusterId: string): Promise<OpportunityDetail> {
  return fetchJson<OpportunityDetail>(`/opportunities/${clusterId}`);
}

export async function getOpportunityDetailWithSignal(clusterId: string, signal: AbortSignal): Promise<OpportunityDetail> {
  return fetchJson<OpportunityDetail>(`/opportunities/${clusterId}`, { signal });
}

export async function getMapMarkers(): Promise<MapMarker[]> {
  const data = await fetchJson<{ items: MapMarker[] }>("/opportunities/map");
  return data.items;
}

export async function getKnowledgeGraph(): Promise<KnowledgeGraphResponse> {
  return fetchJson<KnowledgeGraphResponse>("/knowledge-graph");
}

export async function getPipelineSettings(): Promise<PipelineSettings> {
  return fetchJson<PipelineSettings>("/admin/settings/pipeline");
}

export async function patchPipelineSettings(patch: PipelineSettingsPatch): Promise<PipelineSettings> {
  return fetchJson<PipelineSettings>("/admin/settings/pipeline", {
    method: "PATCH",
    body: JSON.stringify(patch),
  });
}

export async function listGenerationRuns(): Promise<GenerationRun[]> {
  const data = await fetchJson<{ items: GenerationRun[] }>("/admin/generation-runs");
  return data.items;
}

export async function createGenerationRun(request: CreateGenerationRunRequest = { requested_by: "app" }): Promise<GenerationRun> {
  return fetchJson<GenerationRun>("/admin/generation-runs", {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function getGenerationRun(appRunId: string): Promise<GenerationRun> {
  return fetchJson<GenerationRun>(`/admin/generation-runs/${appRunId}`);
}

export async function cancelGenerationRun(appRunId: string): Promise<GenerationRun> {
  return fetchJson<GenerationRun>(`/admin/generation-runs/${appRunId}/cancel`, {
    method: "POST",
  });
}

export async function syncKnowledgeBase(request: KnowledgeBaseSyncRequest = { full_refresh: true }): Promise<KnowledgeBaseStatus> {
  return fetchJson<KnowledgeBaseStatus>("/admin/knowledge-base/sync", {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function cleanupKnowledgeBase(mode?: "off" | "dedupe" | "aggressive"): Promise<KnowledgeBaseStatus> {
  return fetchJson<KnowledgeBaseStatus>("/admin/knowledge-base/cleanup", {
    method: "POST",
    body: JSON.stringify({ mode }),
  });
}

export async function commitChatToKnowledgeBase(request: ChatCommitRequest): Promise<ChatCommitResponse> {
  return fetchJson<ChatCommitResponse>("/chat/commit", {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function getSalesWorkspace(clusterId: string, salesItemId: string, signal?: AbortSignal): Promise<SalesWorkspace> {
  return fetchJson<SalesWorkspace>(`/sales/opportunities/${clusterId}/items/${salesItemId}`, { signal });
}

export async function claimOpportunity(clusterId: string, request: ClaimOpportunityRequest): Promise<SalesWorkspace> {
  return fetchJson<SalesWorkspace>(`/sales/opportunities/${clusterId}/claim`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function updateSalesDraft(clusterId: string, salesItemId: string, request: SalesDraftPatchRequest): Promise<SalesWorkspace> {
  return fetchJson<SalesWorkspace>(`/sales/opportunities/${clusterId}/items/${salesItemId}/draft`, {
    method: "PATCH",
    body: JSON.stringify(request),
  });
}

export async function sendSalesDraftMessage(clusterId: string, salesItemId: string, request: SalesDraftConversationRequest): Promise<SalesWorkspace> {
  return fetchJson<SalesWorkspace>(`/sales/opportunities/${clusterId}/items/${salesItemId}/draft/chat`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function pushSalesDraft(clusterId: string, salesItemId: string, request: SalesWorkspaceActorRequest): Promise<SalesWorkspace> {
  return fetchJson<SalesWorkspace>(`/sales/opportunities/${clusterId}/items/${salesItemId}/push`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function updateSalesWorkspaceStatus(
  clusterId: string,
  salesItemId: string,
  request: SalesWorkspaceStatusPatchRequest,
): Promise<SalesWorkspace> {
  return fetchJson<SalesWorkspace>(`/sales/opportunities/${clusterId}/items/${salesItemId}/status`, {
    method: "PATCH",
    body: JSON.stringify(request),
  });
}

export async function getSalesDashboard(): Promise<SalesDashboard> {
  return fetchJson<SalesDashboard>("/sales/dashboard");
}

export async function getSalesLeads(params?: {
  page?: number;
  page_size?: number;
  sort_by?: "newest_event" | "highest_priority" | "best_confidence";
}): Promise<SalesLeadCatalog> {
  const query = new URLSearchParams();
  if (params?.page) query.set("page", String(params.page));
  if (params?.page_size) query.set("page_size", String(params.page_size));
  if (params?.sort_by) query.set("sort_by", params.sort_by);
  const suffix = query.size ? `?${query.toString()}` : "";
  return fetchJson<SalesLeadCatalog>(`/sales/leads${suffix}`);
}

// ---------------------------------------------------------------------------
// User memory (Sherlock AI)
// ---------------------------------------------------------------------------

export type UserProfile = {
  name?: string | null;
  role?: string | null;
  sector?: string | null;
  region?: string | null;
  deal_stages?: string[];
  active_pursuits?: string[];
  expertise_areas?: string[];
  key_deductions?: string[];
};

export type UserMemoryResponse = {
  user_id: string;
  profile: UserProfile;
  entries: Record<string, unknown>;
};

export async function getUserMemory(userId: string): Promise<UserMemoryResponse> {
  return fetchJson<UserMemoryResponse>(`/user/${encodeURIComponent(userId)}/memory`);
}

export async function upsertUserMemory(userId: string, memoryKey: string, memoryValue: unknown): Promise<void> {
  await fetchJson(`/user/${encodeURIComponent(userId)}/memory`, {
    method: "PUT",
    body: JSON.stringify({ memory_key: memoryKey, memory_value: memoryValue }),
  });
}

type ChatStreamHandlers = {
  signal?: AbortSignal;
  onDelta?: (text: string) => void;
  onResponse?: (response: ChatResponse) => void;
  onError?: (message: string) => void;
};

export async function streamChatResponse(request: ChatRequest, handlers: ChatStreamHandlers = {}): Promise<void> {
  const MAX_RETRIES = 3;
  const BASE_DELAY_MS = 1000;
  let lastResponseId = request.previous_response_id;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    if (attempt > 0) {
      const delay = Math.min(BASE_DELAY_MS * 2 ** (attempt - 1), 8000);
      await new Promise((resolve) => setTimeout(resolve, delay));
      if (handlers.signal?.aborted) throw new DOMException("Aborted", "AbortError");
    }

    const currentRequest = lastResponseId !== request.previous_response_id
      ? { ...request, previous_response_id: lastResponseId }
      : request;

    let response: Response;
    try {
      response = await fetch(`${API_BASE}/chat/responses/stream`, {
        method: "POST",
        cache: "no-store",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(currentRequest),
        signal: handlers.signal,
      });
    } catch (error) {
      if (isAbortError(error)) throw error;
      if (attempt < MAX_RETRIES) continue; // retry on network failure
      throw describeNetworkError("/chat/responses/stream", error);
    }

    if (!response.ok || !response.body) {
      let detail = `Chat request failed: ${response.status}`;
      try {
        const payload = await response.json();
        if (payload?.detail) detail = String(payload.detail);
      } catch {}
      // Only retry on 5xx or 429
      if (attempt < MAX_RETRIES && (response.status >= 500 || response.status === 429)) continue;
      throw new Error(detail);
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let streamFailed = false;

    function processEventBlock(block: string) {
      const lines = block.split("\n").map((line) => line.trimEnd());
      let eventName = "message";
      const dataLines: string[] = [];

      for (const line of lines) {
        if (line.startsWith("event:")) {
          eventName = line.slice(6).trim();
        } else if (line.startsWith("data:")) {
          dataLines.push(line.slice(5).trim());
        }
      }

      if (!dataLines.length) return;
      const raw = dataLines.join("\n");

      try {
        const payload = JSON.parse(raw);
        if (eventName === "delta" && typeof payload.text === "string") {
          handlers.onDelta?.(payload.text);
        } else if (eventName === "response") {
          if (payload.response_id) lastResponseId = payload.response_id;
          handlers.onResponse?.(payload as ChatResponse);
        } else if (eventName === "error") {
          handlers.onError?.(String(payload.message ?? "Chat request failed."));
        }
      } catch (error) {
        handlers.onError?.(error instanceof Error ? error.message : "Unable to parse chat stream.");
      }
    }

    try {
      while (true) {
        const { done, value } = await reader.read();
        buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done });

        let separatorIndex = buffer.indexOf("\n\n");
        while (separatorIndex >= 0) {
          const block = buffer.slice(0, separatorIndex).trim();
          buffer = buffer.slice(separatorIndex + 2);
          if (block) processEventBlock(block);
          separatorIndex = buffer.indexOf("\n\n");
        }

        if (done) break;
      }
    } catch (readError) {
      if (isAbortError(readError)) throw readError;
      streamFailed = true;
    }

    if (buffer.trim()) {
      processEventBlock(buffer.trim());
    }

    // If stream read failed mid-flight and we have retries left, loop back
    if (streamFailed && attempt < MAX_RETRIES) continue;
    if (streamFailed) throw new Error("Chat stream disconnected after retries.");

    // Successful completion — exit retry loop
    return;
  }
}
