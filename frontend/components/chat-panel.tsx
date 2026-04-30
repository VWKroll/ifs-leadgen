"use client";

import { FormEvent, useEffect, useRef, useState } from "react";

import { RichTextBlock } from "@/components/rich-text-block";
import { streamChatResponse } from "@/lib/api";
import { formatDate, formatLabel } from "@/lib/formatters";
import { ChatCommitResponse, ChatMessage, ChatRequest, KnowledgeBaseStatus } from "@/types/api";

type ChatScope = "selected_cluster" | "all";
type ChatPanelMode = "full" | "embedded";

type Props = {
  selectedClusterId: string;
  selectedClusterName: string;
  knowledgeBase: KnowledgeBaseStatus | null;
  onCommitChat: (messages: ChatMessage[], previousResponseId: string | null) => Promise<ChatCommitResponse>;
  mode?: ChatPanelMode;
  assistantTitle?: string;
  assistantDescription?: string;
  contextTitle?: string;
  contextDescription?: string;
  contextPromptPrefix?: string;
  suggestedPrompts?: string[];
  contextChips?: string[];
  requestContext?: Pick<ChatRequest, "active_tab" | "entity_id" | "source_id" | "graph_node_id" | "region_id" | "country_id">;
  defaultScope?: ChatScope;
  lockScope?: boolean;
  selectedScopeLabel?: string;
  corpusScopeLabel?: string;
  selectedScopeDescription?: string;
  corpusScopeDescription?: string;
  footerMeta?: Array<{ label: string; value: string }>;
  userId?: string | null;
};

type StoredChatSession = {
  id: string;
  title: string;
  createdAt: string;
  updatedAt: string;
  clusterId: string;
  messages: ChatMessage[];
  previousResponseId: string;
  scope: ChatScope;
};

type StoredChatState = {
  activeSessionId: string;
  sessions: StoredChatSession[];
};

const STORAGE_KEY = "idc-analyst-chat-session-v2";
const MAX_STORED_SESSIONS = 12;

function emptyAssistantMessage(): ChatMessage {
  return { role: "assistant", content: "", citations: [] };
}

function createSession(scope: ChatScope, clusterId: string): StoredChatSession {
  const now = new Date().toISOString();
  return {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    title: "New strategy thread",
    createdAt: now,
    updatedAt: now,
    clusterId,
    messages: [],
    previousResponseId: "",
    scope,
  };
}

function sessionTitleFromMessages(messages: ChatMessage[]): string {
  const firstUserMessage = messages.find((message) => message.role === "user" && message.content.trim());
  return firstUserMessage ? firstUserMessage.content.trim().slice(0, 56) : "New strategy thread";
}

function formatSessionTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Unknown time";
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

/* ─── Streaming dots indicator ─── */
function StreamingDots() {
  return (
    <span className="cpStreamDots" aria-label="Thinking">
      <span className="cpDot" />
      <span className="cpDot" />
      <span className="cpDot" />
    </span>
  );
}

export function ChatPanel({
  selectedClusterId,
  selectedClusterName,
  knowledgeBase,
  onCommitChat,
  mode = "full",
  assistantTitle = "Sherlock AI",
  assistantDescription,
  contextTitle = "Current Context",
  contextDescription,
  contextPromptPrefix = "",
  suggestedPrompts,
  contextChips = [],
  requestContext,
  defaultScope,
  lockScope = false,
  footerMeta = [],
  userId,
}: Props) {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [scope, setScope] = useState<ChatScope>(defaultScope ?? (selectedClusterId ? "selected_cluster" : "all"));
  const [previousResponseId, setPreviousResponseId] = useState("");
  const [activeSessionId, setActiveSessionId] = useState("");
  const [storedSessions, setStoredSessions] = useState<StoredChatSession[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [commitLoading, setCommitLoading] = useState(false);
  const [commitNotice, setCommitNotice] = useState("");
  const [threadMenuOpen, setThreadMenuOpen] = useState(false);
  const lastFailedMessageRef = useRef("");
  const hydratedRef = useRef(false);
  const autoScopeSettledRef = useRef(false);
  const storedSessionsRef = useRef<StoredChatSession[]>([]);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  /* ── Auto-scroll to bottom on new messages ── */
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  /* ── Hydrate from localStorage ── */
  useEffect(() => {
    if (hydratedRef.current) return;

    let restoredScope: ChatScope = defaultScope ?? (selectedClusterId ? "selected_cluster" : "all");
    let restoredActiveSession = false;
    try {
      const raw = window.localStorage.getItem(STORAGE_KEY);
      if (raw && !lockScope) {
        const stored = JSON.parse(raw) as StoredChatState;
        const sessions = Array.isArray(stored.sessions) ? stored.sessions : [];
        const activeSession = sessions.find((session) => session.id === stored.activeSessionId) ?? sessions[0];

        if (activeSession) {
          setMessages(Array.isArray(activeSession.messages) ? activeSession.messages : []);
          setPreviousResponseId(activeSession.previousResponseId ?? "");
          setScope(activeSession.scope ?? restoredScope);
          setActiveSessionId(activeSession.id);
          setStoredSessions(sessions);
          storedSessionsRef.current = sessions;
          restoredScope = activeSession.scope ?? restoredScope;
          autoScopeSettledRef.current = true;
          restoredActiveSession = true;
        }
      }
    } catch {}
    if (!lockScope && !restoredActiveSession) {
      const initialSession = createSession(restoredScope, selectedClusterId);
      setActiveSessionId(initialSession.id);
      setStoredSessions([initialSession]);
      storedSessionsRef.current = [initialSession];
    }
    hydratedRef.current = true;
  }, [defaultScope, lockScope, selectedClusterId]);

  useEffect(() => {
    if (!hydratedRef.current) return;
    if (autoScopeSettledRef.current || lockScope || defaultScope) return;
    if (messages.length || previousResponseId) return;
    if (scope === "all" && selectedClusterId) {
      autoScopeSettledRef.current = true;
      setScope("selected_cluster");
    }
  }, [defaultScope, lockScope, messages.length, previousResponseId, scope, selectedClusterId]);

  useEffect(() => {
    if (!hydratedRef.current) return;
    if (scope === "selected_cluster" && !selectedClusterId) {
      setScope("all");
    }
  }, [scope, selectedClusterId]);

  useEffect(() => {
    if (!hydratedRef.current) return;
    if (lockScope || !activeSessionId) return;

    const now = new Date().toISOString();
    const existingSessions = storedSessionsRef.current;
    const existingSession = existingSessions.find((session) => session.id === activeSessionId);
    const nextSession: StoredChatSession = {
      id: activeSessionId,
      title: sessionTitleFromMessages(messages),
      createdAt: existingSession?.createdAt ?? now,
      updatedAt: now,
      clusterId: scope === "selected_cluster" ? selectedClusterId : "",
      messages,
      previousResponseId,
      scope,
    };
    const nextSessions = [nextSession, ...existingSessions.filter((session) => session.id !== activeSessionId)]
      .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))
      .slice(0, MAX_STORED_SESSIONS);

    storedSessionsRef.current = nextSessions;
    setStoredSessions(nextSessions);
    window.localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        activeSessionId,
        sessions: nextSessions,
      } satisfies StoredChatState),
    );
  }, [activeSessionId, lockScope, messages, previousResponseId, scope, selectedClusterId]);

  const suggestionButtons =
    scope === "selected_cluster" && selectedClusterName
      ? [
          `Deduce the strongest opportunity around ${selectedClusterName}.`,
          `Which entities in ${selectedClusterName} demand immediate investigation?`,
          `What evidence chain supports this cluster's score?`,
        ]
      : [
          "Which leads demand my attention first? Rank by urgency.",
          "Search for the highest-scoring opportunities in my region.",
          "What patterns are emerging across recent trigger events?",
        ];

  const activeSuggestions = suggestedPrompts?.length ? suggestedPrompts : suggestionButtons;
  const activeDescription =
    assistantDescription ??
    (scope === "selected_cluster" && selectedClusterName
      ? `Deductive analysis grounded in the opportunity around ${selectedClusterName}.`
      : "Deductive reasoning across the full opportunity knowledge base — with persistent memory.");
  const activeContextDescription =
    contextDescription ??
    (scope === "selected_cluster"
      ? selectedClusterName || "No cluster is currently selected."
      : "All generated opportunity documents available through the knowledge base.");

  const kbStatusLabel = knowledgeBase ? formatLabel(knowledgeBase.status) : "Unknown";
  const kbStatusClass =
    knowledgeBase?.status === "ready"
      ? "chip chip-green"
      : knowledgeBase?.status === "fallback"
        ? "chip chip-amber"
        : knowledgeBase?.status === "not_configured"
          ? "chip chip-red"
          : "chip chip-neutral";

  const recentSessions = storedSessions
    .filter((session) => session.id !== activeSessionId)
    .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))
    .slice(0, 4);

  function persistThreadState(nextActiveSessionId: string, nextSessions: StoredChatSession[]) {
    setActiveSessionId(nextActiveSessionId);
    setStoredSessions(nextSessions);
    storedSessionsRef.current = nextSessions;
    if (!lockScope) {
      window.localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({
          activeSessionId: nextActiveSessionId,
          sessions: nextSessions,
        } satisfies StoredChatState),
      );
    }
  }

  function startNewThread(nextScope = scope) {
    autoScopeSettledRef.current = true;
    const nextSession = createSession(nextScope, nextScope === "selected_cluster" ? selectedClusterId : "");
    const nextSessions = [nextSession, ...storedSessions.filter((session) => session.id !== nextSession.id)]
      .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))
      .slice(0, MAX_STORED_SESSIONS);

    persistThreadState(nextSession.id, nextSessions);
    setMessages([]);
    setPreviousResponseId("");
    setError("");
    setCommitNotice("");
    setScope(nextScope);
  }

  function openStoredSession(sessionId: string) {
    const session = storedSessions.find((item) => item.id === sessionId);
    if (!session) return;
    persistThreadState(session.id, storedSessions);
    setMessages(session.messages);
    setPreviousResponseId(session.previousResponseId);
    setScope(session.scope);
    setError("");
    setCommitNotice("");
    setThreadMenuOpen(false);
  }

  async function handleCommit() {
    if (scope !== "selected_cluster" || !selectedClusterId || commitLoading || loading || messages.length < 2) return;
    setCommitLoading(true);
    setError("");
    setCommitNotice("");
    try {
      const response = await onCommitChat(messages, previousResponseId || null);
      setCommitNotice(`Committed to knowledge base as "${response.title}".`);
    } catch (commitError) {
      setError(commitError instanceof Error ? commitError.message : "Unable to commit chat to the knowledge base.");
    } finally {
      setCommitLoading(false);
    }
  }

  async function submitMessage(messageText: string) {
    const message = messageText.trim();
    if (!message || loading) return;
    if (scope === "selected_cluster" && !selectedClusterId) {
      setError("Select an opportunity cluster to continue.");
      return;
    }

    setLoading(true);
    setError("");
    setInput("");
    lastFailedMessageRef.current = message;

    const userMessage: ChatMessage = { role: "user", content: message, citations: [] };
    const transportMessage = contextPromptPrefix
      ? `${contextPromptPrefix.trim()}\n\nAnalyst question:\n${message}`
      : message;
    setMessages((current) => [...current, userMessage, emptyAssistantMessage()]);

    const controller = new AbortController();

    try {
      await streamChatResponse(
        {
          message: transportMessage,
          previous_response_id: previousResponseId || undefined,
          selected_cluster_id: scope === "selected_cluster" ? selectedClusterId : undefined,
          scope,
          active_tab: requestContext?.active_tab,
          entity_id: scope === "selected_cluster" ? requestContext?.entity_id : undefined,
          source_id: scope === "selected_cluster" ? requestContext?.source_id : undefined,
          graph_node_id: scope === "selected_cluster" ? requestContext?.graph_node_id : undefined,
          region_id: requestContext?.region_id,
          country_id: requestContext?.country_id,
          user_id: userId || undefined,
        },
        {
          signal: controller.signal,
          onDelta: (text) => {
            setMessages((current) => {
              const next = [...current];
              const last = next[next.length - 1];
              if (!last || last.role !== "assistant") return current;
              next[next.length - 1] = { ...last, content: `${last.content}${text}` };
              return next;
            });
          },
          onResponse: (response) => {
            setMessages((current) => {
              const next = [...current];
              if (!next.length) return current;
              next[next.length - 1] = response.message;
              return next;
            });
            // Don't store fake response IDs (e.g. "onboarding") that break conversation chaining
            if (response.response_id && response.response_id !== "onboarding") {
              setPreviousResponseId(response.response_id);
            }
          },
          onError: (messageText) => {
            setError(messageText);
            setMessages((current) => {
              const next = [...current];
              const last = next[next.length - 1];
              if (!last || last.role !== "assistant") return current;
              if (!last.content.trim()) {
                next[next.length - 1] = { role: "assistant", content: messageText, citations: [] };
              }
              return next;
            });
          },
        },
      );
    } catch (streamError) {
      setError(streamError instanceof Error ? streamError.message : "Unable to reach analyst chat.");
      setMessages((current) => {
        const next = [...current];
        const last = next[next.length - 1];
        if (!last || last.role !== "assistant") return current;
        if (!last.content.trim()) {
          next[next.length - 1] = {
            role: "assistant",
            content: streamError instanceof Error ? streamError.message : "Unable to reach analyst chat.",
            citations: [],
          };
        }
        return next;
      });
    } finally {
      controller.abort();
      setLoading(false);
    }
  }

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    void submitMessage(input);
  }

  function discardLastExchange() {
    setMessages((current) => {
      // Remove trailing assistant + user pair from a failed exchange
      const next = [...current];
      if (next.length >= 2 && next[next.length - 1]?.role === "assistant" && next[next.length - 2]?.role === "user") {
        next.splice(next.length - 2, 2);
      } else if (next.length >= 1 && next[next.length - 1]?.role === "assistant") {
        next.splice(next.length - 1, 1);
      }
      return next;
    });
    setError("");
    lastFailedMessageRef.current = "";
  }

  function retryLastMessage() {
    const msg = lastFailedMessageRef.current;
    if (!msg) return;
    // Remove the failed exchange before resending
    discardLastExchange();
    void submitMessage(msg);
  }

  /* ── Context label ── */
  const contextLabel =
    scope === "selected_cluster"
      ? selectedClusterName || "None"
      : requestContext?.country_id
        ? `${requestContext.country_id} · ${requestContext.region_id ?? "region"}`
        : requestContext?.region_id
          ? requestContext.region_id
          : "Full knowledge base";

  /* ── Shared: message list ── */
  function renderMessageList() {
    if (!messages.length) {
      return (
        <div className="cpEmptyState">
          <div className="cpEmptyIcon">🔍</div>
          <h3 className="cpEmptyTitle">The game is afoot</h3>
          <p className="cpEmptyHint">State your inquiry, or select a deduction below.</p>
          <div className="cpSuggestionGrid">
            {activeSuggestions.map((suggestion) => (
              <button
                key={suggestion}
                className="cpSuggestionBtn"
                disabled={loading}
                onClick={() => void submitMessage(suggestion)}
                type="button"
              >
                <span className="cpSuggestionIcon">→</span>
                <span>{suggestion}</span>
              </button>
            ))}
          </div>
        </div>
      );
    }

    return (
      <>
        {messages.map((message, index) => {
          const isAssistant = message.role === "assistant";
          const isStreaming = isAssistant && loading && index === messages.length - 1;
          const isEmpty = !message.content.trim();

          return (
            <article key={`${message.role}-${index}`} className={`cpBubble ${isAssistant ? "cpBubbleAssistant" : "cpBubbleUser"}`}>
              <div className="cpBubbleHeader">
                <span className={`cpAvatar ${isAssistant ? "cpAvatarAI" : "cpAvatarUser"}`}>
                  {isAssistant ? "🔍" : "U"}
                </span>
                <span className="cpBubbleRole">{isAssistant ? "Sherlock AI" : "You"}</span>
                {isStreaming && <StreamingDots />}
              </div>
              {isEmpty && isStreaming ? null : (
                <div className="cpBubbleBody">
                  <RichTextBlock className="bodyText bodyTextCompact" text={message.content} />
                </div>
              )}
              {message.citations.length ? (
                <div className="cpCitations">
                  <span className="cpCitationLabel">Sources</span>
                  <div className="cpCitationList">
                    {message.citations.map((citation) =>
                      citation.url && /^https?:\/\//i.test(citation.url) ? (
                        <a key={citation.id} className="cpCitationLink" href={citation.url} target="_blank" rel="noreferrer">
                          {citation.label}
                        </a>
                      ) : (
                        <span key={citation.id} className="cpCitationChip">
                          {citation.label}
                        </span>
                      ),
                    )}
                  </div>
                </div>
              ) : null}
            </article>
          );
        })}
        <div ref={messagesEndRef} />
      </>
    );
  }

  /* ── Shared: composer ── */
  function renderComposer() {
    return (
      <form className="cpComposer" onSubmit={handleSubmit}>
        <div className="cpComposerWrap">
          <textarea
            className="cpTextarea"
            disabled={loading}
            onChange={(event) => setInput(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter" && !event.shiftKey) {
                event.preventDefault();
                void submitMessage(input);
              }
            }}
            placeholder={
              scope === "selected_cluster"
                ? "Ask about this event, evidence, or next steps..."
                : requestContext?.active_tab === "global_graph"
                  ? "Ask a regional or country-level research question..."
                  : "Ask a cross-cluster research question..."
            }
            rows={mode === "embedded" ? 3 : 4}
            value={input}
          />
          <div className="cpComposerActions">
            <span className="cpContextBadge" title={contextLabel}>
              <span className="cpContextDot" />
              {contextLabel}
            </span>
            <button className="cpSendBtn" disabled={loading || !input.trim()} type="submit">
              {loading ? <StreamingDots /> : "Send →"}
            </button>
          </div>
        </div>
        {footerMeta.length ? (
          <div className="cpFooterMeta">
            {footerMeta.map((item) => (
              <span key={`${item.label}-${item.value}`} className="chip chip-neutral">
                <strong>{item.label}:</strong>&nbsp;{item.value}
              </span>
            ))}
          </div>
        ) : null}
      </form>
    );
  }

  /* ── Thread menu ── */
  function renderThreadMenu() {
    return (
      <div className="cpThreadMenu">
        <button
          className="cpThreadMenuToggle"
          onClick={() => setThreadMenuOpen(!threadMenuOpen)}
          type="button"
          aria-label={threadMenuOpen ? "Close threads menu" : "Open threads menu"}
        >
          {threadMenuOpen ? "✕" : "☰"} Threads{recentSessions.length ? ` (${recentSessions.length + 1})` : ""}
        </button>
        {threadMenuOpen && (
          <div className="cpThreadDropdown">
            <button
              className="cpThreadItem cpThreadItemNew"
              disabled={loading || commitLoading}
              onClick={() => { startNewThread(); setThreadMenuOpen(false); }}
              type="button"
            >
              <span className="cpThreadItemIcon">+</span>
              <div>
                <strong>New thread</strong>
                <span>Start a fresh conversation</span>
              </div>
            </button>
            {recentSessions.map((session) => (
              <button
                key={session.id}
                className="cpThreadItem"
                disabled={loading || commitLoading}
                onClick={() => openStoredSession(session.id)}
                type="button"
              >
                <span className="cpThreadItemIcon">💬</span>
                <div>
                  <strong>{session.title}</strong>
                  <span>{formatSessionTimestamp(session.updatedAt)}</span>
                </div>
              </button>
            ))}
          </div>
        )}
      </div>
    );
  }

  /* ── Notice cards ── */
  function renderNotices() {
    return (
      <>
        {commitNotice && (
          <div className="cpNotice cpNoticeSuccess">
            <span className="cpNoticeIcon">✓</span>
            <div>
              <strong>Knowledge Base Updated</strong>
              <RichTextBlock className="bodyText bodyTextMuted" text={commitNotice} />
            </div>
          </div>
        )}
        {error && (
          <div className="cpNotice cpNoticeError">
            <span className="cpNoticeIcon">!</span>
            <div>
              <strong>Error</strong>
              <RichTextBlock className="bodyText bodyTextMuted" text={error} />
              <div className="cpNoticeActions">
                {lastFailedMessageRef.current && (
                  <button className="cpNoticeBtn" type="button" onClick={retryLastMessage} disabled={loading}>
                    ↻ Retry
                  </button>
                )}
                <button className="cpNoticeBtn cpNoticeBtnMuted" type="button" onClick={discardLastExchange}>
                  Discard
                </button>
              </div>
            </div>
          </div>
        )}
        {knowledgeBase?.last_error && (
          <div className="cpNotice cpNoticeWarn">
            <span className="cpNoticeIcon">⚠</span>
            <div>
              <strong>Knowledge Base Notice</strong>
              <RichTextBlock className="bodyText bodyTextMuted" text={knowledgeBase.last_error} />
            </div>
          </div>
        )}
      </>
    );
  }

  /* ════════════════════════════════════════════════
     EMBEDDED MODE (side rail in EI / GKG)
     ════════════════════════════════════════════════ */
  if (mode === "embedded") {
    const headerChips = contextChips.slice(0, 3);

    return (
      <section className="cpPanel">
        {/* ── Identity header ── */}
        <div className="cpIdentity">
          <div className="cpIdentityRow">
            <span className="cpLogo">🔍</span>
            <div>
              <h2 className="cpTitle">{assistantTitle}</h2>
              <p className="cpSubtitle">{activeDescription}</p>
            </div>
          </div>
          {headerChips.length ? (
            <div className="cpChips">
              {headerChips.map((chip) => (
                <span key={chip} className="cpChip">{chip}</span>
              ))}
            </div>
          ) : null}
        </div>

        {/* ── Toolbar ── */}
        <div className="cpToolbar">
          <div className="cpToolbarLeft">
            {messages.length ? (
              <span className="cpMsgCount">{messages.length} messages</span>
            ) : (
              <span className="cpMsgCount">New thread</span>
            )}
          </div>
          <div className="cpToolbarRight">
            <button
              className="cpToolbarBtn"
              disabled={scope !== "selected_cluster" || !selectedClusterId || loading || commitLoading || messages.length < 2}
              onClick={() => void handleCommit()}
              title="Save to knowledge base"
              aria-label="Save to knowledge base"
              type="button"
            >
              {commitLoading ? "…" : "💾 Save"}
            </button>
            {renderThreadMenu()}
          </div>
        </div>

        {/* ── Messages ── */}
        <div className="cpMessages">
          {renderMessageList()}
        </div>

        {/* ── Composer ── */}
        {renderComposer()}

        {/* ── Notices ── */}
        {renderNotices()}
      </section>
    );
  }

  /* ════════════════════════════════════════════════
     FULL MODE (standalone chat page)
     ════════════════════════════════════════════════ */
  return (
    <section className="tabPanel cpFullLayout">
      {/* ── Header ── */}
      <header className="cpFullHeader">
        <div className="cpFullHeaderLeft">
          <span className="cpLogo cpLogoLg">🔍</span>
          <div>
            <h1 className="cpFullTitle">{assistantTitle}</h1>
            <p className="cpFullSubtitle">{activeDescription}</p>
          </div>
        </div>
        <div className="cpFullHeaderRight">
          <span className={kbStatusClass}>KB {kbStatusLabel}</span>
          {knowledgeBase?.last_synced_at ? (
            <span className="chip chip-blue">Synced {formatDate(knowledgeBase.last_synced_at)}</span>
          ) : null}
        </div>
      </header>

      {/* ── Context card ── */}
      <div className="cpFullContext">
        <div className="cpFullContextCard">
          <strong className="cpContextLabel">{contextTitle}</strong>
          <p className="bodyText bodyTextMuted">{activeContextDescription}</p>
          <div className="cpChips">
            <span className="cpChip">Persistent memory</span>
            {contextChips.map((chip) => (
              <span key={chip} className="cpChip">{chip}</span>
            ))}
          </div>
        </div>
      </div>

      {/* ── Notices ── */}
      {renderNotices()}

      {/* ── Conversation ── */}
      <div className="cpFullConversation">
        <div className="cpFullConvHeader">
          <div>
            <h2 className="panelTitle">Conversation</h2>
            <p className="bodyText bodyTextMuted">
              {messages.length ? "Responses stream live with citations attached." : "Start with a prompt below."}
            </p>
          </div>
          <div className="cpFullConvActions">
            <button
              className="secondaryButton"
              disabled={scope !== "selected_cluster" || !selectedClusterId || loading || commitLoading || messages.length < 2}
              onClick={() => void handleCommit()}
              type="button"
            >
              {commitLoading ? "Saving..." : "💾 Commit to KB"}
            </button>
            <button className="secondaryButton" disabled={loading || commitLoading} onClick={() => startNewThread()} type="button">
              + New thread
            </button>
          </div>
        </div>

        <div className="cpMessages cpMessagesFull">
          {renderMessageList()}
        </div>

        {renderComposer()}
      </div>
    </section>
  );
}
