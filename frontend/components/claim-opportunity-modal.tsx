"use client";

import { KeyboardEvent, useEffect, useId, useRef, useState } from "react";

import { formatDate, formatLabel, formatScore } from "@/lib/formatters";
import { SalesDraftPayload, SalesLead, SalesWorkspace } from "@/types/api";

/* ── Speech recognition type shim ── */

type SpeechRecognitionCtor = new () => {
  lang: string;
  interimResults: boolean;
  maxAlternatives: number;
  onresult: ((event: { results?: ArrayLike<ArrayLike<{ transcript?: string }>> }) => void) | null;
  onerror: ((event: { error?: string }) => void) | null;
  onend: (() => void) | null;
  start: () => void;
};

/* ── Props ── */

type Props = {
  open: boolean;
  opportunity: SalesLead | null;
  workspace: SalesWorkspace | null;
  loading: boolean;
  error: string;
  rememberedName: string;
  rememberedEmail: string;
  onRememberUser: (name: string, email: string) => void;
  onClose: () => void;
  onClaim: (payload: { claimed_by_name: string; claimed_by_email?: string; notes?: string }) => Promise<void>;
  onSaveDraft: (draft: SalesDraftPayload) => Promise<void>;
  onSendMessage: (message: string, channel: "chat" | "voice") => Promise<void>;
  onPush: () => Promise<void>;
};

/* ── Helpers ── */

type WizardStep = "claim" | "draft" | "confirm";

function listToText(values: string[]): string {
  return values.join("\n");
}

function textToList(value: string): string[] {
  return value.split("\n").map((item) => item.trim()).filter(Boolean);
}

/* ── Component ── */

export function ClaimOpportunityModal({
  open,
  opportunity,
  workspace,
  loading,
  error,
  rememberedName,
  rememberedEmail,
  onRememberUser,
  onClose,
  onClaim,
  onSaveDraft,
  onSendMessage,
  onPush,
}: Props) {
  /* ── Form state ── */
  const [claimName, setClaimName] = useState(rememberedName);
  const [claimEmail, setClaimEmail] = useState(rememberedEmail);
  const [claimNotes, setClaimNotes] = useState("");
  const [draft, setDraft] = useState<SalesDraftPayload | null>(workspace?.draft_payload ?? null);
  const [chatInput, setChatInput] = useState("");
  const [busyAction, setBusyAction] = useState<"claim" | "save" | "push" | "chat" | "voice" | "">("");
  const [voiceSupported, setVoiceSupported] = useState(false);
  const [voiceError, setVoiceError] = useState("");

  /* ── Wizard state ── */
  const [step, setStep] = useState<WizardStep>(workspace ? "draft" : "claim");
  const [draftSection, setDraftSection] = useState<"overview" | "strategy" | "evidence">("overview");

  /* ── Refs / ids ── */
  const modalRef = useRef<HTMLDivElement | null>(null);
  const closeButtonRef = useRef<HTMLButtonElement | null>(null);
  const messagesEndRef = useRef<HTMLDivElement | null>(null);
  const titleId = useId();

  /* ── Sync from props ── */
  useEffect(() => { setClaimName(rememberedName); }, [rememberedName]);
  useEffect(() => { setClaimEmail(rememberedEmail); }, [rememberedEmail]);
  useEffect(() => { setDraft(workspace?.draft_payload ?? null); }, [workspace]);
  useEffect(() => {
    if (workspace && step === "claim") setStep("draft");
  }, [workspace, step]);

  // Reset the wizard step on every modal open.
  // For claimed leads (opportunity.claim_id set), jump straight to "draft";
  // the workspace-load effect will advance from "claim" → "draft" anyway, but
  // setting it here avoids the flash of the Claim form for already-claimed items.
  const prevOpenRef = useRef(false);
  useEffect(() => {
    if (open && !prevOpenRef.current) {
      setStep(opportunity?.claim_id ? "draft" : "claim");
      setClaimNotes("");
      setVoiceError("");
    }
    prevOpenRef.current = open;
  }, [open, opportunity?.claim_id]);

  useEffect(() => {
    const w = window as Window & { SpeechRecognition?: SpeechRecognitionCtor; webkitSpeechRecognition?: SpeechRecognitionCtor };
    setVoiceSupported(Boolean(w.webkitSpeechRecognition || w.SpeechRecognition));
  }, []);

  /* ── Focus trap + Escape ── */
  const triggerRef = useRef<Element | null>(null);
  useEffect(() => {
    if (!open) return;
    triggerRef.current = document.activeElement;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    closeButtonRef.current?.focus();

    function handleKeyDown(event: globalThis.KeyboardEvent) {
      if (event.key === "Escape") { event.preventDefault(); onClose(); return; }
      if (event.key !== "Tab" || !modalRef.current) return;
      const focusable = modalRef.current.querySelectorAll<HTMLElement>(
        'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])',
      );
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => {
      document.body.style.overflow = prev;
      window.removeEventListener("keydown", handleKeyDown);
      // Restore focus to the element that opened the modal
      if (triggerRef.current && triggerRef.current instanceof HTMLElement) {
        triggerRef.current.focus();
      }
    };
  }, [onClose, open]);

  /* ── Auto-scroll chat ── */
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [workspace?.messages.length]);

  if (!open || !opportunity) return null;

  /* ── Actions ── */

  async function handleClaim() {
    if (!claimName.trim()) return;
    setBusyAction("claim");
    try {
      onRememberUser(claimName.trim(), claimEmail.trim());
      await onClaim({ claimed_by_name: claimName.trim(), claimed_by_email: claimEmail.trim() || undefined, notes: claimNotes.trim() || undefined });
      setStep("draft");
    } finally { setBusyAction(""); }
  }

  async function handleSave() {
    if (!draft) return;
    setBusyAction("save");
    try { await onSaveDraft(draft); } finally { setBusyAction(""); }
  }

  async function handlePush() {
    if (!draft) return;
    setBusyAction("push");
    try { await onSaveDraft(draft); await onPush(); } finally { setBusyAction(""); }
  }

  async function handleSend(channel: "chat" | "voice", messageOverride?: string) {
    const msg = (messageOverride ?? chatInput).trim();
    if (!msg) return;
    setBusyAction(channel === "voice" ? "voice" : "chat");
    try { await onSendMessage(msg, channel); if (!messageOverride) setChatInput(""); } finally { setBusyAction(""); }
  }

  function updateDraftField<K extends keyof SalesDraftPayload>(key: K, value: SalesDraftPayload[K]) {
    setDraft((c) => (c ? { ...c, [key]: value } : c));
  }

  function handleVoiceCapture() {
    const w = window as Window & { SpeechRecognition?: SpeechRecognitionCtor; webkitSpeechRecognition?: SpeechRecognitionCtor };
    const Ctor = w.SpeechRecognition || w.webkitSpeechRecognition;
    if (!Ctor) return;
    const recognition = new Ctor();
    recognition.lang = "en-US";
    recognition.interimResults = false;
    recognition.maxAlternatives = 1;
    setVoiceError("");
    setBusyAction("voice");
    recognition.onresult = (event) => { const t = event.results?.[0]?.[0]?.transcript?.trim(); if (t) void handleSend("voice", t); };
    recognition.onerror = (event) => { setVoiceError(event.error ? `Voice: ${event.error}` : "Voice failed."); setBusyAction(""); };
    recognition.onend = () => { setBusyAction((c) => (c === "voice" ? "" : c)); };
    recognition.start();
  }

  function handleBackdropKeyDown(event: KeyboardEvent<HTMLDivElement>) {
    if (event.key === "Escape") { event.preventDefault(); onClose(); }
  }

  /* ── Step indicator labels ── */
  const steps: { key: WizardStep; label: string }[] = [
    { key: "claim", label: "Claim" },
    { key: "draft", label: "Review & Refine" },
    { key: "confirm", label: "Push" },
  ];
  const stepIndex = steps.findIndex((s) => s.key === step);

  /* ── Render ── */

  return (
    <div className="modalBackdrop" onClick={onClose} onKeyDown={handleBackdropKeyDown}>
      <div
        ref={modalRef}
        className="modalCard cmModal"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        onClick={(e) => e.stopPropagation()}
      >
        {/* ── Top bar ── */}
        <div className="cmTopBar">
          <div className="cmTopBarLeft">
            <h2 id={titleId} className="cmTitle">{opportunity.subject_company_name}</h2>
            <div className="cmTopBarMeta">
              <span className="pill pill-blue">{formatLabel(opportunity.trigger_type)}</span>
              {opportunity.subject_country ? <span className="chip">{opportunity.subject_country}</span> : null}
              {opportunity.event_date ? <span className="cmMetaDate">{formatDate(opportunity.event_date)}</span> : null}
            </div>
          </div>
          <button ref={closeButtonRef} className="cmClose" type="button" onClick={onClose} aria-label="Close">✕</button>
        </div>

        {/* ── Step indicator ── */}
        <div className="cmSteps">
          {steps.map((s, i) => (
            <button
              key={s.key}
              className="cmStep"
              data-state={i < stepIndex ? "done" : i === stepIndex ? "active" : "upcoming"}
              aria-label={`Step ${i + 1}: ${s.label}${i < stepIndex ? " (completed)" : i === stepIndex ? " (current)" : ""}`}
              onClick={() => {
                if (s.key === "claim" && workspace) return;
                if (s.key === "draft" && !workspace) return;
                if (s.key === "confirm" && !workspace) return;
                setStep(s.key);
              }}
              disabled={s.key === "claim" && !!workspace}
            >
              <span className="cmStepDot">{i < stepIndex ? "✓" : i + 1}</span>
              <span className="cmStepLabel">{s.label}</span>
            </button>
          ))}
          <div className="cmStepLine" style={{ width: `${(stepIndex / (steps.length - 1)) * 100}%` }} />
        </div>

        {/* ── Step body ── */}
        <div className="cmBody">

          {/* ════════ Step 1: Claim ════════ */}
          {step === "claim" ? (
            <div className="cmClaimStep">
              <div className="cmSnapshotRow">
                <div className="cmSnapshotItem"><span>Event</span><strong>{opportunity.event_subject_company_name}</strong></div>
                <div className="cmSnapshotItem"><span>Branch</span><strong>{formatLabel(opportunity.branch_type)}</strong></div>
                <div className="cmSnapshotItem"><span>Confidence</span><strong>{formatScore(opportunity.confidence_score)}</strong></div>
                <div className="cmSnapshotItem"><span>Opportunity</span><strong>{formatScore(opportunity.opportunity_score)}</strong></div>
              </div>

              <div className="cmClaimForm">
                <div className="cmClaimFields">
                  <label className="cmField">
                    <span className="cmFieldLabel">Your name</span>
                    <input className="cmInput" value={claimName} onChange={(e) => setClaimName(e.target.value)} placeholder="Jordan Lee" />
                  </label>
                  <label className="cmField">
                    <span className="cmFieldLabel">Email</span>
                    <input className="cmInput" value={claimEmail} onChange={(e) => setClaimEmail(e.target.value)} placeholder="jordan@company.com" />
                  </label>
                </div>
                <label className="cmField">
                  <span className="cmFieldLabel">AI instruction <span className="cmOptional">(optional)</span></span>
                  <textarea
                    className="cmInput cmTextarea"
                    value={claimNotes}
                    onChange={(e) => setClaimNotes(e.target.value)}
                    placeholder="Focus on restructuring angle, mention we know their finance team..."
                    rows={3}
                  />
                </label>
                <button
                  className="primaryButton cmClaimButton"
                  type="button"
                  disabled={busyAction === "claim" || loading || !claimName.trim()}
                  onClick={handleClaim}
                >
                  {busyAction === "claim" || loading ? "Generating draft..." : "Claim & generate draft"}
                </button>
                {error && step === "claim" ? (
                  <div className="cmInlineError">
                    <p>{error}</p>
                    <div className="cmInlineErrorActions">
                      <button className="secondaryButton" type="button" onClick={handleClaim} disabled={busyAction === "claim" || loading || !claimName.trim()}>
                        ↻ Try again
                      </button>
                      <button className="secondaryButton cmBtnMuted" type="button" onClick={onClose}>
                        Cancel
                      </button>
                    </div>
                  </div>
                ) : null}
              </div>
            </div>
          ) : null}

          {/* ════════ Step 2: Review & Refine ════════ */}
          {step === "draft" && workspace && draft ? (
            <div className="cmDraftStep">
              {/* Status strip */}
              <div className="cmStatusStrip">
                <span className="cmStatusBadge" data-status={workspace.status}>{formatLabel(workspace.status)}</span>
                <span className="cmStatusBadge" data-status={workspace.draft_status}>{formatLabel(workspace.draft_status)}</span>
                <span className="cmStatusMeta">Owner: <strong>{workspace.salesforce_owner_name}</strong></span>
                {workspace.last_pushed_at ? <span className="cmStatusMeta">Pushed: {formatDate(workspace.last_pushed_at)}</span> : null}
                <div className="cmStatusActions">
                  <button className="secondaryButton" type="button" disabled={busyAction === "save"} onClick={handleSave}>
                    {busyAction === "save" ? "Saving..." : "Save draft"}
                  </button>
                  <button className="primaryButton" type="button" onClick={() => setStep("confirm")}>
                    Push to Salesforce →
                  </button>
                </div>
              </div>

              <div className="cmDraftLayout">
                {/* Draft editor */}
                <div className="cmDraftEditor">
                  {/* Section tabs */}
                  <div className="cmDraftTabs">
                    {([
                      { key: "overview" as const, label: "Overview" },
                      { key: "strategy" as const, label: "Strategy & Outreach" },
                      { key: "evidence" as const, label: "Evidence & Lists" },
                    ]).map((tab) => (
                      <button
                        key={tab.key}
                        className="cmDraftTab"
                        data-active={draftSection === tab.key}
                        onClick={() => setDraftSection(tab.key)}
                      >
                        {tab.label}
                      </button>
                    ))}
                  </div>

                  {draftSection === "overview" ? (
                    <div className="cmFieldGroup">
                      <div className="cmFieldRow">
                        <label className="cmField">
                          <span className="cmFieldLabel">Company</span>
                          <input className="cmInput" value={draft.company_name} onChange={(e) => updateDraftField("company_name", e.target.value)} />
                        </label>
                        <label className="cmField">
                          <span className="cmFieldLabel">Owner</span>
                          <input className="cmInput" value={draft.owner_name} onChange={(e) => updateDraftField("owner_name", e.target.value)} />
                        </label>
                      </div>
                      <div className="cmFieldRow">
                        <label className="cmField">
                          <span className="cmFieldLabel">Email</span>
                          <input className="cmInput" value={draft.owner_email ?? ""} onChange={(e) => updateDraftField("owner_email", e.target.value)} />
                        </label>
                        <label className="cmField">
                          <span className="cmFieldLabel">SF Status</span>
                          <input className="cmInput" value={draft.salesforce_status} onChange={(e) => updateDraftField("salesforce_status", e.target.value)} />
                        </label>
                      </div>
                      <div className="cmFieldRow">
                        <label className="cmField">
                          <span className="cmFieldLabel">Priority</span>
                          <input className="cmInput" value={draft.priority_label} onChange={(e) => updateDraftField("priority_label", e.target.value)} />
                        </label>
                        <label className="cmField">
                          <span className="cmFieldLabel">Confidence</span>
                          <input className="cmInput" value={draft.confidence_label} onChange={(e) => updateDraftField("confidence_label", e.target.value)} />
                        </label>
                      </div>
                      <label className="cmField">
                        <span className="cmFieldLabel">Prospect summary</span>
                        <textarea className="cmInput cmTextarea" value={draft.prospect_summary} onChange={(e) => updateDraftField("prospect_summary", e.target.value)} rows={4} />
                      </label>
                      <label className="cmField">
                        <span className="cmFieldLabel">Why now</span>
                        <textarea className="cmInput cmTextarea" value={draft.why_now} onChange={(e) => updateDraftField("why_now", e.target.value)} rows={3} />
                      </label>
                    </div>
                  ) : null}

                  {draftSection === "strategy" ? (
                    <div className="cmFieldGroup">
                      <label className="cmField">
                        <span className="cmFieldLabel">Sales strategy</span>
                        <textarea className="cmInput cmTextarea" value={draft.sales_strategy} onChange={(e) => updateDraftField("sales_strategy", e.target.value)} rows={4} />
                      </label>
                      <label className="cmField">
                        <span className="cmFieldLabel">Outreach angle</span>
                        <textarea className="cmInput cmTextarea" value={draft.outreach_angle} onChange={(e) => updateDraftField("outreach_angle", e.target.value)} rows={4} />
                      </label>
                      <label className="cmField">
                        <span className="cmFieldLabel">Recommended next step</span>
                        <textarea className="cmInput cmTextarea" value={draft.recommended_next_step} onChange={(e) => updateDraftField("recommended_next_step", e.target.value)} rows={3} />
                      </label>
                      <label className="cmField">
                        <span className="cmFieldLabel">Internal notes</span>
                        <textarea className="cmInput cmTextarea" value={draft.internal_notes} onChange={(e) => updateDraftField("internal_notes", e.target.value)} rows={3} />
                      </label>
                    </div>
                  ) : null}

                  {draftSection === "evidence" ? (
                    <div className="cmFieldGroup">
                      <div className="cmFieldRow">
                        <label className="cmField">
                          <span className="cmFieldLabel">Stakeholder focus</span>
                          <textarea className="cmInput cmTextarea cmTextareaSmall" value={listToText(draft.stakeholder_focus)} onChange={(e) => updateDraftField("stakeholder_focus", textToList(e.target.value))} rows={4} />
                        </label>
                        <label className="cmField">
                          <span className="cmFieldLabel">Relevant services</span>
                          <textarea className="cmInput cmTextarea cmTextareaSmall" value={listToText(draft.relevant_services)} onChange={(e) => updateDraftField("relevant_services", textToList(e.target.value))} rows={4} />
                        </label>
                      </div>
                      <label className="cmField">
                        <span className="cmFieldLabel">Evidence bullets</span>
                        <textarea className="cmInput cmTextarea" value={listToText(draft.evidence_bullets)} onChange={(e) => updateDraftField("evidence_bullets", textToList(e.target.value))} rows={4} />
                      </label>
                      <label className="cmField">
                        <span className="cmFieldLabel">Source URLs</span>
                        <textarea className="cmInput cmTextarea cmTextareaSmall" value={listToText(draft.source_urls)} onChange={(e) => updateDraftField("source_urls", textToList(e.target.value))} rows={3} />
                      </label>
                    </div>
                  ) : null}
                </div>

                {/* Chat copilot rail */}
                <div className="cmChatRail">
                  <div className="cmChatHeader">
                    <strong>Draft Copilot</strong>
                    <span className="chip">{workspace.messages.length} messages</span>
                  </div>
                  <div className="cmChatMessages">
                    {workspace.messages.map((msg) => (
                      <div key={msg.message_id} className="cmMsg" data-role={msg.role}>
                        <div className="cmMsgMeta">
                          <span className="cmMsgRole">{msg.role === "assistant" ? "AI" : msg.role === "system" ? "SYS" : "You"}</span>
                          <span className="cmMsgTime">{formatDate(msg.created_at)}</span>
                        </div>
                        <p className="cmMsgText">{msg.content}</p>
                      </div>
                    ))}
                    <div ref={messagesEndRef} />
                  </div>
                  <div className="cmChatComposer">
                    <textarea
                      className="cmInput cmChatInput"
                      value={chatInput}
                      onChange={(e) => setChatInput(e.target.value)}
                      placeholder="Ask AI to adjust the draft..."
                      rows={2}
                      onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); void handleSend("chat"); } }}
                    />
                    <div className="cmChatActions">
                      {voiceSupported ? (
                        <button className="cmVoiceBtn" type="button" disabled={busyAction === "voice"} onClick={handleVoiceCapture} aria-label="Voice input">
                          {busyAction === "voice" ? "●" : "🎙"}
                        </button>
                      ) : null}
                      <button className="cmSendBtn" type="button" disabled={busyAction === "chat" || !chatInput.trim()} onClick={() => void handleSend("chat")}>
                        {busyAction === "chat" ? "..." : "Send"}
                      </button>
                    </div>
                    {voiceError ? <span className="cmVoiceError">{voiceError}</span> : null}
                  </div>

                  {/* CRM context */}
                  {workspace.match_summary ? (
                    <div className="cmCrmContext">
                      <span className="cmFieldLabel">CRM Match</span>
                      <div className="cmCrmGrid">
                        <div><span>Contacts</span><strong>{workspace.match_summary.contact_count}</strong></div>
                        <div><span>Open opps</span><strong>{workspace.match_summary.open_opportunity_count}</strong></div>
                      </div>
                      {workspace.match_summary.relationship_summary ? (
                        <p className="cmCrmSummary">{workspace.match_summary.relationship_summary}</p>
                      ) : null}
                    </div>
                  ) : null}
                </div>
              </div>
            </div>
          ) : step === "draft" && workspace && workspace.draft_status === "generating" ? (
            <div className="cmLoading">
              <div className="cmLoadingDot" />
              <p>Generating your AI-drafted prospect… This can take up to a minute. The draft will appear automatically.</p>
            </div>
          ) : step === "draft" && !workspace ? (
            error ? (
              <div style={{ padding: "40px 24px", textAlign: "center", display: "grid", gap: "16px" }}>
                <p style={{ color: "var(--text)", opacity: 0.8 }}>{error}</p>
                <button className="secondaryButton" type="button" style={{ justifySelf: "center" }} onClick={() => setStep("claim")}>
                  ← Back to claim
                </button>
              </div>
            ) : (
              <div className="cmLoading">
                <div className="cmLoadingDot" />
                <p>Loading workspace...</p>
              </div>
            )
          ) : null}

          {/* ════════ Step 3: Push Confirmation ════════ */}
          {step === "confirm" && workspace && draft ? (
            <div className="cmConfirmStep">
              <div className="cmConfirmCard">
                <h3 className="cmConfirmTitle">Ready to push to Salesforce?</h3>
                <p className="cmConfirmDesc">This will create or update a {formatLabel(workspace.salesforce_record_type)} record in Salesforce. Review the summary below.</p>

                <div className="cmConfirmGrid">
                  <div className="cmConfirmItem"><span>Company</span><strong>{draft.company_name}</strong></div>
                  <div className="cmConfirmItem"><span>Owner</span><strong>{draft.owner_name}</strong></div>
                  <div className="cmConfirmItem"><span>Priority</span><strong>{draft.priority_label}</strong></div>
                  <div className="cmConfirmItem"><span>Confidence</span><strong>{draft.confidence_label}</strong></div>
                  <div className="cmConfirmItem"><span>SF Status</span><strong>{draft.salesforce_status}</strong></div>
                  <div className="cmConfirmItem"><span>Record type</span><strong>{formatLabel(workspace.salesforce_record_type)}</strong></div>
                </div>

                <div className="cmConfirmPreview">
                  <span className="cmFieldLabel">Prospect summary</span>
                  <p>{draft.prospect_summary}</p>
                </div>

                <div className="cmConfirmPreview">
                  <span className="cmFieldLabel">Why now</span>
                  <p>{draft.why_now}</p>
                </div>

                {workspace.salesforce_record_id ? (
                  <div className="cmConfirmNotice">
                    Existing record: <strong>{workspace.salesforce_record_id}</strong> — this will update in place.
                  </div>
                ) : null}
              </div>

              <div className="cmConfirmActions">
                <button className="secondaryButton" type="button" onClick={() => setStep("draft")}>
                  ← Back to draft
                </button>
                <button className="primaryButton cmPushButton" type="button" disabled={busyAction === "push"} onClick={handlePush}>
                  {busyAction === "push" ? "Pushing..." : "Confirm & push to Salesforce"}
                </button>
              </div>
            </div>
          ) : null}

        </div>

        {error ? <div className="cmError">{error}</div> : null}
      </div>
    </div>
  );
}
