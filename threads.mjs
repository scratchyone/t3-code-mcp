// Thread helpers shared by the tools (server.mjs) and MCP Events (events.mjs).

const RUNNING = new Set(["preparing", "queued", "starting", "running"]);

export const isSubagent = (t) => t.lineage?.relationshipToParent === "subagent";

export function threadState(t) {
  if (t.pendingRuntimeRequest || t.status === "waiting") return "waiting_on_you";
  if (t.activeRunId || RUNNING.has(t.status)) return "running";
  return t.status; // idle, completed, failed, interrupted, cancelled, rolled_back
}

export function projectMatches(p, query) {
  const q = query.toLowerCase();
  const base = p.workspaceRoot?.split("/").filter(Boolean).pop()?.toLowerCase();
  return p.id === query || p.title.toLowerCase() === q || base === q;
}

export const trunc = (s, n) => (typeof s === "string" && s.length > n ? s.slice(0, n) + "…" : s);

// --- read_thread text paging: never clip silently ---

export const DEFAULT_MESSAGE_CHARS = 8_000;
export const DEFAULT_TOTAL_CHARS = 24_000;
export const MAX_CHUNK_CHARS = 20_000;

// One chunk of a message's text with explicit continuation metadata.
export function textChunk(text, offset = 0, maxChars = DEFAULT_MESSAGE_CHARS) {
  const full = text ?? "";
  const start = Math.max(0, Math.min(offset, full.length));
  const chunk = full.slice(start, start + maxChars);
  const end = start + chunk.length;
  return {
    text: chunk,
    text_length: full.length,
    ...(start > 0 ? { text_offset: start } : {}),
    ...(end < full.length ? { text_truncated: true, next_text_offset: end } : {}),
  };
}

// Shapes the newest n messages. Text budget goes to the newest messages first; anything cut is
// marked text_truncated with next_text_offset, and its id is listed in truncated_message_ids.
export function shapeMessages(visible, n, { perMessage = DEFAULT_MESSAGE_CHARS, total = DEFAULT_TOTAL_CHARS, describe }) {
  const picked = visible.slice(-n);
  let budget = total;
  const shaped = new Array(picked.length);
  for (let i = picked.length - 1; i >= 0; i--) {
    const msg = picked[i];
    const allowance = Math.max(0, Math.min(perMessage, budget));
    const chunk = textChunk(msg.text, 0, allowance);
    budget -= chunk.text.length;
    shaped[i] = { message_id: msg.id, ...describe(msg), ...chunk };
  }
  return { messages: shaped, truncated_message_ids: shaped.filter((m) => m.text_truncated).map((m) => m.message_id) };
}

// --- send_message delivery: choose the dispatch, then read back what T3 actually did ---

const ACTIVE_RUN = new Set(["preparing", "starting", "running", "waiting"]);

export const activeRun = (projection) => (projection?.runs || []).findLast((r) => ACTIVE_RUN.has(r.status));

// A run is "waiting" briefly after its turn ends, while T3 captures a checkpoint. T3's "auto" can pick
// steering there, but its steer step only accepts running runs, so queue instead (it starts right after).
export const shouldQueue = (mode, projection) => mode === "queue" || activeRun(projection)?.status === "waiting";

export const pendingRequests = (projection) => (projection?.runtimeRequests || []).filter((r) => r.status === "pending");

// Thread state from a live snapshot (the shell list is cached for 10 s); falls back to the shell's.
export function liveState(projection, shellThread) {
  if (!projection) return threadState(shellThread);
  if (pendingRequests(projection).length) return "waiting_on_you";
  const run = activeRun(projection);
  if (run) return "running";
  return projection.runs?.at(-1)?.status ?? threadState(shellThread);
}

// Errors T3 raises when a steer can't land (turn ended or paused between our check and its lock). These are
// raised before anything is written, so retrying the same message as queued can't duplicate it.
const STEER_REJECTED = /cannot be steered|No running provider(?:InstanceId)? turn|no active provider session|is not the active turn|cannot satisfy (?:message dispatch mode steer_active|active_steering)|is not active\b/i;
export const isSteerRejection = (err) => STEER_REJECTED.test(err?.message || "");

// What happened to messageId, from a thread snapshot. Returns null if it isn't visible yet.
export function deliveryOutcome(projection, messageId, previousRunId) {
  const p = projection || {};
  const ownRun = (p.runs || []).find((r) => r.userMessageId === messageId);
  if (ownRun) {
    const replaced = previousRunId && previousRunId !== ownRun.id ? (p.runs || []).find((r) => r.id === previousRunId) : null;
    if (ownRun.status === "queued" || ownRun.queuePosition != null) {
      return { delivery: "queued", detail: "Queued; it starts as a new turn when the current turn ends.", run_id: ownRun.id, run_status: ownRun.status, ...(ownRun.queuePosition != null ? { queue_position: ownRun.queuePosition } : {}) };
    }
    if (replaced?.status === "interrupted") {
      return { delivery: "restarted", detail: "The running turn was interrupted and restarted with this message.", run_id: ownRun.id, run_status: ownRun.status };
    }
    return { delivery: "started", detail: "Started a new turn with this message.", run_id: ownRun.id, run_status: ownRun.status };
  }
  const item = (p.turnItems || []).find((i) => i.messageId === messageId && i.type === "user_message");
  const msg = (p.messages || []).find((m) => m.id === messageId);
  const runId = item?.runId || msg?.runId;
  // Attached to a run some other message started: steered into that turn.
  if (runId) {
    const run = (p.runs || []).find((r) => r.id === runId);
    return { delivery: "steered", detail: "Delivered into the running turn; the agent picks it up at its next step.", run_id: runId, run_status: run?.status };
  }
  return null;
}
