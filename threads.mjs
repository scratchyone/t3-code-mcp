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
