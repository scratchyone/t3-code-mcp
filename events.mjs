// MCP Events (webhook delivery) for T3 thread status: ChatGPT subscribes with events/subscribe and
// gets a signed POST when a thread finishes, fails, or starts waiting on the user.
//
// Spec: developers.openai.com/plugins/build/mcp-events + the MCP events design sketch
// (modelcontextprotocol/experimental-ext-triggers-events). Webhook mode only; no replay (cursor null).
//
// State (subscriptions incl. their signing secrets, last-seen thread states, delivered event ids)
// lives in state/events.json (dir 0700, file 0600). Several server processes may run (tunnel-client's
// plus ad-hoc test runs): any of them can accept subscribe/unsubscribe, but only the process holding
// state/watcher.lock watches T3 and delivers, so nothing is sent twice.
//
// Principal: the connector is reachable only through your private OpenAI tunnel (workspace-scoped,
// no other callers), so every call is treated as one principal, "tunnel-owner".

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { DeliveryError, MAX_BODY_BYTES, parseSecret, safePost, sign, statusReason } from "./webhook.mjs";
import { isSubagent, projectMatches, threadState, trunc } from "./threads.mjs";

export const EVENT_NAME = "t3.thread.status_changed";
export const NOTIFY_STATES = ["completed", "failed", "interrupted", "cancelled", "waiting_on_you"];
const PRINCIPAL = "tunnel-owner";
const DEFAULT_TTL_MS = 24 * 3600_000;
const MIN_TTL_MS = 5 * 60_000;
const MAX_TTL_MS = 7 * 24 * 3600_000;
const MAX_SUBSCRIPTIONS = 25;
const VERIFY_CACHE_MS = 24 * 3600_000;
const ROTATION_GRACE_MS = 10 * 60_000;
const RETRY_DELAYS_MS = [2_000, 8_000, 30_000, 120_000, 300_000];
const MAX_SENT_IDS = 2_000;

export class RpcError extends Error {
  constructor(code, message, data) { super(message); this.code = code; this.data = data; }
}

export const EVENT_DEFINITION = {
  name: EVENT_NAME,
  description:
    "Fires when a T3 Code thread on one of the user's machines finishes (completed), fails, is interrupted or cancelled, or starts waiting on the user (an approval or a question). The payload is a short status summary; call read_thread with thread_id for the details.",
  delivery: ["webhook"],
  inputSchema: {
    type: "object",
    properties: {
      machine: { type: "string", description: "Only this machine (e.g. desktop)." },
      project: { type: "string", description: "Only this project (name or id)." },
      thread_id: { type: "string", description: "Only this thread." },
      statuses: {
        type: "array",
        items: { type: "string", enum: NOTIFY_STATES },
        description: `Which new statuses to notify on. Default: all (${NOTIFY_STATES.join(", ")}).`,
      },
      include_subagents: { type: "boolean", description: "Also notify for subagent threads. Default false." },
    },
    additionalProperties: false,
  },
  payloadSchema: {
    type: "object",
    properties: {
      machine: { type: "string" },
      thread_id: { type: "string" },
      title: { type: "string" },
      project: { type: "string" },
      status: { type: "string", enum: NOTIFY_STATES },
      previous_status: { type: "string" },
      waiting_for: { type: "string", description: "What the thread is waiting on (approval, user_input, …) when status is waiting_on_you." },
      last_error: { type: "string", description: "Short error summary when status is failed." },
      occurred_at: { type: "string", format: "date-time" },
    },
    required: ["machine", "thread_id", "title", "project", "status", "occurred_at"],
  },
};

// Canonical JSON: sorted keys, so equal arguments always hash the same.
function canonical(v) {
  if (Array.isArray(v)) return `[${v.map(canonical).join(",")}]`;
  if (v && typeof v === "object") return `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${canonical(v[k])}`).join(",")}}`;
  return JSON.stringify(v);
}
const sha = (s) => crypto.createHash("sha256").update(s).digest("hex");
const subscriptionId = (url, name, args) => "sub_" + sha(canonical([PRINCIPAL, url, name, args])).slice(0, 24);

function validateArguments(args, machineNames) {
  if (args == null) return {};
  if (typeof args !== "object" || Array.isArray(args)) throw new RpcError(-32602, "arguments must be an object");
  const allowed = Object.keys(EVENT_DEFINITION.inputSchema.properties);
  for (const k of Object.keys(args)) if (!allowed.includes(k)) throw new RpcError(-32602, `Unknown argument: ${k}`);
  for (const k of ["machine", "project", "thread_id"]) {
    if (args[k] !== undefined && (typeof args[k] !== "string" || !args[k].trim())) throw new RpcError(-32602, `${k} must be a non-empty string`);
  }
  if (args.machine && !machineNames.some((m) => m.toLowerCase() === args.machine.toLowerCase())) {
    throw new RpcError(-32602, `Unknown machine "${args.machine}". Machines: ${machineNames.join(", ")}.`);
  }
  if (args.statuses !== undefined) {
    if (!Array.isArray(args.statuses) || !args.statuses.length || !args.statuses.every((s) => NOTIFY_STATES.includes(s))) {
      throw new RpcError(-32602, `statuses must be a non-empty list of: ${NOTIFY_STATES.join(", ")}`);
    }
  }
  if (args.include_subagents !== undefined && typeof args.include_subagents !== "boolean") {
    throw new RpcError(-32602, "include_subagents must be a boolean");
  }
  return args;
}

function validateDelivery(delivery, needSecret) {
  if (!delivery || delivery.mode !== "webhook") throw new RpcError(-32014, "Only webhook delivery is supported", { feature: "deliveryMode", value: delivery?.mode });
  let url;
  try { url = new URL(delivery.url); } catch { throw new RpcError(-32602, "delivery.url must be an https URL"); }
  const loopbackTest = process.env.T3_EVENTS_TEST_ALLOW_LOOPBACK === "1" && url.protocol === "http:" && url.hostname === "127.0.0.1";
  if (url.protocol !== "https:" && !loopbackTest) throw new RpcError(-32602, "delivery.url must be an https URL");
  if (needSecret && !parseSecret(delivery.secret)) throw new RpcError(-32602, "delivery.secret must be whsec_ + base64 of 24–64 bytes");
  return url.toString();
}

export class EventHub {
  /**
   * @param {object} o
   * @param {Array<{name: string, getShell(): Promise<object>}>} o.machines
   * @param {string} o.stateDir
   * @param {(msg: string) => void} o.log
   * @param {number} [o.pollMs]
   */
  constructor({ machines, stateDir, log, pollMs = 20_000 }) {
    this.machines = machines;
    this.stateDir = stateDir;
    this.file = path.join(stateDir, "events.json");
    this.lockFile = path.join(stateDir, "watcher.lock");
    this.log = log;
    this.pollMs = pollMs;
    this.timer = null;
    this.inflight = new Set();
    fs.mkdirSync(stateDir, { recursive: true, mode: 0o700 });
    fs.chmodSync(stateDir, 0o700);
  }

  // --- state ---

  load() {
    try {
      const s = JSON.parse(fs.readFileSync(this.file, "utf8"));
      return { subscriptions: s.subscriptions ?? {}, seen: s.seen ?? {}, seenAt: s.seenAt ?? {}, sent: s.sent ?? [], verified: s.verified ?? {} };
    } catch {
      return { subscriptions: {}, seen: {}, sent: [], verified: {} };
    }
  }

  // Read-modify-write under the same process; the rename keeps the file whole for other readers.
  update(fn) {
    const state = this.load();
    const result = fn(state);
    const tmp = `${this.file}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(state), { mode: 0o600 });
    fs.renameSync(tmp, this.file);
    return result;
  }

  // --- protocol methods ---

  list() {
    return { events: [EVENT_DEFINITION] };
  }

  async subscribe(params = {}) {
    if (params.name !== EVENT_NAME) throw new RpcError(-32011, `Unknown event: ${params.name}`, { kind: "event" });
    const args = validateArguments(params.arguments, this.machines.map((m) => m.name));
    const url = validateDelivery(params.delivery, true);
    const secret = params.delivery.secret;
    const id = subscriptionId(url, EVENT_NAME, args);
    this.log(`events: subscribe ${id} callback host ${new URL(url).host}, args ${JSON.stringify(args)}, ttlMs ${params.ttlMs}`);
    const now = Date.now();

    const existing = this.load().subscriptions[id];
    if (!existing && Object.keys(this.load().subscriptions).length >= MAX_SUBSCRIPTIONS) {
      throw new RpcError(-32013, "Too many subscriptions", { limit: "subscriptions", max: MAX_SUBSCRIPTIONS });
    }

    // Verify the endpoint unless this (principal, url) was verified recently.
    const verifiedAt = this.load().verified[url];
    if (!verifiedAt || now - verifiedAt > VERIFY_CACHE_MS) {
      await this.verifyEndpoint(url, id, [secret]);
      this.update((s) => { s.verified[url] = now; });
    }

    let ttl = params.ttlMs === undefined || params.ttlMs === null ? (params.ttlMs === null ? MAX_TTL_MS : DEFAULT_TTL_MS) : Number(params.ttlMs);
    if (!Number.isFinite(ttl)) ttl = DEFAULT_TTL_MS;
    ttl = Math.min(MAX_TTL_MS, Math.max(MIN_TTL_MS, ttl));
    const expiresAt = now + ttl;

    this.update((s) => {
      const prev = s.subscriptions[id];
      const rotated = prev && prev.secret !== secret;
      s.subscriptions[id] = {
        id, name: EVENT_NAME, arguments: args, url, secret,
        previousSecret: rotated ? prev.secret : prev?.previousSecret,
        previousSecretUntil: rotated ? now + ROTATION_GRACE_MS : prev?.previousSecretUntil,
        createdAt: prev?.createdAt ?? now, expiresAt, active: true, lastError: null,
      };
    });
    this.log(`events: subscription ${id} ${existing ? "refreshed" : "created"} until ${new Date(expiresAt).toISOString()}`);
    this.ensureWatcher();
    return { id, refreshBefore: new Date(expiresAt).toISOString(), cursor: null, truncated: false };
  }

  unsubscribe(params = {}) {
    if (params.name !== EVENT_NAME) throw new RpcError(-32011, `Unknown event: ${params.name}`, { kind: "event" });
    const args = validateArguments(params.arguments, this.machines.map((m) => m.name));
    const url = validateDelivery(params.delivery, false);
    const id = subscriptionId(url, EVENT_NAME, args);
    const removed = this.update((s) => { const had = !!s.subscriptions[id]; delete s.subscriptions[id]; return had; });
    this.log(`events: subscription ${id} ${removed ? "removed" : "already gone"}`);
    return {};
  }

  async verifyEndpoint(url, subId, secrets) {
    const challenge = crypto.randomBytes(24).toString("base64url");
    const body = JSON.stringify({ type: "verification", challenge });
    const msgId = `msg_verification_${crypto.randomBytes(8).toString("hex")}`;
    const ts = Math.floor(Date.now() / 1000);
    let res;
    try {
      res = await safePost(url, this.headers(msgId, ts, body, subId, secrets), body);
    } catch (e) {
      this.log(`events: verification POST to ${new URL(url).host} failed: ${e.reason ?? ""} ${e.detail ?? e.message}`);
      throw new RpcError(-32015, "Callback endpoint could not be reached", { reason: e instanceof DeliveryError ? e.reason : "connection_refused" });
    }
    if (res.status < 200 || res.status >= 300) {
      this.log(`events: verification POST to ${new URL(url).host} answered HTTP ${res.status}`);
      throw new RpcError(-32015, "Callback endpoint rejected verification", { reason: statusReason(res.status) });
    }
    let echoed;
    try { echoed = JSON.parse(res.text).challenge; } catch {}
    const a = Buffer.from(String(echoed ?? "")), b = Buffer.from(challenge);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
      this.log(`events: verification to ${new URL(url).host} returned HTTP ${res.status} without the challenge`);
      throw new RpcError(-32015, "Callback endpoint did not echo the challenge", { reason: "challenge_failed" });
    }
  }

  headers(id, ts, body, subId, secrets) {
    return {
      "webhook-id": id,
      "webhook-timestamp": String(ts),
      "webhook-signature": sign(secrets, id, ts, body),
      "X-MCP-Subscription-Id": subId,
    };
  }

  // --- watching T3 ---

  ensureWatcher() {
    if (this.timer) return;
    this.timer = setInterval(() => this.tick().catch((e) => this.log(`events: tick failed: ${e.message}`)), this.pollMs);
    this.timer.unref?.();
    this.tick().catch((e) => this.log(`events: tick failed: ${e.message}`));
  }

  stop() {
    clearInterval(this.timer);
    this.timer = null;
    this.releaseLock();
  }

  // Single deliverer across processes: hold state/watcher.lock (pid) while alive.
  holdLock() {
    try {
      fs.writeFileSync(this.lockFile, String(process.pid), { flag: "wx", mode: 0o600 });
      this.ownsLock = true;
      return true;
    } catch {
      const pid = Number(fs.readFileSync(this.lockFile, "utf8"));
      if (pid === process.pid) return true;
      try { process.kill(pid, 0); return false; } catch {
        fs.rmSync(this.lockFile, { force: true }); // stale
        return this.holdLock();
      }
    }
  }

  releaseLock() {
    if (!this.ownsLock) return;
    try { if (Number(fs.readFileSync(this.lockFile, "utf8")) === process.pid) fs.rmSync(this.lockFile); } catch {}
    this.ownsLock = false;
  }

  async tick() {
    const now = Date.now();
    // Expire lapsed subscriptions.
    const live = this.update((s) => {
      for (const [id, sub] of Object.entries(s.subscriptions)) if (sub.expiresAt <= now) delete s.subscriptions[id];
      return Object.values(s.subscriptions);
    });
    if (!live.length) { this.stop(); return; }
    if (!this.holdLock()) return;

    const events = [];
    for (const m of this.machines) {
      let shell;
      try { shell = await m.getShell(); } catch (e) { this.log(`events: ${m.name}: ${e.message}`); continue; }
      events.push(...this.diff(m, shell));
    }
    for (const ev of events) this.dispatch(ev, live);
  }

  // Compares each thread with its last-seen state; the first observation of a machine only records
  // a baseline. Seen states persist, so transitions during a restart are still caught.
  diff(m, shell) {
    const out = [];
    this.update((s) => {
      const firstLook = !s.seen[m.name];
      const seen = (s.seen[m.name] ??= {});
      s.seenAt ??= {};
      const lastLookAt = s.seenAt[m.name] ?? Date.now();
      const current = {};
      for (const t of shell.threads) {
        const state = threadState(t);
        const fp = `${state}|${t.latestRunId ?? ""}|${state === "waiting_on_you" ? t.pendingRuntimeRequest?.id ?? "" : ""}`;
        current[t.id] = fp;
        const prevFp = seen[t.id];
        if (firstLook || prevFp === fp || !NOTIFY_STATES.includes(state)) continue;
        const prevState = prevFp?.split("|")[0];
        // A thread we've never seen is only news if it was created after our last look (otherwise it's old history).
        if (prevFp === undefined && state !== "waiting_on_you" && !(Date.parse(t.createdAt) >= lastLookAt - 5_000)) continue;
        const project = shell.projects.find((p) => p.id === t.projectId);
        out.push({
          eventId: "evt_" + sha(`${m.name}|${t.id}|${fp}`).slice(0, 32),
          timestamp: t.pendingRuntimeRequest?.createdAt ?? t.latestRunCompletedAt ?? t.updatedAt,
          thread: t,
          project,
          data: {
            machine: m.name,
            thread_id: t.id,
            title: trunc(t.title, 200),
            project: project?.title ?? t.projectId,
            status: state,
            ...(prevState ? { previous_status: prevState } : {}),
            ...(state === "waiting_on_you" && t.pendingRuntimeRequest?.kind ? { waiting_for: t.pendingRuntimeRequest.kind } : {}),
            ...(state === "failed" && t.lastError ? { last_error: trunc(t.lastError, 240) } : {}),
            occurred_at: t.pendingRuntimeRequest?.createdAt ?? t.latestRunCompletedAt ?? t.updatedAt,
          },
        });
      }
      s.seen[m.name] = current;
      s.seenAt[m.name] = Date.now();
    });
    return out;
  }

  matches(sub, ev) {
    const a = sub.arguments ?? {};
    if (a.machine && a.machine.toLowerCase() !== ev.data.machine.toLowerCase()) return false;
    if (a.thread_id && a.thread_id !== ev.data.thread_id) return false;
    if (a.project && !(ev.project && projectMatches(ev.project, a.project))) return false;
    if (!(a.statuses ?? NOTIFY_STATES).includes(ev.data.status)) return false;
    if (!a.include_subagents && isSubagent(ev.thread)) return false;
    return true;
  }

  dispatch(ev, subs) {
    for (const sub of subs) {
      if (!sub.active || !this.matches(sub, ev)) continue;
      const key = `${sub.id}:${ev.eventId}`;
      if (this.load().sent.includes(key) || this.inflight.has(key)) continue;
      this.inflight.add(key);
      this.deliver(sub.id, ev, key).finally(() => this.inflight.delete(key));
    }
  }

  // One event per POST; same eventId on every retry, fresh timestamp + signature each attempt.
  async deliver(subId, ev, key) {
    const body = JSON.stringify({ eventId: ev.eventId, name: EVENT_NAME, timestamp: new Date(ev.timestamp).toISOString(), data: ev.data, cursor: null });
    if (Buffer.byteLength(body) > MAX_BODY_BYTES) { this.log(`events: ${ev.eventId} too large, dropped`); return; }
    for (let attempt = 0; ; attempt++) {
      const sub = this.load().subscriptions[subId];
      if (!sub || sub.expiresAt <= Date.now()) return;
      const secrets = [sub.secret];
      if (sub.previousSecret && sub.previousSecretUntil > Date.now()) secrets.push(sub.previousSecret);
      let reason, status;
      try {
        const res = await safePost(sub.url, this.headers(ev.eventId, Math.floor(Date.now() / 1000), body, subId, secrets), body);
        status = res.status;
        if (status >= 200 && status < 300) {
          this.update((s) => {
            s.sent.push(key);
            if (s.sent.length > MAX_SENT_IDS) s.sent.splice(0, s.sent.length - MAX_SENT_IDS);
            if (s.subscriptions[subId]) s.subscriptions[subId].lastError = null;
          });
          this.log(`events: delivered ${ev.eventId} (${ev.data.status}) to ${subId}`);
          return;
        }
        reason = statusReason(status);
      } catch (e) {
        reason = e instanceof DeliveryError ? e.reason : "connection_refused";
        this.log(`events: delivery attempt ${attempt + 1} for ${ev.eventId} failed: ${reason} ${e.detail ?? e.message}`);
      }
      if (status === 410) {
        this.update((s) => { delete s.subscriptions[subId]; });
        this.log(`events: ${subId} answered 410; subscription removed`);
        return;
      }
      const retryable = status !== 413 && !(status >= 400 && status < 500 && status !== 408 && status !== 429);
      if (!retryable || attempt >= RETRY_DELAYS_MS.length) {
        this.update((s) => {
          const sub2 = s.subscriptions[subId];
          if (sub2) { sub2.lastError = reason; if (retryable) sub2.active = false; }
        });
        this.log(`events: giving up on ${ev.eventId} for ${subId} (${reason})`);
        return;
      }
      await new Promise((r) => setTimeout(r, RETRY_DELAYS_MS[attempt] * (this.retryScale ?? 1)));
    }
  }

  // Resume watching at startup if subscriptions survived a restart.
  resume() {
    if (Object.keys(this.load().subscriptions).length) this.ensureWatcher();
  }
}
