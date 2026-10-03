// MCP Events tests: `node --test test/`. Uses fake T3 machines and a local webhook receiver that
// verifies Standard Webhooks signatures with its own HMAC code (not webhook.mjs's).
process.env.T3_EVENTS_TEST_ALLOW_LOOPBACK = "1";

import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import readline from "node:readline";
import { EventHub, EVENT_NAME, RpcError } from "../events.mjs";
import { safePost } from "../webhook.mjs";

const newSecret = () => "whsec_" + crypto.randomBytes(32).toString("base64");
const tmpDir = () => fs.mkdtempSync(path.join(os.tmpdir(), "t3-events-"));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function verifySig(secret, headers, raw) {
  const key = Buffer.from(secret.slice(6), "base64");
  const expected = crypto.createHmac("sha256", key).update(`${headers["webhook-id"]}.${headers["webhook-timestamp"]}.${raw}`).digest("base64");
  return headers["webhook-signature"].split(" ").some((s) => s === `v1,${expected}`);
}

// Receiver: echoes verification challenges, records deliveries, can be told to fail N times or answer a fixed status.
async function receiver(getSecrets) {
  const r = { deliveries: [], verifications: 0, failNext: 0, fixedStatus: null, badSig: 0, echoWrong: false };
  r.server = http.createServer((req, res) => {
    let raw = "";
    req.on("data", (c) => (raw += c));
    req.on("end", () => {
      const ok = getSecrets().some((s) => verifySig(s, req.headers, raw));
      if (!ok) { r.badSig++; res.writeHead(401).end(); return; }
      const body = JSON.parse(raw);
      if (body.type === "verification") {
        r.verifications++;
        res.writeHead(200, { "Content-Type": "application/json" }).end(JSON.stringify({ challenge: r.echoWrong ? "nope" : body.challenge }));
        return;
      }
      if (r.fixedStatus) { res.writeHead(r.fixedStatus).end(); return; }
      if (r.failNext > 0) { r.failNext--; r.deliveries.push({ failed: true, headers: req.headers, body }); res.writeHead(503).end(); return; }
      r.deliveries.push({ headers: req.headers, body, subId: req.headers["x-mcp-subscription-id"] });
      res.writeHead(204).end();
    });
  });
  await new Promise((ok) => r.server.listen(0, "127.0.0.1", ok));
  r.url = `http://127.0.0.1:${r.server.address().port}/hook`;
  return r;
}

function fakeMachine(name, shell) {
  return { name, shell, getShell: async () => structuredClone(shell) };
}

const project = { id: "p1", title: "website", workspaceRoot: "/home/me/website" };
const thread = (id, status, extra = {}) => ({
  id, projectId: "p1", title: `Thread ${id}`, status, activeRunId: status === "running" ? "r1" : null,
  latestRunId: "r1", pendingRuntimeRequest: null, lineage: { relationshipToParent: null }, updatedAt: "2026-10-02T20:00:00Z", ...extra,
});

function makeHub(machines) {
  const hub = new EventHub({ machines, stateDir: tmpDir(), log: () => {}, pollMs: 60_000 });
  hub.retryScale = 0.001;
  return hub;
}

test("events/list describes the event with filters and payload", () => {
  const hub = makeHub([fakeMachine("desktop", { threads: [], projects: [] })]);
  const { events } = hub.list();
  assert.equal(events.length, 1);
  assert.equal(events[0].name, EVENT_NAME);
  assert.deepEqual(events[0].delivery, ["webhook"]);
  assert.deepEqual(Object.keys(events[0].inputSchema.properties).sort(), ["include_subagents", "machine", "project", "statuses", "thread_id"]);
  assert.equal(events[0].inputSchema.additionalProperties, false);
  hub.stop();
});

test("subscribe validates secret, url, arguments and event name", async () => {
  const hub = makeHub([fakeMachine("desktop", { threads: [], projects: [] })]);
  const base = { name: EVENT_NAME, arguments: {}, delivery: { mode: "webhook", url: "https://example.com/h", secret: newSecret() } };
  const code = async (p) => { try { await hub.subscribe(p); return "ok"; } catch (e) { assert.ok(e instanceof RpcError); return e.code; } };
  assert.equal(await code({ ...base, delivery: { ...base.delivery, secret: "whsec_c2hvcnQ=" } }), -32602);
  assert.equal(await code({ ...base, delivery: { ...base.delivery, secret: "nope" } }), -32602);
  assert.equal(await code({ ...base, delivery: { ...base.delivery, url: "http://example.com/h" } }), -32602);
  assert.equal(await code({ ...base, arguments: { bogus: 1 } }), -32602);
  assert.equal(await code({ ...base, arguments: { machine: "laptop" } }), -32602);
  assert.equal(await code({ ...base, arguments: { statuses: ["running"] } }), -32602);
  assert.equal(await code({ ...base, name: "other.event" }), -32011);
  assert.equal(await code({ ...base, delivery: { mode: "push" } }), -32014);
  hub.stop();
});

test("verification failure → -32015 challenge_failed; private addresses refused", async () => {
  const secret = newSecret();
  const r = await receiver(() => [secret]);
  r.echoWrong = true;
  const hub = makeHub([fakeMachine("desktop", { threads: [], projects: [] })]);
  await assert.rejects(
    hub.subscribe({ name: EVENT_NAME, arguments: {}, delivery: { mode: "webhook", url: r.url, secret } }),
    (e) => e.code === -32015 && e.data.reason === "challenge_failed",
  );
  await assert.rejects(safePost("https://127.0.0.1:9/x", {}, "{}"), (e) => e.reason === "connection_refused");
  await assert.rejects(safePost("https://10.0.0.1/x", {}, "{}"), (e) => e.reason === "connection_refused");
  await assert.rejects(safePost("https://localhost/x", {}, "{}"), (e) => e.reason === "connection_refused");
  hub.stop();
  r.server.close();
});

test("subscribe is idempotent, persists 0600, refresh rotates secret; unsubscribe removes", async () => {
  let secrets = [newSecret()];
  const r = await receiver(() => secrets);
  const hub = makeHub([fakeMachine("desktop", { threads: [], projects: [project] })]);
  const params = { name: EVENT_NAME, arguments: { project: "website", statuses: ["failed"] }, delivery: { mode: "webhook", url: r.url, secret: secrets[0] }, ttlMs: 3600_000 };
  const a = await hub.subscribe(params);
  const b = await hub.subscribe({ ...params, arguments: { statuses: ["failed"], project: "website" } }); // same args, other key order
  assert.equal(a.id, b.id);
  assert.equal(a.cursor, null);
  assert.equal(r.verifications, 1, "verification cached per url");
  assert.ok(Date.parse(a.refreshBefore) - Date.now() <= 3600_000);
  const mode = fs.statSync(hub.file).mode & 0o777;
  assert.equal(mode, 0o600);
  secrets = [newSecret(), secrets[0]];
  await hub.subscribe({ ...params, delivery: { ...params.delivery, secret: secrets[0] } });
  const sub = hub.load().subscriptions[a.id];
  assert.equal(sub.secret, secrets[0]);
  assert.equal(sub.previousSecret, secrets[1]);
  assert.deepEqual(hub.unsubscribe({ name: EVENT_NAME, arguments: params.arguments, delivery: { mode: "webhook", url: r.url } }), {});
  assert.equal(Object.keys(hub.load().subscriptions).length, 0);
  assert.deepEqual(hub.unsubscribe({ name: EVENT_NAME, arguments: params.arguments, delivery: { mode: "webhook", url: r.url } }), {}, "idempotent");
  hub.stop();
  r.server.close();
});

test("status transitions deliver signed, filtered, deduplicated events", async () => {
  const secret = newSecret();
  const r = await receiver(() => [secret]);
  const shell = { projects: [project, { id: "p2", title: "Generic", workspaceRoot: "/x/Generic" }], threads: [
    thread("a", "running"), thread("b", "running"), thread("c", "running", { projectId: "p2" }),
    thread("s", "running", { lineage: { relationshipToParent: "subagent", parentThreadId: "a" } }),
  ] };
  const m = fakeMachine("desktop", shell);
  const hub = makeHub([m]);
  await hub.subscribe({ name: EVENT_NAME, arguments: { project: "website", statuses: ["completed", "waiting_on_you"] }, delivery: { mode: "webhook", url: r.url, secret } });
  await hub.tick(); // baseline
  assert.equal(r.deliveries.length, 0);

  m.shell.threads = [
    thread("a", "completed", { latestRunCompletedAt: "2026-10-02T20:05:00Z" }),
    thread("b", "failed", { lastError: "boom" }), // filtered out by statuses
    thread("c", "completed", { projectId: "p2" }), // other project
    thread("s", "completed", { lineage: { relationshipToParent: "subagent", parentThreadId: "a" } }), // subagent
  ];
  await hub.tick();
  await sleep(100);
  assert.equal(r.deliveries.length, 1);
  const d = r.deliveries[0];
  assert.equal(d.body.name, EVENT_NAME);
  assert.equal(d.body.eventId, d.headers["webhook-id"]);
  assert.equal(d.body.cursor, null);
  assert.deepEqual(d.body.data, { machine: "desktop", thread_id: "a", title: "Thread a", project: "website", status: "completed", previous_status: "running", occurred_at: "2026-10-02T20:05:00Z" });
  assert.ok(Math.abs(Number(d.headers["webhook-timestamp"]) - Date.now() / 1000) < 10);

  // A brand-new thread that finished between two looks still counts.
  m.shell.threads.push(thread("n", "completed", { createdAt: new Date().toISOString() }));
  // ...but an old thread appearing for the first time (e.g. unarchived) doesn't.
  m.shell.threads.push(thread("o", "completed", { createdAt: "2020-01-01T00:00:00Z" }));
  await hub.tick();
  await sleep(100);
  assert.equal(r.deliveries.length, 2);
  assert.equal(r.deliveries[1].body.data.thread_id, "n");
  assert.equal(r.deliveries[1].body.data.previous_status, undefined);
  m.shell.threads = m.shell.threads.filter((t) => t.id !== "n" && t.id !== "o");
  r.deliveries.pop();

  await hub.tick(); // nothing changed → nothing new
  await sleep(50);
  assert.equal(r.deliveries.length, 1);

  // Waiting on the user.
  m.shell.threads[0] = thread("a", "running", { latestRunId: "r2", activeRunId: "r2", pendingRuntimeRequest: { id: "req1", kind: "approval", createdAt: "2026-10-02T20:10:00Z" } });
  await hub.tick();
  await sleep(100);
  assert.equal(r.deliveries.length, 2);
  assert.equal(r.deliveries[1].body.data.status, "waiting_on_you");
  assert.equal(r.deliveries[1].body.data.waiting_for, "approval");
  assert.equal(r.badSig, 0);
  hub.stop();
  r.server.close();
});

test("retries keep eventId with fresh signatures; 410 removes the subscription", async () => {
  const secret = newSecret();
  const r = await receiver(() => [secret]);
  const m = fakeMachine("desktop", { projects: [project], threads: [thread("a", "running")] });
  const hub = makeHub([m]);
  const { id } = await hub.subscribe({ name: EVENT_NAME, arguments: {}, delivery: { mode: "webhook", url: r.url, secret } });
  await hub.tick();
  r.failNext = 2;
  m.shell.threads = [thread("a", "failed", { lastError: "x".repeat(500) })];
  await hub.tick();
  for (let i = 0; i < 50 && r.deliveries.filter((d) => !d.failed).length < 1; i++) await sleep(20);
  const ids = new Set(r.deliveries.map((d) => d.body.eventId));
  assert.equal(r.deliveries.length, 3);
  assert.equal(ids.size, 1, "same eventId on every attempt");
  assert.equal(r.deliveries.at(-1).body.data.last_error.length, 241, "error truncated");

  r.fixedStatus = 410;
  m.shell.threads = [thread("a", "running", { latestRunId: "r2", activeRunId: "r2" })];
  await hub.tick();
  m.shell.threads = [thread("a", "completed", { latestRunId: "r2" })];
  await hub.tick();
  await sleep(100);
  assert.equal(hub.load().subscriptions[id], undefined);
  hub.stop();
  r.server.close();
});

test("server: discover + events only when enabled; legacy initialize and tools unchanged", async () => {
  const dir = tmpDir();
  const cfg = path.join(dir, "config.json");
  const rpc = async (enabled, msgs) => {
    fs.writeFileSync(cfg, JSON.stringify({ machines: [{ name: "desktop", baseUrl: "http://127.0.0.1:9", cli: ["false"] }], events: { enabled } }));
    const child = spawn(process.execPath, [new URL("../server.mjs", import.meta.url).pathname], {
      env: { ...process.env, T3_MCP_CONFIG: cfg, T3_MCP_STATE_DIR: path.join(dir, "state"), T3_MCP_NO_RENEW: "1" },
      stdio: ["pipe", "pipe", "ignore"],
    });
    const replies = new Map();
    readline.createInterface({ input: child.stdout }).on("line", (l) => { const m = JSON.parse(l); replies.set(m.id, m); });
    for (const m of msgs) child.stdin.write(JSON.stringify({ jsonrpc: "2.0", ...m }) + "\n");
    for (let i = 0; i < 100 && replies.size < msgs.length; i++) await sleep(20);
    child.kill();
    return replies;
  };
  const meta = (v) => ({ _meta: { "io.modelcontextprotocol/protocolVersion": v, "io.modelcontextprotocol/clientCapabilities": {} } });
  const msgs = [
    { id: 1, method: "server/discover", params: meta("2026-07-28") },
    { id: 2, method: "events/list", params: meta("2026-07-28") },
    { id: 3, method: "tools/list", params: meta("2026-07-28") },
    { id: 4, method: "initialize", params: { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "t", version: "1" } } },
    { id: 5, method: "tools/list" },
    { id: 6, method: "tools/list", params: meta("1900-01-01") },
  ];
  const on = await rpc(true, msgs);
  assert.deepEqual(on.get(1).result.supportedVersions, ["2026-07-28"]);
  assert.deepEqual(on.get(1).result.capabilities.events, {});
  assert.equal(on.get(1).result.resultType, "complete");
  assert.equal(on.get(2).result.events[0].name, EVENT_NAME);
  assert.equal(on.get(3).result.tools.length, 8);
  assert.equal(on.get(4).result.capabilities.events, undefined);
  assert.equal(on.get(5).result.tools.length, 8);
  assert.equal(on.get(5).result.resultType, undefined);
  assert.equal(on.get(6).error.code, -32022);

  const off = await rpc(false, msgs);
  assert.equal(off.get(1).error.code, -32601, "discover stays unanswered when disabled (today's behaviour)");
  assert.equal(off.get(2).error.code, -32601);
  assert.equal(off.get(3).result.tools.length, 8);
  assert.equal(off.get(3).result.resultType, undefined);
});
