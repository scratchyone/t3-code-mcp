// Client for one T3 Code server ("machine"): plain HTTP for reads, the app's own WebSocket RPC
// (Effect RPC, JSON frames) for search/launch/dispatch, and a self-renewing paired session.
//
// Auth: the MCP is its own paired T3 client with only orchestration:read + orchestration:operate.
// It renews itself before the 30-day session expires by running T3's CLI (`auth pairing create`
// → exchange at /oauth/token → revoke our older sessions). The CLI talks to T3's local state
// directly, so renewing needs no extra scope; for another machine the CLI runs over ssh.
// The token lives in the macOS login keychain (service "t3-mcp-token", account = machine name), or on
// other systems in state/tokens/<machine>.json (mode 0600).
//
// Wire details were taken from T3 0.0.43-preview.20260925.2240 (orchestration protocol 2).
// Responses are read leniently (plain JSON, unknown fields ignored) so additive T3 changes don't break us.

import { execFile } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

const PROTOCOL = 2;
const SCOPES = "orchestration:read orchestration:operate";
const RENEW_BEFORE_MS = 7 * 24 * 3600 * 1000;
const KEYCHAIN_SERVICE = "t3-mcp-token";
const USE_KEYCHAIN = process.platform === "darwin" && !process.env.T3_MCP_FILE_TOKENS;
const TOKEN_DIR = path.join(process.env.T3_MCP_STATE_DIR || new URL("./state", import.meta.url).pathname, "tokens");

function run(cmd, args, { env, timeout = 90_000 } = {}) {
  return new Promise((resolve, reject) => {
    execFile(cmd, args, { timeout, maxBuffer: 16 << 20, env: env ? { ...process.env, ...env } : process.env }, (err, stdout, stderr) => {
      if (err) {
        err.message = `${path.basename(cmd)} failed: ${(stderr || err.message).trim().slice(0, 400)}`;
        return reject(err);
      }
      resolve(stdout);
    });
  });
}

const shq = (s) => `'${String(s).replace(/'/g, `'\\''`)}'`;

// Effect RPC failures arrive as a serialized Cause; dig out the first human-readable message.
function causeMessage(cause) {
  const seen = new Set();
  const walk = (o) => {
    if (!o || typeof o !== "object" || seen.has(o)) return null;
    seen.add(o);
    if (typeof o.message === "string" && o.message) return o.message;
    for (const v of Object.values(o)) {
      const m = walk(v);
      if (m) return m;
    }
    return null;
  };
  return walk(cause) || JSON.stringify(cause).slice(0, 400);
}

export class Machine {
  /**
   * @param {{name: string, baseUrl: string, cli: string[], cliEnv?: object}} cfg
   *   cli: command prefix for T3's CLI, e.g. ["/Applications/…/T3 Code (Alpha)", "…/bin.mjs"] with
   *   cliEnv {ELECTRON_RUN_AS_NODE: "1"}, or ["ssh", "laptop", "t3"] for another machine.
   */
  constructor(cfg, { label, log }) {
    this.name = cfg.name;
    this.baseUrl = cfg.baseUrl.replace(/\/$/, "");
    this.cli = cfg.cli;
    this.cliEnv = cfg.cliEnv;
    this.label = label;
    this.log = log;
    this.token = null;
    this.expiresAt = 0;
    this.renewing = null;
    this.envCheckedAt = 0;
    this.environment = null;
    this.shell = null;
    this.shellAt = 0;
  }

  // --- auth ---

  async t3cli(args) {
    const [cmd, ...pre] = this.cli;
    const remote = path.basename(cmd) === "ssh";
    return run(cmd, [...pre, ...(remote ? args.map(shq) : args)], { env: this.cliEnv });
  }

  async loadToken() {
    try {
      const raw = USE_KEYCHAIN
        ? await run("security", ["find-generic-password", "-s", KEYCHAIN_SERVICE, "-a", this.name, "-w"])
        : fs.readFileSync(this.tokenFile(), "utf8");
      const saved = JSON.parse(raw.trim());
      this.token = saved.token;
      this.expiresAt = Date.parse(saved.expiresAt) || 0;
    } catch {
      // No saved token yet (or keychain locked); renew() will make one.
    }
  }

  async saveToken() {
    const value = JSON.stringify({ token: this.token, expiresAt: new Date(this.expiresAt).toISOString() });
    if (USE_KEYCHAIN) return void (await run("security", ["add-generic-password", "-U", "-s", KEYCHAIN_SERVICE, "-a", this.name, "-w", value]));
    fs.mkdirSync(TOKEN_DIR, { recursive: true, mode: 0o700 });
    const tmp = `${this.tokenFile()}.tmp`;
    fs.writeFileSync(tmp, value, { mode: 0o600 });
    fs.renameSync(tmp, this.tokenFile());
  }

  tokenFile() {
    return path.join(TOKEN_DIR, `${this.name.replace(/[^\w.-]/g, "_")}.json`);
  }

  renew(reason) {
    this.renewing ??= (async () => {
      this.log(`${this.name}: renewing T3 session (${reason})`);
      const pairing = JSON.parse(await this.t3cli(["auth", "pairing", "create", "--ttl", "10m", "--label", this.label, "--json"]));
      const body = new URLSearchParams({
        grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
        subject_token: pairing.credential,
        subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
        requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
        scope: SCOPES,
        client_label: this.label,
        client_device_type: "bot",
      });
      const res = await fetch(`${this.baseUrl}/oauth/token`, { method: "POST", body, signal: AbortSignal.timeout(20_000) });
      if (!res.ok) throw new Error(`${this.name}: T3 token exchange failed (HTTP ${res.status})`);
      const issued = await res.json();
      this.token = issued.access_token;
      this.expiresAt = Date.now() + issued.expires_in * 1000;
      await this.saveToken();
      await this.revokeOlderSessions().catch((e) => this.log(`${this.name}: couldn't revoke old sessions: ${e.message}`));
      this.log(`${this.name}: session renewed until ${new Date(this.expiresAt).toISOString()}`);
    })().finally(() => {
      this.renewing = null;
    });
    return this.renewing;
  }

  // Keep only our newest session; older ones are from previous renewals.
  async revokeOlderSessions() {
    const sessions = JSON.parse(await this.t3cli(["auth", "session", "list", "--json"]));
    const ours = sessions
      .filter((s) => s.client?.label === this.label)
      .sort((a, b) => Date.parse(b.issuedAt) - Date.parse(a.issuedAt));
    for (const s of ours.slice(1)) await this.t3cli(["auth", "session", "revoke", s.sessionId]);
  }

  async ensureToken() {
    const fresh = () => this.token && this.expiresAt - Date.now() > RENEW_BEFORE_MS;
    if (fresh()) return this.token;
    await this.loadToken(); // another process may have renewed already
    if (!fresh()) await this.renew(this.token ? "expires within 7 days" : "no saved session");
    return this.token;
  }

  // Runs fn(token); on a 401 reloads/renews once and retries.
  async withToken(fn) {
    const token = await this.ensureToken();
    const res = await fn(token);
    if (res.status !== 401) return res;
    await this.loadToken();
    if (this.token === token) await this.renew("session rejected");
    return fn(this.token);
  }

  // --- transport ---

  async checkEnvironment() {
    if (Date.now() - this.envCheckedAt < 5 * 60_000) return this.environment;
    const res = await fetch(`${this.baseUrl}/.well-known/t3/environment`, { signal: AbortSignal.timeout(10_000) });
    if (!res.ok) throw new Error(`${this.name}: T3 isn't answering (HTTP ${res.status})`);
    const env = await res.json();
    const proto = env.orchestrationProtocolVersion ?? 1;
    if (proto !== PROTOCOL) {
      throw new Error(
        `${this.name}: T3 ${env.serverVersion} speaks orchestration protocol ${proto}, this MCP needs ${PROTOCOL}. The T3 MCP needs an update for this T3 version.`,
      );
    }
    this.environment = env;
    this.envCheckedAt = Date.now();
    return env;
  }

  async getJson(urlPath) {
    await this.checkEnvironment();
    const res = await this.withToken((token) =>
      fetch(this.baseUrl + urlPath, {
        headers: { Authorization: `Bearer ${token}`, "x-t3-orchestration-protocol": String(PROTOCOL) },
        signal: AbortSignal.timeout(30_000),
      }),
    );
    if (!res.ok) {
      const detail = (await res.text().catch(() => "")).slice(0, 300);
      const err = new Error(`${this.name}: T3 returned HTTP ${res.status} for ${urlPath}${detail ? `: ${detail}` : ""}`);
      err.status = res.status;
      throw err;
    }
    return res.json();
  }

  // One short-lived WebSocket per call: ticket → /ws → single Request → Exit.
  async rpc(tag, payload, timeoutMs = 60_000) {
    await this.checkEnvironment();
    const res = await this.withToken((token) =>
      fetch(`${this.baseUrl}/api/auth/websocket-ticket`, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}` },
        signal: AbortSignal.timeout(15_000),
      }),
    );
    if (!res.ok) throw new Error(`${this.name}: couldn't get a WebSocket ticket (HTTP ${res.status})`);
    const { ticket } = await res.json();
    const url = new URL("/ws", this.baseUrl);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.searchParams.set("wsTicket", ticket);
    url.searchParams.set("orchestrationProtocol", String(PROTOCOL));
    url.searchParams.set("clientDeviceType", "bot");

    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url);
      let settled = false;
      const finish = (fn, value) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        try { ws.close(); } catch {}
        fn(value);
      };
      const timer = setTimeout(() => finish(reject, new Error(`${this.name}: ${tag} timed out`)), timeoutMs);
      ws.onopen = () => ws.send(JSON.stringify({ _tag: "Request", id: "1", tag, payload, headers: [] }));
      ws.onmessage = (event) => {
        let msg;
        try { msg = JSON.parse(String(event.data)); } catch { return; }
        if (msg._tag === "Ping") return ws.send(JSON.stringify({ _tag: "Pong" }));
        if (msg._tag !== "Exit" || msg.requestId !== "1") return;
        if (msg.exit?._tag === "Success") finish(resolve, msg.exit.value);
        else finish(reject, new Error(`${this.name}: ${causeMessage(msg.exit?.cause ?? msg.exit)}`));
      };
      ws.onerror = () => finish(reject, new Error(`${this.name}: WebSocket error during ${tag}`));
      ws.onclose = (event) => finish(reject, new Error(`${this.name}: WebSocket closed during ${tag} (${event.code})`));
    });
  }

  // --- data ---

  async getShell() {
    if (this.shell && Date.now() - this.shellAt < 10_000) return this.shell;
    this.shell = await this.getJson("/api/orchestration/shell");
    this.shellAt = Date.now();
    return this.shell;
  }

  invalidateShell() {
    this.shell = null;
  }

  getThread(threadId) {
    return this.getJson(`/api/orchestration/threads/${encodeURIComponent(threadId)}/bounded`);
  }

  async getSettings() {
    if (this.settings && Date.now() - this.settingsAt < 5 * 60_000) return this.settings;
    this.settings = await this.rpc("server.getSettings", {});
    this.settingsAt = Date.now();
    return this.settings;
  }
}
