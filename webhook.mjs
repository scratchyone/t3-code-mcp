// Standard Webhooks signing + SSRF-hardened HTTPS POST for MCP Events webhook delivery.
//
// Signature: "v1," + base64(HMAC-SHA256(secretBytes, `${id}.${timestamp}.${body}`)), where secretBytes is
// the base64 after "whsec_". Several space-separated signatures are sent during secret rotation.
// Delivery only goes to https URLs whose resolved addresses are all public; the connection is pinned
// to the validated address (no re-resolution), TLS keeps the original hostname, redirects aren't
// followed. T3_EVENTS_TEST_ALLOW_LOOPBACK=1 (tests only) also permits http://127.0.0.1.

import crypto from "node:crypto";
import dns from "node:dns/promises";
import http from "node:http";
import https from "node:https";
import net from "node:net";

export const MAX_BODY_BYTES = 262_144;

// Returns the secret's key bytes, or null if it isn't a valid whsec_ value (24–64 bytes).
export function parseSecret(secret) {
  if (typeof secret !== "string" || !secret.startsWith("whsec_")) return null;
  const b64 = secret.slice(6);
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(b64)) return null;
  const key = Buffer.from(b64, "base64");
  return key.length >= 24 && key.length <= 64 ? key : null;
}

export function sign(secrets, id, timestamp, body) {
  return secrets
    .map((s) => "v1," + crypto.createHmac("sha256", parseSecret(s)).update(`${id}.${timestamp}.${body}`).digest("base64"))
    .join(" ");
}

function isPublicAddress(address) {
  if (net.isIPv4(address)) {
    const [a, b] = address.split(".").map(Number);
    return !(
      a === 0 || a === 10 || a === 127 || a >= 224 ||
      (a === 100 && b >= 64 && b <= 127) ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) ||
      (a === 192 && b === 0) ||
      (a === 198 && (b === 18 || b === 19))
    );
  }
  const v6 = address.toLowerCase();
  const mapped = v6.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/);
  if (mapped) return isPublicAddress(mapped[1]);
  return !(v6 === "::" || v6 === "::1" || /^f[cd]/.test(v6) || /^fe[89ab]/.test(v6) || v6.startsWith("ff") || v6.startsWith("2001:db8"));
}

const testLoopback = () => process.env.T3_EVENTS_TEST_ALLOW_LOOPBACK === "1";

export class DeliveryError extends Error {
  constructor(reason, detail) {
    super(reason);
    this.reason = reason; // one of connection_refused, timeout, tls_error, http_4xx, http_5xx, challenge_failed
    this.detail = detail; // for our logs only (never returned to callers)
  }
}

// POSTs body (a string, sent byte-for-byte) and resolves {status, text}. Throws DeliveryError for
// network failures; HTTP error statuses resolve normally so callers can decide about retries.
export async function safePost(rawUrl, headers, body, timeoutMs = 10_000) {
  let url;
  try { url = new URL(rawUrl); } catch { throw new DeliveryError("connection_refused", "invalid URL"); }
  const loopback = testLoopback() && url.protocol === "http:" && url.hostname === "127.0.0.1";
  if (url.protocol !== "https:" && !loopback) throw new DeliveryError("connection_refused", `scheme ${url.protocol}`);
  if (url.username || url.password) throw new DeliveryError("connection_refused", "credentials in URL");

  let address, family;
  if (loopback) {
    [address, family] = ["127.0.0.1", 4];
  } else {
    let records;
    try { records = await dns.lookup(url.hostname, { all: true, verbatim: true }); } catch (e) { throw new DeliveryError("connection_refused", `DNS ${e.code}`); }
    const blocked = records.filter((r) => !isPublicAddress(r.address)).map((r) => r.address);
    if (!records.length || blocked.length) throw new DeliveryError("connection_refused", `non-public address ${blocked.join(",") || "none"}`);
    // Prefer IPv4: this Mac has no IPv6 route.
    ({ address, family } = records.find((r) => r.family === 4) ?? records[0]);
  }

  const mod = url.protocol === "https:" ? https : http;
  return new Promise((resolve, reject) => {
    const req = mod.request(
      url,
      {
        method: "POST",
        headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body), ...headers },
        // Pin to the validated address. Node may ask for a list (opts.all) or a single address.
        lookup: (_host, opts, cb) => (opts?.all ? cb(null, [{ address, family }]) : cb(null, address, family)),
        servername: url.hostname,
        timeout: timeoutMs,
      },
      (res) => {
        const chunks = [];
        let size = 0;
        res.on("data", (c) => { size += c.length; if (size <= 65_536) chunks.push(c); });
        res.on("end", () => resolve({ status: res.statusCode, text: Buffer.concat(chunks).toString("utf8") }));
      },
    );
    req.on("timeout", () => req.destroy(new DeliveryError("timeout", `no response in ${timeoutMs} ms from ${address}`)));
    req.on("error", (e) => reject(e instanceof DeliveryError ? e : new DeliveryError(/certificate|tls|ssl/i.test(e.message) ? "tls_error" : "connection_refused", `${e.code ?? ""} ${e.message} (${address})`)));
    req.end(body);
  });
}

export const statusReason = (status) => (status >= 500 ? "http_5xx" : "http_4xx");
