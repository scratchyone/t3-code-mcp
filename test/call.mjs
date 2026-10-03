// Live helper: node test/call.mjs '<tool>' '<json args>' [...more pairs]. Prints each result.
import { spawn } from "node:child_process";
import readline from "node:readline";

const pairs = [];
for (let i = 2; i < process.argv.length; i += 2) pairs.push([process.argv[i], JSON.parse(process.argv[i + 1] || "{}")]);
const child = spawn(new URL("../run-mcp.sh", import.meta.url).pathname, { stdio: ["pipe", "pipe", "inherit"] });
const send = (m) => child.stdin.write(JSON.stringify(m) + "\n");
const pending = new Set(pairs.map((_, i) => i + 2).concat([1]));
const timer = setTimeout(() => { console.error("timed out"); child.kill(); process.exit(1); }, 300_000);
readline.createInterface({ input: child.stdout }).on("line", (l) => {
  const m = JSON.parse(l);
  if (m.id === 1) console.log("init:", m.result?.serverInfo?.name);
  else console.log(`--- ${pairs[m.id - 2][0]}${m.result?.isError ? " (error)" : ""}\n${m.result?.content?.[0]?.text ?? JSON.stringify(m.error)}`);
  pending.delete(m.id);
  if (!pending.size) { clearTimeout(timer); child.kill(); }
});
send({ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "call", version: "1" } } });
send({ jsonrpc: "2.0", method: "notifications/initialized" });
pairs.forEach(([name, args], i) => send({ jsonrpc: "2.0", id: i + 2, method: "tools/call", params: { name, arguments: args } }));
