// Forces one session renewal for every machine and lists the remaining "ChatGPT MCP" sessions.
import fs from "node:fs";
import { Machine } from "../t3client.mjs";
const config = JSON.parse(fs.readFileSync(new URL("../config.json", import.meta.url), "utf8"));
for (const cfg of config.machines) {
  const m = new Machine(cfg, { label: config.sessionLabel, log: console.log });
  await m.renew("manual test");
  const sessions = JSON.parse(await m.t3cli(["auth", "session", "list", "--json"]));
  console.log(cfg.name, "sessions labelled", config.sessionLabel, "=", sessions.filter((s) => s.client?.label === config.sessionLabel).map((s) => s.expiresAt));
  const shell = await m.getShell();
  console.log(cfg.name, "shell OK with new token:", shell.threads.length, "threads");
}
