# t3-code-mcp

An [MCP](https://modelcontextprotocol.io/) server that connects ChatGPT to T3 Code. T3 Code is an app for running coding agents like Claude and Codex, where work happens in threads that are grouped by project.

I run T3 on more than one machine, and I wanted ChatGPT to be able to check on my threads, start new ones, send them messages, and hear the moment one finishes. This server gives it that.

## What ChatGPT can do

Once connected, ChatGPT gets six tools:

- `list_projects` - see all of your projects
- `list_threads` - list recent threads
- `search_threads` - find threads by keyword
- `read_thread` - read a thread's status, any approvals or questions waiting on you, and its most recent messages. Long messages are delivered in chunks, with a note when there's more to fetch.
- `create_thread` - start a new thread with a prompt
- `send_message` - send a follow-up message to an existing thread

A single instance of this server can talk to every T3 Code machine in its config. Every result tells you which machine it came from, and if one machine is offline the others continue to respond.

## How it works

This project has no dependencies. It needs Node 22 or newer.

It connects to your existing T3 Code installation using the same undocumented endpoints that T3 itself uses - there is no official public API, so a T3 update could break things. If that happens, the server will fail with a clear error message.

It authenticates by logging into T3 as a new session called "ChatGPT MCP", with permissions limited to reading threads and running them. You can revoke access at any time from T3's sessions list.

T3 sessions expire after 30 days. This server renews its own session before that happens - once fewer than 7 days remain, or immediately if T3 rejects the current session - using the T3 command-line tool, which has full access to T3 on that machine.

The server connects to ChatGPT through OpenAI's Secure MCP Tunnel. ChatGPT is instructed to only use these tools when you explicitly ask it to, and to ignore any instructions it finds inside content it reads - though prompt injection remains a real risk, so treat `create_thread` and `send_message` with care since they can cause a coding agent to execute code on your machines.

Threads created by ChatGPT are titled `[ChatGPT] ...` and every message it sends starts with `[From ChatGPT]`, so you can spot them in the T3 interface.

## Setup

1. Copy `config.example.json` to `config.json` and list your T3 machines.
2. Run `node test/call.mjs list_threads '{"limit":3}'` to check that it can talk to T3.
3. In the OpenAI Platform, go to Organization settings → Tunnels and create a tunnel.
4. Separately, go to Organization settings → API keys and create a restricted API key with the Tunnels permissions Read and Use - this is the runtime key.
5. Install OpenAI's tunnel-client. On macOS: `brew install openai/tools/tunnel-client`.
6. Set the runtime key as the `CONTROL_PLANE_API_KEY` environment variable.
7. Run `./set-tunnel tunnel_<id>` to configure the server to use your tunnel.
8. Run `tunnel/run.sh`. This starts OpenAI's tunnel-client, which launches the server.
9. In ChatGPT, add a connector. Enter your tunnel ID (`tunnel_<id>`) in the text field. The connector uses no authentication - the tunnel is private to your workspace.

## Notifications

By default, ChatGPT only knows about your threads when you ask it to check. Enabling notifications lets ChatGPT hear the moment a thread finishes, fails, or needs your attention.

I think these are essential if you want ChatGPT to reliably keep an eye on your T3 sessions, and strongly recommend enabling them. They're off by default only because enabling them bumps the connector up to a newer version of the MCP protocol.

To enable, set this in `config.json` and restart:

```json
{
  "events": {
    "enabled": true
  }
}
```

## License

MIT licensed. This project is not affiliated with T3 Code or OpenAI.
