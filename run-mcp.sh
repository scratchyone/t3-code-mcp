#!/bin/bash
# The stdio MCP server tunnel-client launches. Sessions/tokens are managed by the server itself.
DIR="$(cd "$(dirname "$0")" && pwd)"
exec "${NODE:-node}" "$DIR/server.mjs"
