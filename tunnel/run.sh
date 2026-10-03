#!/bin/bash
# Starts OpenAI's tunnel-client for the T3 MCP. It launches ../run-mcp.sh over stdio;
# there's no proxy to wait for. The runtime key comes from CONTROL_PLANE_API_KEY, or else from the
# macOS login keychain (service "openai-tunnel-runtime-key", account $TUNNEL_KEY_ACCOUNT, default "t3-code-mcp").
set -uo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
READYZ=http://127.0.0.1:23442/readyz
log() { echo "$(date -u +%FT%TZ) run.sh: $*"; }

if [ -z "${CONTROL_PLANE_API_KEY:-}" ]; then
  CONTROL_PLANE_API_KEY="$(security find-generic-password -s openai-tunnel-runtime-key -a "${TUNNEL_KEY_ACCOUNT:-t3-code-mcp}" -w)" \
    || { log "runtime key not set and not readable from the keychain (locked?)"; sleep 30; exit 1; }
fi
export CONTROL_PLANE_API_KEY

tunnel-client run --profile-dir "$DIR" --profile t3 &
PID=$!
trap 'kill $PID 2>/dev/null; wait $PID; exit 0' TERM INT

sleep 60
bad=0
while kill -0 $PID 2>/dev/null; do
  if [ "$(curl -s -m 5 "$READYZ")" = ready ]; then bad=0; else bad=$((bad + 1)); fi
  if [ $bad -ge 6 ]; then log "tunnel-client not ready for 3 minutes; restarting"; kill $PID; wait $PID; exit 1; fi
  sleep 30
done
wait $PID
