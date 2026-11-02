#!/usr/bin/env bash
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

# WeChat bridge inbox watcher, pair second.
# Long-polls Tokyo's notify endpoint (tailnet only, ids only) via the
# tunnel proxy. No secrets in this script: no bot_token, no typing
# ticket, no proxy password embedded; proxy comes from the environment.
# State: ~/hooks/state/wechat-bridge-inbox-second.json (seen ids).
# Dry runs must not consume detections: state is not written when
# HATCH_HOOK_DRY_RUN=1.
PAIR_ID="second"
STATE_FILE="$HOME/hooks/state/wechat-bridge-inbox-second.json"
TUNNEL_PROXY="${HTTPS_PROXY%:*}:3130"
URL="http://${MUCHAT_NOTIFY_HOST:?MUCHAT_NOTIFY_HOST-unset}:3811/inbox/wait?timeout=12"

if ! resp=$(curl --fail --silent --show-error --max-time 15 --proxy "$TUNNEL_PROXY" "$URL" 2>&1); then
  log "wechat_inbox_fetch_failed" '{"error": "curl_failed"}'
  silent "wechat inbox fetch failed, retry next poll"
  exit 0
fi

pair=$(echo "$resp" | jq -r '.pair_id // empty' 2>/dev/null || true)
if [[ "$pair" != "$PAIR_ID" ]]; then
  log "wechat_inbox_pair_mismatch" '{"pair": "REDACTED"}'
  silent "wechat inbox pair mismatch, ignoring"
  exit 0
fi

# Alert surfacing (reliability batch, 2026-11-01): daemon reports
# active alert kinds (session_expired / poll_failed / worker_stuck)
# in the notify response. Log kinds only, never tokens or text,
# and carry them in the wake payload so the worker can mention
# them in its reply. No wake is created for alerts alone: they
# ride along with the next real message, so the user is not
# spammed with alert-only wakes.
alerts=$(echo "$resp" | jq -r '.alerts[]?' 2>/dev/null || true)
if [[ -n "$alerts" ]]; then
  log "wechat_bridge_alerts" "{\"alerts\": \"$(echo "$alerts" | paste -sd, -)\"}"
fi

current_ids=$(echo "$resp" | jq -r '.new[]?' 2>/dev/null || true)
# Text passthrough (2026-11-01, user-approved): the daemon includes
# verbatim text for text-only messages so the worker can answer
# without an SSH fetch first. Texts travel only in the wake payload
# for this pair; media is never inlined here.
texts_json=$(echo "$resp" | jq -c '.texts // {}' 2>/dev/null || echo '{}')
export CURRENT_IDS="$current_ids"
new_ids=$(python3 - << 'PY'
import json, os
cur = [l.strip() for l in os.environ.get("CURRENT_IDS", "").splitlines() if l.strip()]
state_file = os.path.expanduser("~/hooks/state/wechat-bridge-inbox-second.json")
try:
    seen = set(json.load(open(state_file)).get("seen", []))
except Exception:
    seen = set()
new = [i for i in cur if i not in seen]
print("\n".join(new))
PY
)

if [[ -z "$new_ids" ]]; then
  silent "no new wechat inbox messages"
  exit 0
fi

if [[ "${HATCH_HOOK_DRY_RUN:-0}" != "1" ]]; then
  python3 - << 'PY'
import json, os
cur = [l.strip() for l in os.environ.get("CURRENT_IDS", "").splitlines() if l.strip()]
state_file = os.path.expanduser("~/hooks/state/wechat-bridge-inbox-second.json")
try:
    seen = set(json.load(open(state_file)).get("seen", []))
except Exception:
    seen = set()
merged = sorted(seen | set(cur))
tmp = state_file + ".tmp"
json.dump({"seen": merged}, open(tmp, "w"))
os.replace(tmp, state_file)
PY
fi

export ALERTS="$alerts"
export TEXTS_JSON="$texts_json"
payload=$(printf "%s" "$new_ids" | python3 -c "
import json, os, sys
ids = [l.strip() for l in sys.stdin if l.strip()]
alerts = [l.strip() for l in os.environ.get('ALERTS', '').splitlines() if l.strip()]
try:
    texts = json.loads(os.environ.get('TEXTS_JSON', '{}'))
except Exception:
    texts = {}
texts = {k: v for k, v in texts.items() if k in ids}
payload = {'pair_id': 'second', 'message_ids': ids}
if texts:
    payload['texts'] = texts
if alerts:
    payload['bridge_alerts'] = alerts
print(json.dumps(payload))
")
wake "new wechat inbox messages" "$payload"

# Supplement merge is done by the worker BEFORE writing its
# outbox (hook definition, user-set 2026-11-01): the worker
# re-checks inbox before writing, so a follow-up arriving while
# the reply is being assembled is merged into the same reply.
# No warm marker or second wake is created here; the daemon
# notifies immediately on the first message, with no batch wait.
