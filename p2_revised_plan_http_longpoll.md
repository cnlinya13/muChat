# P2 revised plan — HTTP long-poll version (replaces the SSH-ls cron draft)
# Status: PLAN ONLY, not yet implemented. Approved by the user for review
# on 2026-11-01 after she said she cares about the ~10s hook latency.
# Do NOT build from the old worker_cron_instructions.md cron-agent plan.

## Why this version
- SSH-ls hook: hook every 30s, average wake wait ~15s, max 30s, plus
  an SSH login every check (2880/day).
- HTTP long-poll hook: hook request hangs up to 30s waiting for Tokyo
  to report a new inbox file, so wake wait drops to ~2-3s average,
  max ~5-10s (hook gap + startup). Token cost stays 0 in both
  versions (the checker is a script, not an agent); the gain is
  latency and fewer SSH logins, not tokens.
- The saving only survives end-to-end if the outbox sender is also
  decoupled from the getupdates long-poll (see daemon threads below).
  Otherwise outbox latency (up to 35-50s) would eat the hook saving.

## Tokyo side: queue daemon, threaded (replaces echo daemon)
Daemon: bridge/daemon.py --mode queue, running under systemd on Tokyo.
Threads in one process (single writer for iLink sends):
1. Poll thread: POST getupdates long-poll (35s hold), writes each new
   message atomically to queue/inbox/<message_id>.json (0600), updates
   cursor + seen_ids, and signals the notify event. On -14, it does
   NOT exit: it cancels typing, writes session_expired marker, pauses
   polling internally for 1 hour, then resumes. systemd RestartSec
   is therefore 30s (crash recovery), not 3600s.
2. Notify thread: tiny HTTP server, bound ONLY to the tailnet IP
   <BIND_HOST>, port 3810, no public listen. Endpoint:
   GET /inbox/wait?since=<last_seen_id>&timeout=30
   - Waits on the notify event up to timeout, then returns
     {"new": [<message_id>, ...]} immediately when a file appears,
     or {"new": []} on timeout.
   - Returns message IDs ONLY. Never message text, context_token,
     bot_token, or user IDs in the HTTP response or logs.
3. Sender thread: scans queue/outbox/ every 2s (independent of the
   poll thread), chunks replies (900 chars), counts per-token budget
   (9, brief budget), sends in order with 1s between chunks, moves
   sent files to done/, failures to failed/. -2 is unclassified
   send failure, not auto-labelled quota. -14 pauses as above.
4. Typing thread: for each queued/running message, getconfig once
   (ticket cached per user, 24h, in state/typing_tickets.json 0600),
   sendtyping status=1 every 5s (community-observed refresh;
   official doc gives the endpoints, not the refresh interval),
   status=2 when that message's outbox is sent or fails.

## Muse side: hook script, not a cron agent
- Hook script: ~/hooks/scripts/wechat-bridge-inbox.sh (bash, sources
  $HATCH_HOOK_RUNTIME, ends with exactly one silent/wake).
- Each run: curl via the tunnel proxy (same ${HTTPS_PROXY%:*}:3130
  route used for SSH, HTTP this time) to
  http://<BIND_HOST>:3810/inbox/wait?since=...&timeout=30,
  --max-time 35. No secrets in the script: no token, no ticket,
  no proxy password embedded; proxy comes from the environment.
- Hook poll_interval_secs: 35, long-poll timeout 30, gap ~5s.
  Average wake wait ~2-3s, max ~5-10s plus hook startup.
- State: ~/hooks/state/wechat-bridge-inbox.json, last seen ids.
  Dry runs (HATCH_HOOK_DRY_RUN=1) must not consume detections.
  Inconclusive fetch (curl fail, timeout, bad JSON): log and silent,
  next run retries. No fallback to SSH-ls in v1; if HTTP fails
  repeatedly, that is a reported failure, not a silent fallback.
- Wake payload: {"message_ids": [...]} — IDs only.
- Worker agent (launched only on wake): SSH to Tokyo with the
  deployment helper, move the oldest inbox file to
  running/, read it, execute the task, write queue/outbox/<id>.json,
  move inbox to done/ or failed/. One message at a time, in order.
  If the worker crashes, the file stays in running/; the next worker
  run must move stale running/ files (older than 30 min, no outbox)
  back to inbox/ for retry, up to 3 tries, then failed/ + a WeChat
  reply "这个任务失败了" (via outbox, so the user is not left
  hanging). High-consequence actions still require Muse approval;
  worker must not auto-approve, and must reply via outbox
  "已在 Muse 里发起确认，请去那边批准" and move to done/.

## Latency budget, simple task (revised)
- WeChat -> Tokyo inbox: a few seconds (iLink long-poll).
- Inbox -> hook wake: ~2-3s average, ~5-10s max.
- Worker execution: task-dependent.
- Outbox -> WeChat: ~2s (sender thread, every 2s) + send time.
- Total simple task: roughly 30-60s, down from 60-120s in the
  SSH-ls version. The remaining time is task execution, not polling.

## Security / stop
- HTTP endpoint: tailnet IP only, IDs only, no text/token in
  response or hook logs. Worker SSH still uses the existing
  ~/.ssh/id_ed25519 key already authorised on Tokyo.
- Stop: stop Tokyo systemd queue daemon + disable hook + WeChat
  plugin unbind. Three steps, as in the brief.

## Implementation steps (only after the user approves this plan)
1. Rewrite daemon.py queue mode with the four threads above.
2. Copy to Tokyo, stop echo daemon, start queue daemon under
   systemd (RestartSec=30, -14 handled internally).
3. Write hook script, hooks.add (disabled), hooks.dry_run until
   clean, then hooks.enable.
4. Live test: send ping in WeChat, measure inbox time, wake time,
   reply time, typing on/off, chunking, no duplicate after restart.
5. Report P2 results in the brief's four-part format; P2 is not
   complete until the live test passes.
