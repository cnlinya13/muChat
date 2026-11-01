# Second pair onboarding checklist (multi-WeChat / multi-Muse, one-to-one)
# This is preparation for the final goal, not a live second pair yet.
# A second pair cannot be created from the primary Muse chat alone.

## What is already pair-agnostic (no redesign needed)
- Daemon code: --pair <pair_id> selects pairs/<pair_id>/state|queue|logs,
  port from PAIR_PORTS (primary 3810, add second 3811, etc. in code
  before onboarding a second pair), HTTP response includes pair_id.
- Hook script: pair_id, port and state file are variables at the top;
  copy the primary script and change those three values for a new pair.
- Worker prompt: pair_id, queue paths and HTTP port are parameters;
  copy the primary hook definition and change them.

## Steps to add a second pair (in order, do not skip the cross-talk test)
1. the user provides: second WeChat account (to scan QR) and second
   Muse account (to set up its hook). The second Muse account's hook
   MUST be created in that account's own Muse chat, with its own
   approval, not from the primary chat.
2. On Tokyo, create pairs/second/ (state/queue/logs), run QR login
   with --pair second (login.py must be updated to accept --pair
   before this step; currently login.py writes to the legacy top-level
   state/ and must not be used for a second pair until that change
   is made — this is a known gap, not a live capability).
3. Add second=3811 to PAIR_PORTS in daemon.py, copy to Tokyo.
4. Create /etc/systemd/system/wechat-bridge-second.service from the
   primary unit (change Description, ExecStart --pair second),
   daemon-reload, enable, start, check active + QUEUE_DAEMON_STARTED.
5. In the second Muse account's chat: copy the hook script with
   pair=second, port=3811, state file wechat-bridge-inbox-second.json,
   hooks.add (disabled), hooks.dry_run until clean, hooks.enable.
6. Cross-talk test (mandatory before second pair goes live):
   - Send ping from pair A's WeChat: only pair A's Muse replies,
     pair B's queue/inbox must stay empty, pair B's hook must stay silent.
   - Send ping from pair B's WeChat: only pair B replies, pair A silent.
   - If either test shows cross-talk, stop both pairs' new traffic
     and investigate before continuing.
7. Record the second pair in MEMORY.md (pair_id, ports, hook id,
   service name, QR login date) without recording tokens.

## Known gaps before a second pair can go live
- login.py is still single-pair (legacy state/); it needs --pair
  support before a second QR login, otherwise a second login would
  overwrite the primary credentials. DO NOT run a second QR login
  until that change is made and reviewed.
- PAIR_PORTS currently only lists primary; second must be added
  in code and copied to Tokyo before its daemon starts.
- No second WeChat or second Muse account has been provided yet;
  onboarding cannot start until the user provides them and is ready
  to scan / approve in the second Muse account.
