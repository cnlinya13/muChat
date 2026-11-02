# Gateway hook templates (single-port architecture)

These templates connect a Muse to the muChat gateway (one port for
all pairs). Each pair is identified by a permanent `pair-xxxxxxxx`
id; the gateway verifies the pair's hook token on every request.

Order matters: install the hook script first, link it to the
gateway, and only then generate the binding QR. Do not generate
the QR before the hook is linked.

## Files

- `hook-script-gateway.sh.template` — polling script (copy per
  pair; never write a token into it).
- `hook-definition-gateway.json.template` — worker definition
  (copy per pair and replace `PAIR_ID`, `delivery.to`, and the
  environment placeholders).

## Install

```sh
curl -sSL https://raw.githubusercontent.com/cnlinya13/muChat/main/examples/gateway/hook-script-gateway.sh.template -o ~/hooks/scripts/wechat-bridge-gateway-<pair_id>.sh
curl -sSL https://raw.githubusercontent.com/cnlinya13/muChat/main/examples/gateway/hook-definition-gateway.json.template -o /tmp/hook-definition-gateway.json
```

## Setup

1. Copy the script to the hooks scripts dir as
   `wechat-bridge-gateway-<pair_id>.sh` and make it executable.
2. Create the pair in the gateway registry
   (`gateway_cli create-pair`) and note its pair id. Fill your
   own Muse chat id as `muse_chat_id`.
3. Issue the pair's hook token (`gateway_cli issue-token`). The
   token is printed once, as a labeled line (`HOOK_TOKEN pair=...
   token=...`): extract just the token value (for example with
   `sed 's/.* token=//'`) and put it ONLY in a 0600 environment
   file that the script sources (for example
   `~/hooks/env/wechat-bridge-gateway-<pair_id>.env` holding
   `MUCHAT_GATEWAY_HOST`, `MUCHAT_GATEWAY_PORT`,
   `MUCHAT_GATEWAY_PAIR_ID`, `MUCHAT_GATEWAY_TOKEN`). Never store
   the whole labeled line, and never write the token into the
   script, the definition, chat, logs, or screenshots. (Note:
   some hook tools do not accept an `environment` field in the
   definition — the env file is the portable way.)
4. Create the hook from the definition template (replace
   `PAIR_ID`, set `delivery.to` to your own chat id, set the
   gateway host to your gateway's tailnet address, port 3820
   unless your deployment differs). Start disabled, dry-run
   clean, then enable.
5. Only after the hook polls clean (empty queue, no errors),
   generate the binding QR with the gateway's one-command bind
   (`MUCHAT_BRIDGE_DIR=<old-tree bridge dir> gateway_cli.py
   --root <gateway-root> bind --pair <pair_id>`): it prints
   QR_READY, waits for the scan, imports the credentials
   (hash-verify only, never print values), deletes the
   temporary directory, and the pair goes live within ~5s via
   the reloader — no restart, no further step. Manual runs need
   the MUCHAT_BRIDGE_DIR variable (the service unit sets it, a
   manual shell does not). The QR lasts ~2 minutes; if it
   expires, re-run bind for a fresh one.
6. Ping test: the bound WeChat sends ping, only this Muse
   replies. Cross-talk test: ping on one pair while the other
   pair's queue and hook stay silent, then reverse.

## Notes

- The old per-pair ports (3810/3811) and per-pair daemons are
  retired; everything goes through the gateway port.
- Worker rules preserved: one worker one reply, inventory with
  exact-copied file names, settle-wait before outbox, generic
  daemon ack, safety red lines (WeChat is not an approval
  channel).
