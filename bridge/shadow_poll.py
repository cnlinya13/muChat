#!/usr/bin/env python3
"""Shadow poll recorder for the burst-loss test (Scheme A).

Runs ONLY while the main wechat-bridge service is stopped. It polls
ilink getupdates with its own shadow cursor, writes every raw batch
(headers only: message_id/seq/types/file_names/timestamp) to
pairs/<pair>/state/shadow_audit.jsonl, and NEVER writes inbox files,
NEVER sends anything, NEVER touches the main cursor.

Why a separate cursor: two pollers sharing one cursor would split
messages between them. The shadow therefore starts from the main
cursor snapshot taken at stop time, and after the test the main
service is restored by copying the shadow cursor back ONLY if the
test batch was fully processed by the shadow log review -- in
practice the test procedure is: stop main, snapshot cursor, run
shadow, user sends burst, stop shadow, compare shadow_audit vs
user's send list, then restart main with the ORIGINAL snapshot
cursor so the main daemon re-fetches the test batch and the worker
replies normally. The shadow cursor is never fed back into main
unless the operator explicitly chooses to skip re-fetch.

Usage (on Tokyo, from ~/wechat-bridge):
  .venv/bin/python bridge/shadow_poll.py --pair primary --seconds 180
"""
import argparse
import json
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ilink_client import getupdates  # noqa: E402


def now_beijing():
    return (datetime.now(timezone.utc) + timedelta(hours=8)).isoformat()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", default="primary")
    ap.add_argument("--seconds", type=int, default=180)
    args = ap.parse_args()
    base = Path(__file__).resolve().parent.parent
    pair_dir = base / "pairs" / args.pair
    state = pair_dir / "state"
    creds = json.loads((state / "credentials.json").read_text(encoding="utf-8"))
    # Start from the main cursor snapshot, NOT from the live cursor file
    # (which the stopped main daemon no longer updates).
    snap = state / "main_cursor_snapshot.json"
    if snap.exists():
        cursor = json.loads(snap.read_text(encoding="utf-8")).get("get_updates_buf", "")
    else:
        cursor = json.loads((state / "cursor.json").read_text(encoding="utf-8")).get(
            "get_updates_buf", "")
    out = state / "shadow_audit.jsonl"
    print(f"SHADOW_START pair={args.pair} seconds={args.seconds}", flush=True)
    deadline = time.time() + args.seconds
    while time.time() < deadline:
        try:
            data = getupdates(creds["bot_token"], creds["baseurl"], cursor)
        except Exception as exc:
            print(f"SHADOW_POLL_ERROR {type(exc).__name__}", flush=True)
            time.sleep(5)
            continue
        if data.get("ret") not in (0, None):
            print(f"SHADOW_POLL_RET={data.get('ret')} errcode={data.get('errcode')}", flush=True)
            time.sleep(5)
            continue
        msgs = data.get("msgs") or []
        new_cursor = data.get("get_updates_buf")
        if new_cursor:
            cursor = new_cursor
            # Shadow cursor is kept separate; never write cursor.json.
            (state / "shadow_cursor.json").write_text(
                json.dumps({"get_updates_buf": cursor}), encoding="utf-8")
        if msgs:
            line = json.dumps({
                "ts": now_beijing(),
                "count": len(msgs),
                "msgs": [{
                    "message_id": str(m.get("message_id", "")),
                    "seq": m.get("seq"),
                    "types": [i.get("type") for i in (m.get("item_list") or [])],
                    "file_names": [(i.get("file_item") or {}).get("file_name", "")
                                   for i in (m.get("item_list") or [])
                                   if i.get("type") == 4],
                } for m in msgs],
            }, ensure_ascii=False)
            with open(out, "a", encoding="utf-8") as f:
                f.write(line + "\n")
            print(f"SHADOW_BATCH count={len(msgs)}", flush=True)
    print("SHADOW_DONE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
