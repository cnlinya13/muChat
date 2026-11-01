"""Proactive (P4) outbox writer for scheduled Muse tasks.

Usage (on Tokyo, or with --pair-dir pointing at a pair dir):
  python3 bridge/proactive_write.py --pair primary --reply "text"
  python3 bridge/proactive_write.py --pair primary --kind image --media-path pairs/primary/media/out/test.png

Rules:
- Reads pairs/<pair>/state/last_context.json (written by the daemon on
  each inbound) and pairs/<pair>/state/sent_count.json.
- If the last inbound is older than ~24h (brief/mirror figure, not a
  verified official constant) or the token budget (9, same figure) is
  exhausted, it does NOT write an outbox file, prints WINDOW_EXPIRED
  or OVER_BUDGET, and exits 2. The scheduled task must then report
  back in Muse that the user needs to send a message in WeChat to
  reopen the window, instead of silently dropping the task.
- It never reads bot_token and never calls iLink directly; only the
  daemon sender (which owns the token) sends.
"""
import argparse, json, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from ilink_client import check_proactive_window

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pair", default="primary")
    p.add_argument("--pair-dir", default="")
    p.add_argument("--reply", default="")
    p.add_argument("--kind", default="text")
    p.add_argument("--media-path", default="")
    a = p.parse_args()
    pair_dir = Path(a.pair_dir) if a.pair_dir else Path(__file__).resolve().parent.parent / "pairs" / a.pair
    lc_path = pair_dir / "state" / "last_context.json"
    if not lc_path.exists():
        print("NO_LAST_CONTEXT"); return 1
    lc = json.loads(lc_path.read_text())
    chk = check_proactive_window(lc)
    if not chk["ok"]:
        print("WINDOW_EXPIRED " + chk["reason"]); return 2
    sc_path = pair_dir / "state" / "sent_count.json"
    sc = json.loads(sc_path.read_text()) if sc_path.exists() else {}
    used = int(sc.get(lc["context_token"], 0))
    if used >= 9:
        print(f"OVER_BUDGET used={used}"); return 2
    if a.kind == "image" and not a.media_path:
        print("MISSING_MEDIA_PATH"); return 1
    if a.kind == "text" and not a.reply:
        print("MISSING_REPLY"); return 1
    mid = f"proactive-{int(time.time())}"
    out = {"message_id": mid, "pair_id": a.pair, "from_user_id": lc["from_user_id"],
           "context_token": lc["context_token"], "reply": a.reply, "proactive": True,
           "kind": a.kind, "media_path": a.media_path}
    (pair_dir / "queue" / "outbox").mkdir(parents=True, exist_ok=True)
    (pair_dir / "queue" / "outbox" / f"{mid}.json").write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(f"PROACTIVE_QUEUED id={mid} window={chk['reason']} used={used}")
    return 0
if __name__ == "__main__":
    raise SystemExit(main())
