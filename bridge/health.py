#!/usr/bin/env python3
"""WeChat bridge health check (batch 3, reference ilink.py health).

Run ON Tokyo: ~/wechat-bridge/.venv/bin/python ~/wechat-bridge/bridge/health.py --pair primary
Reads only local pair state, never tokens. Prints one line per check
and exits 0 (ok), 1 (degraded: queue backlog or alert active), or
2 (down: no credentials, no bridge.log, or service not active when
systemctl is available). Also supports --prune (media prune only).
"""
import argparse
import json
import subprocess
import sys
import time
from pathlib import Path


def _load(path, default):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return default


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", default="primary")
    ap.add_argument("--prune", action="store_true")
    args = ap.parse_args()
    base = Path.home() / "wechat-bridge" / "pairs" / args.pair
    state, queue, logs = base / "state", base / "queue", base / "logs"
    checks = []
    code = 0

    def check(name, ok, detail=""):
        nonlocal code
        checks.append(f"{'OK' if ok else 'FAIL'} {name} {detail}".rstrip())
        if not ok and code == 0:
            code = 1

    creds = state / "credentials.json"
    check("credentials", creds.exists())
    log_path = logs / "bridge.log"
    if log_path.exists():
        age = time.time() - log_path.stat().st_mtime
        check("bridge_log", age < 3600, f"age_s={int(age)}")
    else:
        check("bridge_log", False, "missing")
        code = 2
    for sub in ("inbox", "running", "outbox", "failed"):
        d = queue / sub
        n = len(list(d.glob("*.json"))) if d.exists() else -1
        check(f"queue_{sub}", n >= 0, f"n={n}")
        if sub in ("inbox", "running", "outbox") and n > 5:
            code = 1
    alerts = _load(state / "alert_state.json", {})
    active = [k for k, v in alerts.items() if v.get("active")]
    check("alerts", not active, f"active={','.join(active) or 'none'}")
    if active:
        code = 1
    expired = state / "session_expired.json"
    check("session_expired", not expired.exists())
    if expired.exists():
        code = 1
    try:
        r = subprocess.run(["systemctl", "is-active", f"wechat-bridge-{args.pair}.service"],
                           capture_output=True, text=True, timeout=10)
        svc = r.stdout.strip()
        check("service", svc == "active", svc)
        if svc != "active":
            code = 2
    except Exception:
        check("service", True, "systemctl_unavailable")
    if args.prune:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        try:
            from daemon import PairCtx, prune_media_dir
            ctx = PairCtx(args.pair)
            removed, total = prune_media_dir(ctx)
            checks.append(f"OK media_prune removed={removed} remaining_bytes={total}")
        except Exception as exc:
            checks.append(f"FAIL media_prune {type(exc).__name__}")
            code = 1
    print("\n".join(checks))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
