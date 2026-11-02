"""Gateway registry CLI (operator tool, run on the server).

Commands (all take --root <gateway root>):
  create-pair --display-name NAME --muse-chat-id ID
      Create a pair, print its permanent pair-xxx id. Creates the
      pair's queue/state/log/media directories.
  set-credentials --pair ID --json-file FILE
      Encrypt a credential JSON ({bot_token, baseurl,
      ilink_bot_id, ilink_user_id}) into the pair's secret file.
      The FILE is read once and never copied anywhere.
  issue-token --pair ID
      Generate a hook token, store only its sha256, and print the
      token ONCE. Copy it into the hook's environment; this tool
      never writes it to any file. Re-issuing invalidates the old
      token (rotation).
  set-status --pair ID --status active|disabled
  list
      Pair ids, display names, statuses, muse chat ids only.
      No hashes, no refs, no secrets are ever printed.
  bind --pair ID [--timeout 280]
      One-command bind: run the old-tree login helper for a
      temporary pair dir, print QR_READY with the QR file path,
      wait for the scan, then import the credentials via the
      set-credentials path (hash-verify only, never print secret
      values), delete the temp dir, and print BOUND. The pair
      goes live within ~5s via the gateway reloader: no restart,
      no further step, no "I scanned, please confirm" message.

Tokens, bot_token values, context_tokens and tickets must never
be pasted into chat, logs, memory, or screenshots; the operator
carries the issued token straight into the hook environment.
"""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from registry import PAIR_ID_RE, Registry  # noqa: E402


def _check_pair_id(pair_id: str) -> None:
    if not PAIR_ID_RE.match(pair_id or ""):
        raise SystemExit(f"BAD_PAIR_ID {pair_id}")


def _bridge_login_python(bridge_dir: Path) -> tuple[str, Path]:
    """Return (python_bin, login_py) for the old-tree login helper.
    Prefers the old tree's venv (it has requests + qrcode); falls
    back to the current interpreter."""
    login_py = Path(bridge_dir) / "login.py"
    venv_py = login_py.parent.parent / ".venv" / "bin" / "python"
    py = str(venv_py) if venv_py.exists() else sys.executable
    return py, login_py


def cmd_bind(reg: Registry, pair_id: str, timeout: int) -> int:
    """One-command bind: QR -> wait -> import -> temp cleanup.
    Prints only QR_READY <path>, CRED_MATCH, BOUND (never secret
    values). The pair goes live within ~5s via the gateway
    reloader; no restart and no operator confirmation step."""
    _check_pair_id(pair_id)
    pair = reg.get(pair_id)
    if pair is None:
        print(f"UNKNOWN_PAIR {pair_id}")
        return 1
    bridge_dir = os.environ.get("MUCHAT_BRIDGE_DIR") or str(
        Path(__file__).resolve().parent.parent / "bridge")
    py, login_py = _bridge_login_python(Path(bridge_dir))
    if not login_py.exists():
        print(f"LOGIN_HELPER_MISSING {login_py}")
        return 1
    temp_name = f"gwbind-{pair_id}"
    temp_dir = login_py.parent.parent / "pairs" / temp_name
    if temp_dir.exists():
        # Temp dirs are single-use by construction; a stale one is
        # always safe to drop before a fresh bind.
        shutil.rmtree(temp_dir, ignore_errors=True)
    try:
        try:
            qr = subprocess.run(
                [py, str(login_py), "--get-qr", "--pair", temp_name],
                capture_output=True, text=True, timeout=120)
        except subprocess.TimeoutExpired:
            print("QR_FAILED timeout")
            return 1
        qr_out = (qr.stdout or "").strip()
        if qr.returncode != 0 or "QR_OK" not in qr_out:
            last = qr_out.splitlines()[-1] if qr_out else "no output"
            print(f"QR_FAILED {last}")
            return 1
        qr_path = temp_dir / "login_qr.png"
        if not qr_path.exists():
            print("QR_FAILED missing login_qr.png")
            return 1
        print(f"QR_READY {qr_path}", flush=True)
        if pair.get("status") != "active":
            print(f"PAIR_NOT_ACTIVE pair={pair_id} "
                  f"status={pair.get('status')} "
                  "(reloader will not start it until set-status active)")
        try:
            wait = subprocess.run(
                [py, str(login_py), "--wait", "--pair", temp_name,
                 "--timeout", str(timeout)],
                capture_output=True, text=True, timeout=timeout + 120)
        except subprocess.TimeoutExpired:
            print(f"EXPIRED pair={pair_id} status=WAIT_TIMEOUT")
            return 2
        wait_out = (wait.stdout or "").strip()
        if "CONFIRMED" not in wait_out:
            if "EXPIRED" in wait_out:
                status = "EXPIRED"
            elif "TIMEOUT" in wait_out:
                status = "TIMEOUT"
            else:
                status = (wait_out.splitlines()[-1] if wait_out
                          else "WAIT_FAILED")
            print(f"EXPIRED pair={pair_id} status={status}")
            return 2
        creds_path = temp_dir / "state" / "credentials.json"
        try:
            creds = json.loads(creds_path.read_text(encoding="utf-8"))
        except Exception:
            print("CRED_READ_FAILED")
            return 1
        if not creds.get("bot_token") or not creds.get("baseurl"):
            print("CRED_INCOMPLETE")
            return 1
        ref = reg.store_credentials(pair_id, creds)
        print(f"CREDENTIALS_STORED pair={pair_id} ref={ref}")
        # Hash-verify only: compare digests, never print values.
        try:
            back = reg.load_credentials(pair_id)
            ok = (hashlib.sha256(
                       str(creds.get("bot_token")).encode()).hexdigest()
                   == hashlib.sha256(
                       str(back.get("bot_token")).encode()).hexdigest())
        except Exception:
            ok = False
        print("CRED_MATCH" if ok else "CRED_MISMATCH")
        if not ok:
            return 1
        print(f"BOUND pair={pair_id}")
        print("NOTE pair goes live within ~5s via the gateway "
              "reloader (no restart, no further step)")
        return 0
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("create-pair")
    p.add_argument("--display-name", required=True)
    p.add_argument("--muse-chat-id", required=True)

    p = sub.add_parser("set-credentials")
    p.add_argument("--pair", required=True)
    p.add_argument("--json-file", required=True)

    p = sub.add_parser("issue-token")
    p.add_argument("--pair", required=True)

    p = sub.add_parser("set-status")
    p.add_argument("--pair", required=True)
    p.add_argument("--status", required=True)

    p = sub.add_parser("bind")
    p.add_argument("--pair", required=True)
    p.add_argument("--timeout", type=int, default=280)

    sub.add_parser("list")
    args = ap.parse_args()

    root = Path(args.root)
    reg = Registry(root)

    if args.cmd == "create-pair":
        pair_id = reg.create_pair(args.display_name, args.muse_chat_id)
        for subdir in ("queue/inbox", "queue/running", "queue/done",
                       "queue/outbox", "queue/failed", "state", "logs",
                       "media/in", "media/out"):
            (root / "pairs" / pair_id / subdir).mkdir(
                parents=True, exist_ok=True)
        print(f"PAIR_CREATED {pair_id}")
        return 0
    if args.cmd == "set-credentials":
        _check_pair_id(args.pair)
        creds = json.loads(Path(args.json_file).read_text(encoding="utf-8"))
        for key in ("bot_token", "baseurl"):
            if not creds.get(key):
                raise SystemExit(f"MISSING_CREDENTIAL_FIELD {key}")
        ref = reg.store_credentials(args.pair, creds)
        print(f"CREDENTIALS_STORED pair={args.pair} ref={ref}")
        return 0
    if args.cmd == "issue-token":
        _check_pair_id(args.pair)
        token = reg.issue_token(args.pair)
        # The ONLY place the plaintext token ever appears: this
        # stdout, for the operator to copy into the hook env.
        print(f"HOOK_TOKEN pair={args.pair} token={token}")
        return 0
    if args.cmd == "set-status":
        _check_pair_id(args.pair)
        reg.set_status(args.pair, args.status)
        print(f"STATUS_SET pair={args.pair} status={args.status}")
        return 0
    if args.cmd == "bind":
        return cmd_bind(reg, args.pair, args.timeout)
    if args.cmd == "list":
        for pid, info in sorted(reg.list_pairs().items()):
            print(f"{pid} status={info['status']} "
                  f"display={info['display_name']} "
                  f"muse_chat_id={info['muse_chat_id']}")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
