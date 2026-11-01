"""QR login for the WeChat bridge (P1).

Usage:
  python3 bridge/login.py --get-qr
      Request a QR session, save the (non-token) session id to
      state/qr_session.json (0600), download the QR image to
      login_qr.png, and print only: status keys, not values.

  python3 bridge/login.py --wait [--timeout 300]
      Poll get_qrcode_status until confirmed / expired / blocked.
      On confirmed, atomically write state/credentials.json (0600)
      with bot_token + baseurl + ilink_bot_id + ilink_user_id, then
      delete qr_session.json. Prints only CONFIRMED / EXPIRED /
      NEED_VERIFYCODE / VERIFY_CODE_BLOCKED / BINDED_REDIRECT /
      TIMEOUT -- never the token, qrcode value, or user ids.

Status handling follows the official protocol doc:
  wait, scaned -> keep polling
  scaned_but_redirect -> keep polling (redirect_host noted, not chased
      in P1; if it occurs in practice, P1 is re-tested before proceeding)
  confirmed -> save credentials
  expired -> caller must request a fresh QR
  need_verifycode -> stop; the user must supply the code shown in WeChat
  verify_code_blocked -> stop, refresh or stop per official doc
  binded_redirect -> stop; bot already bound to this instance
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ilink_client import (  # noqa: E402
    DEFAULT_BASE_URL,
    STATE_DIR,
    _atomic_write_json,
    get_bot_qrcode,
    get_qrcode_status,
)

BRIDGE_DIR = Path(__file__).resolve().parent.parent


def pair_dirs(pair_id: str):
    pair_dir = BRIDGE_DIR / "pairs" / pair_id
    state_dir = pair_dir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    pair_dir.mkdir(parents=True, exist_ok=True)
    return pair_dir, state_dir


def cmd_get_qr(pair_id: str, force: bool = False) -> int:
    import requests

    pair_dir, state_dir = pair_dirs(pair_id)
    if (state_dir / "credentials.json").exists() and not force:
        print("CREDENTIALS_EXIST pair=" + pair_id + " (use --force to re-login)")
        return 8
    data = get_bot_qrcode()
    qrcode = data.get("qrcode")
    img = data.get("qrcode_img_content")
    if not qrcode or not img:
        print(f"QR_REQUEST_FAILED keys={sorted(data.keys())}")
        return 1
    _atomic_write_json(state_dir / "qr_session.json", {"qrcode": qrcode})
    # For Test 1 decode-back: store the exact img_content string (0600,
    # deleted on confirmed/expired by the caller) so the rendered PNG
    # can be decoded and compared byte-for-byte before showing it.
    _atomic_write_json(state_dir / "qr_img_content.json", {"img_content": img})
    # Correction from live test 2026-11-01: qrcode_img_content is NOT an
    # image file. It is a liteapp URL (https://liteapp.weixin.qq.com/q/...)
    # that must be ENCODED into a QR image; downloading it from the VM
    # returns a 1.4 KB HTML shell, not a QR picture. So we encode the
    # string itself with the `qrcode` library (Pillow for rendering).
    try:
        import qrcode
    except ImportError:
        print("QR_ENCODER_MISSING")
        return 7
    out = pair_dir / "login_qr.png"
    qr = qrcode.QRCode(border=2)
    qr.add_data(str(img))
    qr.make()
    qr.make_image(fill_color="black", back_color="white").save(out)
    print(f"QR_OK img_bytes={out.stat().st_size}")
    return 0


def cmd_wait(pair_id: str, timeout: int) -> int:
    _pair_dir, state_dir = pair_dirs(pair_id)
    session_path = state_dir / "qr_session.json"
    if not session_path.exists():
        print("NO_QR_SESSION")
        return 1
    qrcode = json.loads(session_path.read_text(encoding="utf-8"))["qrcode"]
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            data = get_qrcode_status(qrcode)
        except Exception as exc:  # network hiccup: retry, do not abort
            print(f"POLL_ERROR {type(exc).__name__}")
            time.sleep(3)
            continue
        status = data.get("status")
        if status == "confirmed":
            creds = {
                "bot_token": data.get("bot_token"),
                "baseurl": data.get("baseurl") or DEFAULT_BASE_URL,
                "ilink_bot_id": data.get("ilink_bot_id"),
                "ilink_user_id": data.get("ilink_user_id"),
            }
            if not creds["bot_token"]:
                print("CONFIRMED_NO_TOKEN")
                return 1
            _atomic_write_json(state_dir / "credentials.json", creds, mode=0o600)
            session_path.unlink(missing_ok=True)
            (state_dir / "qr_img_content.json").unlink(missing_ok=True)
            print("CONFIRMED")
            return 0
        if status in ("wait", "scaned", "scaned_but_redirect"):
            time.sleep(2)
            continue
        if status == "expired":
            print("EXPIRED")
            return 2
        if status == "need_verifycode":
            print("NEED_VERIFYCODE")
            return 3
        if status == "verify_code_blocked":
            print("VERIFY_CODE_BLOCKED")
            return 4
        if status == "binded_redirect":
            print("BINDED_REDIRECT")
            return 5
        print(f"UNKNOWN_STATUS={status}")
        time.sleep(2)
    print("TIMEOUT")
    return 6


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--get-qr", action="store_true")
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--pair", default="primary")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    if args.get_qr:
        return cmd_get_qr(args.pair, force=args.force)
    if args.wait:
        return cmd_wait(args.pair, args.timeout)
    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
