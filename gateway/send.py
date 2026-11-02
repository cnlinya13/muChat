"""Unified sender for the gateway.

One thread scans every active pair's outbox in turn and applies
the live daemon's sender rules per file:

- Pair check: an outbox whose pair_id differs from the directory's
  pair is moved to failed/ (never sent to the wrong WeChat).
- Validation: empty or "--"-prefixed text replies are never sent.
- Send ledger: a previous 'sent' for the same outbox key is never
  re-sent (server accepted, ack lost on crash).
- Merge wait 5s (daemon acks skip it): a later outbox for the same
  user supersedes this one.
- Chunking 900 chars, per-context_token budget 9, 1s between
  chunks, per-chunk sent_chunks resume.
- Image outboxes go through Tencent's OFFICIAL openclaw-weixin
  driver (compiled dist, Node 22), via a 0600 request file that is
  deleted after the run. The custom Python image path is not used.
- -14: cancel typing, write session_expired.json, pause the pair.
- Done/failed moves, stop_typing, out log lines (counts only).

DRY-RUN mode (env GATEWAY_SENDER_DRY_RUN=1): validation, pair
check and ledger rules still run, but instead of calling Tencent
the outbox is moved to done/ with note "dry_run_no_send". This is
how routing is self-tested without a real bound bot; it proves
the right pair's outbox was picked up and would have used the
right credentials, without sending anything.

Network functions are injectable (send_text_fn, send_typing_fn,
image_driver_fn) so unit tests never touch Tencent or Node.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import os as _os
# Bridge dir: on the dev VM the gateway lives inside wechat-bridge/
# (parent/bridge); on the server it lives in wechat-bridge-gateway/,
# so the unit sets MUCHAT_BRIDGE_DIR=/home/ubuntu/wechat-bridge/bridge.
_BRIDGE_DIR = _os.environ.get(
    "MUCHAT_BRIDGE_DIR",
    str(Path(__file__).resolve().parent.parent / "bridge"))
sys.path.insert(0, _BRIDGE_DIR)

from runtime import (  # noqa: E402
    MAX_PER_TOKEN,
    _atomic_write_json,
    _load_json,
    _now_beijing,
)

MERGE_WAIT_SECONDS = 5


def _validate_reply_text(reply):
    if reply is None:
        return "empty_reply"
    if not str(reply).strip():
        return "empty_reply"
    if str(reply).lstrip().startswith("--"):
        return "flag_like_reply"
    return None


def _default_send_text(token, baseurl, to_user, context_token, chunk):
    from ilink_client import send_text
    return send_text(token, baseurl, to_user, context_token, chunk)


def _default_send_typing(token, baseurl, user_id, ticket, status):
    from ilink_client import send_typing
    return send_typing(token, baseurl, user_id, ticket, status)


def _default_image_driver(req_path, node_bin, driver_path):
    """Run the official compiled driver. Returns True on ok."""
    run = subprocess.run([node_bin, driver_path, str(req_path)],
                         capture_output=True, text=True, timeout=60)
    if not run.stdout.strip():
        return False
    try:
        out = json.loads(run.stdout.strip().splitlines()[-1])
    except Exception:
        return False
    return bool(out.get("ok"))


def send_one_outbox(rt, out_path, deps=None, dry_run=False):
    """Process one outbox file for one pair. Returns a short status
    string (sent / failed / superseded / skipped_duplicate /
    dry_run). Never raises for a per-file problem."""
    deps = deps or {}
    send_text_fn = deps.get("send_text_fn") or _default_send_text
    send_typing_fn = deps.get("send_typing_fn") or _default_send_typing
    image_driver_fn = deps.get("image_driver_fn") or _default_image_driver
    node_bin = deps.get("node_bin", "/home/ubuntu/node22/bin/node")
    driver_path = deps.get(
        "driver_path", "/home/ubuntu/official-send-dist/driver.mjs")

    from chunking import chunk_text

    try:
        out = json.loads(out_path.read_text(encoding="utf-8"))
    except Exception:
        try:
            out_path.rename(rt.queue_dir / "failed" / out_path.name)
        except Exception:
            pass
        return "failed"
    msg_id = out.get("message_id", out_path.stem)
    to_user = out.get("from_user_id", "")
    context_token = out.get("context_token", "")
    reply = out.get("reply", "")

    def _fail(error):
        out["error"] = error
        _atomic_write_json(rt.queue_dir / "failed" / out_path.name, out)
        out_path.unlink(missing_ok=True)
        rt.stop_typing(to_user, msg_id, send_typing_fn)
        return "failed"

    if out.get("pair_id") and out["pair_id"] != rt.pair_id:
        try:
            out_path.rename(rt.queue_dir / "failed" / out_path.name)
        except Exception:
            pass
        print(f"PAIR_MISMATCH pair={rt.pair_id} id={msg_id}", flush=True)
        return "failed"
    if not to_user or not context_token or (
            not reply and not (out.get("kind") in ("image", "image_plain")
                               and out.get("media_path"))):
        return _fail("missing_route_or_reply")
    if out.get("kind") not in ("image", "image_plain"):
        verr = _validate_reply_text(reply)
        if verr:
            return _fail(verr)
    if rt.ledger_status(msg_id) == "sent":
        out["sent_at"] = _now_beijing().isoformat()
        out["note"] = "skipped_duplicate_ledger_sent"
        _atomic_write_json(rt.queue_dir / "done" / out_path.name, out)
        out_path.unlink(missing_ok=True)
        rt.stop_typing(to_user, msg_id, send_typing_fn)
        return "skipped_duplicate"
    skip_merge = bool(out.get("skip_merge_wait"))
    if not skip_merge and not dry_run:
        deadline = time.time() + MERGE_WAIT_SECONDS
        superseded = False
        while time.time() < deadline:
            later = [p for p in sorted(
                (rt.queue_dir / "outbox").glob("*.json"))
                if p != out_path and p.stem != msg_id]
            if later:
                superseded = True
                break
            time.sleep(0.5)
        if superseded:
            out["note"] = "superseded_by_later_outbox"
            _atomic_write_json(rt.queue_dir / "done" / out_path.name, out)
            out_path.unlink(missing_ok=True)
            rt.stop_typing(to_user, msg_id, send_typing_fn)
            return "superseded"
    chunks = chunk_text(reply) if reply else []
    if out.get("kind") in ("image", "image_plain") and out.get("media_path"):
        chunks = ["<media>"]
    with rt.lock:
        used = int(rt.sent_count.get(context_token, 0))
    if len(chunks) > MAX_PER_TOKEN - used:
        print(f"OVER_BUDGET pair={rt.pair_id} id={msg_id} "
              f"chunks={len(chunks)} used={used}", flush=True)
        return _fail("over_token_budget")
    if dry_run:
        out["sent_at"] = _now_beijing().isoformat()
        out["note"] = "dry_run_no_send"
        rt.ledger_mark(msg_id, "sent")
        _atomic_write_json(rt.queue_dir / "done" / out_path.name, out)
        out_path.unlink(missing_ok=True)
        rt.stop_typing(to_user, msg_id, send_typing_fn)
        return "dry_run"
    failed = False
    sent_chunks = set(out.get("sent_chunks", []) or [])
    for ci, chunk in enumerate(chunks):
        if ci in sent_chunks:
            continue
        if rt.paused.is_set():
            failed = True
            break
        try:
            if chunk == "<media>":
                req_path = rt.state_dir / f"send_req_{msg_id}.json"
                _atomic_write_json(req_path, {
                    "baseUrl": rt.baseurl, "token": rt.token,
                    "to": to_user, "contextToken": context_token,
                    "filePath": out["media_path"],
                    "caption": out.get("caption", "") or out.get("reply", ""),
                })
                try:
                    ok = image_driver_fn(req_path, node_bin, driver_path)
                    res = {"ret": 0} if ok else {"ret": 1}
                finally:
                    req_path.unlink(missing_ok=True)
            else:
                res = send_text_fn(rt.token, rt.baseurl, to_user,
                                   context_token, chunk)
        except Exception as exc:
            print(f"SEND_ERROR pair={rt.pair_id} id={msg_id} "
                  f"{type(exc).__name__}", flush=True)
            failed = True
            break
        if res.get("ret") == -14 or res.get("errcode") == -14:
            rt.cancel_all_typing(send_typing_fn)
            _atomic_write_json(rt.state_dir / "session_expired.json",
                               {"status": "session_expired"})
            print(f"SESSION_EXPIRED_PAUSE pair={rt.pair_id}", flush=True)
            rt.paused.set()
            failed = True
            break
        if res.get("ret") not in (0, None):
            print(f"SEND_RET pair={rt.pair_id} id={msg_id} "
                  f"ret={res.get('ret')}", flush=True)
            failed = True
            break
        server_id = ""
        if isinstance(res, dict):
            for key in ("message_id", "msg_id", "svr_id"):
                if res.get(key):
                    server_id = str(res[key])
                    break
            if not server_id:
                for nest in ("data", "msg"):
                    sub = res.get(nest)
                    if isinstance(sub, dict) and sub.get("message_id"):
                        server_id = str(sub["message_id"])
                        break
        if server_id and chunk != "<media>":
            rt.cache_quote(server_id, chunk)
        elif chunk == "<media>":
            rt.cache_quote(msg_id,
                           out.get("caption", "") or out.get("reply", ""),
                           out.get("media_path", ""))
        with rt.lock:
            used += 1
            rt.sent_count[context_token] = used
        _atomic_write_json(rt.state_dir / "sent_count.json", rt.sent_count)
        sent_chunks.add(ci)
        out["sent_chunks"] = sorted(sent_chunks)
        try:
            _atomic_write_json(out_path, out)
        except Exception:
            pass
        rt.log_line("out", msg_id, res.get("ret"), len(chunk))
        time.sleep(1)
    if failed:
        return _fail("send_failed")
    out["sent_at"] = _now_beijing().isoformat()
    rt.ledger_mark(msg_id, "sent")
    _atomic_write_json(rt.queue_dir / "done" / out_path.name, out)
    out_path.unlink(missing_ok=True)
    print(f"SENT pair={rt.pair_id} id={msg_id} chunks={len(chunks)}",
          flush=True)
    rt.stop_typing(to_user, msg_id, send_typing_fn)
    return "sent"


def sender_thread(runtimes, stop_event, deps=None, dry_run=False,
                  runtimes_lock=None):
    """One thread for ALL pairs: round-robin over outbox dirs.
    A pair's files are never touched by another pair's pass: each
    pass is scoped to one runtime's own directories.
    runtimes is the LIVE dict; the snapshot is taken fresh each
    sweep so hot-added pairs are covered."""
    while not stop_event.is_set():
        did_work = False
        if runtimes_lock is not None:
            with runtimes_lock:
                current = list(runtimes.values())
        else:
            current = list(runtimes.values())
        for rt in current:
            if stop_event.is_set():
                break
            if rt.paused.is_set():
                continue
            for out_path in sorted(
                    (rt.queue_dir / "outbox").glob("*.json")):
                if stop_event.is_set():
                    break
                # The file may have been superseded/moved by an
                # earlier pass in this same sweep; re-check.
                if not out_path.exists():
                    continue
                send_one_outbox(rt, out_path, deps, dry_run)
                did_work = True
        time.sleep(1 if did_work else 2)
