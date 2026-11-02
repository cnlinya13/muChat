"""Poller pool for the gateway.

One poller thread owns a shard of active pairs (pair_id hash mod
pool size) and polls Tencent's official iLink getupdates for each
pair in turn, writing inbound messages to that pair's inbox queue.
Batch semantics mirror the live daemon exactly:

- Official-order cursor: the new cursor is committed immediately
  after a successful response, BEFORE processing messages.
- Two-phase commit: a leftover commit_pending.json is retried
  before the next poll; a corrupt one is quarantined.
- Poll audit records ids/seq/types/file names only, never
  content or tokens.
- Seen-id dedup, owner whitelist BEFORE any media download
  (unknown senders recorded and dropped), media download +
  decrypt, quote extraction with cache + partial resolution,
  verbatim inbox files, last_context, typing start, generic ack,
  seen snapshot persist.
- ret -14: cancel typing, write session_expired.json, alert,
  pause this pair 1 hour, then resume.
- Network errors: exponential backoff 5s -> 60s; poll_failed
  alert after 10 min of continuous failure.

Network functions are injectable (fetch_fn, download_fn,
get_config_fn) so unit tests run with canned data and never touch
Tencent. Defaults are the official-protocol helpers from
bridge/ilink_client.py.
"""

import hashlib
import json
import sys
import threading
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
    POLL_ALERT_AFTER_SECONDS,
    _atomic_write_json,
    _load_json,
    _now_beijing,
)


def _nth_index_of(text, value, occurrence, from_index=0):
    if not value or not isinstance(occurrence, int) or occurrence < 0:
        return -1
    pos = from_index
    for current in range(occurrence + 1):
        pos = text.find(value, pos)
        if pos < 0:
            return -1
        if current < occurrence:
            pos += len(value)
    return pos


def resolve_partial_quote(full_text, partial):
    """Vendored mirror of the live daemon's official partial-quote
    resolver (same MD5-gated candidate rule)."""
    if not full_text or not isinstance(partial, dict):
        return None
    start = partial.get("start") or ""
    end = partial.get("end") or ""
    if not start or not end:
        return None
    try:
        startindex = int(partial.get("startindex", 0))
        endindex = int(partial.get("endindex", 0))
    except (TypeError, ValueError):
        return None
    candidates = []
    s = _nth_index_of(full_text, start, startindex)
    if s >= 0:
        for mode in ("global", "relative"):
            if mode == "global":
                e = _nth_index_of(full_text, end, endindex)
            else:
                e = _nth_index_of(full_text, end, endindex, s + len(start))
            if e >= s:
                cand = full_text[s:e + len(end)]
                if cand and cand not in candidates:
                    candidates.append(cand)
    if not candidates:
        return None
    quotemd5 = (partial.get("quotemd5") or "").strip().lower()
    if not quotemd5:
        return candidates[0]
    for cand in candidates:
        if hashlib.md5(cand.encode("utf-8")).hexdigest() == quotemd5:
            return cand
    return None


def _default_fetch(token, baseurl, cursor):
    from ilink_client import getupdates
    return getupdates(token, baseurl, cursor)


def _default_download(media, aeskey):
    from ilink_client import download_media
    return download_media(media, aeskey)


def _default_get_config(token, baseurl, user_id, context_token):
    from ilink_client import get_config
    return get_config(token, baseurl, user_id, context_token)


def _default_send_typing(token, baseurl, user_id, ticket, status):
    from ilink_client import send_typing
    return send_typing(token, baseurl, user_id, ticket, status)


def process_batch(rt, data, fetch_deps=None):
    """Process ONE successful getupdates response for one pair.
    Returns the number of messages queued. Raises nothing for
    per-message problems (they are logged and skipped); the -14
    case is signalled by returning the string "session_expired".
    fetch_deps: dict with optional download_fn / get_config_fn /
    send_typing_fn overrides (tests)."""
    deps = fetch_deps or {}
    download_fn = deps.get("download_fn") or _default_download
    get_config_fn = deps.get("get_config_fn") or _default_get_config
    send_typing_fn = deps.get("send_typing_fn") or _default_send_typing

    ret, errcode = data.get("ret"), data.get("errcode")
    if ret == -14 or errcode == -14:
        rt.cancel_all_typing(send_typing_fn)
        _atomic_write_json(rt.state_dir / "session_expired.json",
                           {"status": "session_expired"})
        rt.alert_set("session_expired", "token_expired")
        print(f"SESSION_EXPIRED pair={rt.pair_id}", flush=True)
        return "session_expired"
    if ret not in (0, None):
        print(f"POLL_RET pair={rt.pair_id} ret={ret} errcode={errcode}",
              flush=True)
        return 0

    # Commit-pending retry (from a previous crashed cursor write).
    commit_pending = rt.state_dir / "commit_pending.json"
    if commit_pending.exists():
        try:
            pending = json.loads(commit_pending.read_text(encoding="utf-8"))
            _atomic_write_json(rt.state_dir / "cursor.json",
                               {"get_updates_buf": pending.get(
                                   "get_updates_buf", "")})
            with rt.lock:
                rt.cursor = pending.get("get_updates_buf", rt.cursor)
            commit_pending.unlink(missing_ok=True)
        except Exception:
            commit_pending.rename(
                rt.state_dir / f"commit_pending.corrupt-{int(time.time())}")
            print("COMMIT_PENDING_CORRUPT quarantined", flush=True)

    from ilink_client import extract_quote, extract_text

    new_cursor = data.get("get_updates_buf")
    cursor_before = rt.cursor
    msgs = data.get("msgs") or []
    # Official order: commit cursor BEFORE processing.
    if new_cursor and new_cursor != rt.cursor:
        try:
            _atomic_write_json(rt.state_dir / "cursor.json",
                               {"get_updates_buf": new_cursor})
            with rt.lock:
                rt.cursor = new_cursor
        except Exception:
            print("CURSOR_EARLY_COMMIT_FAILED", flush=True)
    if msgs:
        print("POLL_BATCH pair=%s count=%d cursor_changed=%s ids=%s" % (
            rt.pair_id, len(msgs),
            bool(new_cursor and new_cursor != cursor_before),
            ",".join("%s:%s:%s" % (
                str(m.get("message_id", "")), str(m.get("seq", "")),
                ",".join(str(i.get("type"))
                         for i in (m.get("item_list") or [])))
                for m in msgs)), flush=True)
        try:
            with open(rt.state_dir / "poll_audit.jsonl", "a",
                      encoding="utf-8") as af:
                af.write(json.dumps({
                    "ts": _now_beijing().isoformat(),
                    "count": len(msgs),
                    "cursor_changed": bool(
                        new_cursor and new_cursor != cursor_before),
                    "msgs": [{
                        "message_id": str(m.get("message_id", "")),
                        "seq": m.get("seq"),
                        "types": [i.get("type")
                                  for i in (m.get("item_list") or [])],
                        "file_names": [
                            (i.get("file_item") or {}).get("file_name", "")
                            for i in (m.get("item_list") or [])
                            if i.get("type") == 4],
                    } for m in msgs],
                }, ensure_ascii=False) + "\n")
        except Exception:
            print("POLL_AUDIT_WRITE_FAILED", flush=True)

    turn_start_inbox = {f.stem for f in
                        (rt.queue_dir / "inbox").glob("*.json")}
    turn_start_running_empty = not any(
        (rt.queue_dir / "running").glob("*.json"))
    queued_batch = []
    for msg in msgs:
        msg_id = str(msg.get("message_id", ""))
        if not msg_id:
            continue
        with rt.lock:
            if msg_id in rt.seen:
                continue
            rt.seen.add(msg_id)
        if not rt.is_owner_or_bind(msg.get("from_user_id") or ""):
            rt.note_unknown_sender(msg.get("from_user_id") or "")
            print(f"UNKNOWN_SENDER_DROPPED id={msg_id}", flush=True)
            continue
        rt.log_line("in", msg_id, ret,
                    len(json.dumps(msg, ensure_ascii=False)))
        text = extract_text(msg)
        voice_texts = []
        voice_playtime_ms = None
        for item in msg.get("item_list") or []:
            if item.get("type") == 3 and item.get("voice_item"):
                vt = (item["voice_item"].get("text") or "").strip()
                if vt:
                    voice_texts.append(vt)
                if item["voice_item"].get("playtime") is not None:
                    voice_playtime_ms = item["voice_item"].get("playtime")
        voice_text = " ".join(voice_texts)

        media_files = []
        for item in msg.get("item_list") or []:
            itype = item.get("type")
            if itype == 1:
                continue
            sub = None
            for key in ("image_item", "voice_item", "file_item",
                        "video_item"):
                if item.get(key):
                    sub = item[key]
                    break
            if not sub:
                media_files.append({"type": itype, "error": "no_sub_item"})
                continue
            media = sub.get("media") or {}
            entry = {"type": itype,
                     "mid_size": sub.get("mid_size"),
                     "thumb_size": sub.get("thumb_size"),
                     "file_name": sub.get("file_name"),
                     "encrypt_type": media.get("encrypt_type"),
                     "has_full_url": bool(media.get("full_url")),
                     "has_encrypt_param": bool(
                         media.get("encrypt_query_param"))}
            try:
                raw = download_fn(media, sub.get("aeskey"))
                if itype == 4 and sub.get("file_name"):
                    fn = str(sub["file_name"]).rsplit(".", 1)
                    ext = fn[1].lower() if len(fn) == 2 else ""
                    ext = "".join(c for c in ext if c.isalnum())[:10]
                    ext = ext or "bin"
                elif itype == 2:
                    ext = "png"
                elif itype == 3:
                    ext = "silk"
                elif itype == 5:
                    ext = "mp4"
                else:
                    ext = "bin"
                out_name = f"{msg_id}_{len(media_files)}.{ext}"
                out_path = rt.media_dir / "in" / out_name
                out_path.write_bytes(raw)
                entry["path"] = str(out_path)
                entry["size"] = len(raw)
                if itype == 5:
                    # Video thumb when the server supplies one
                    # (mirror of the live daemon); otherwise the
                    # worker extracts frames from the file itself.
                    entry["thumb_path"] = ""
                    thumb_media = sub.get("thumb_media")
                    if thumb_media:
                        try:
                            traw = download_fn(thumb_media,
                                               sub.get("aeskey"))
                            tfile = rt.media_dir / "in" / \
                                f"{msg_id}-5-thumb.png"
                            tfile.write_bytes(traw)
                            entry["thumb_path"] = str(tfile)
                            entry["thumb_size"] = len(traw)
                            entry["thumb_magic_ok"] = (
                                traw[:8] == b"\x89PNG\r\n\x1a\n"
                                or traw[:2] == b"\xff\xd8")
                        except Exception:
                            pass
            except Exception as exc:
                entry["error"] = type(exc).__name__
            media_files.append(entry)

        # Quote handling (same resolution order as the live daemon).
        quoted_id = quoted_text = quoted_partial_text = ""
        quoted_partial = None
        quoted_media_files = []
        quoted_resolved_from = ""
        try:
            q = extract_quote(msg)
        except Exception:
            q = None
        if q:
            # extract_quote shape (mirrors the live daemon):
            # {quoted_id, quoted_text, quoted_item, quoted_partial}.
            quoted_id = q.get("quoted_id") or ""
            quoted_text = q.get("quoted_text") or ""
            quoted_partial = q.get("quoted_partial")
            qitem = q.get("quoted_item") or {}
            qtype = qitem.get("type")
            if qtype in (2, 3, 4, 5):
                qsub = None
                for key in ("image_item", "voice_item", "file_item",
                            "video_item"):
                    if qitem.get(key):
                        qsub = qitem[key]
                        break
                qentry = {"type": qtype, "quoted": True}
                if qsub:
                    try:
                        qraw = download_fn(qsub.get("media") or {},
                                           qsub.get("aeskey"))
                        if qtype == 4 and qsub.get("file_name"):
                            fn = str(qsub["file_name"]).rsplit(".", 1)
                            qext = "".join(
                                c for c in (fn[1].lower() if len(fn) == 2
                                            else "") if c.isalnum())[:10]
                            qext = qext or "bin"
                        else:
                            qext = {2: "png", 3: "silk",
                                    4: "bin", 5: "mp4"}.get(qtype, "bin")
                        qpath = rt.media_dir / "in" / \
                            f"{msg_id}-quote-{qtype}.{qext}"
                        qpath.write_bytes(qraw)
                        qentry["path"] = str(qpath)
                        qentry["size"] = len(qraw)
                    except Exception as exc:
                        qentry["error"] = type(exc).__name__
                else:
                    qentry["error"] = "no_sub_item"
                quoted_media_files.append(qentry)
                if not quoted_text:
                    quoted_text = {2: "[图片]", 3: "[语音]",
                                   4: "[文件]", 5: "[视频]"}.get(qtype, "")
            if quoted_text:
                quoted_resolved_from = "inline"
            elif quoted_id:
                hit = rt.lookup_quote(quoted_id)
                if hit.get("text"):
                    quoted_text = hit["text"]
                    quoted_resolved_from = "cache"
                    if quoted_partial:
                        resolved = resolve_partial_quote(
                            quoted_text, quoted_partial)
                        if resolved:
                            quoted_partial_text = resolved
                else:
                    quoted_resolved_from = "unresolved"
        rt.cache_quote(msg_id, text or voice_text,
                       media_files[0].get("path") if media_files else "")
        for item in msg.get("item_list") or []:
            if item.get("msg_id"):
                rt.cache_quote(item["msg_id"], text or voice_text,
                               media_files[0].get("path")
                               if media_files else "")
        inbox = {
            "message_id": msg_id,
            "pair_id": rt.pair_id,
            "from_user_id": msg.get("from_user_id") or "",
            "context_token": msg.get("context_token") or "",
            "text": text,
            "voice_text": voice_text,
            "voice_playtime_ms": voice_playtime_ms,
            "kind": ("voice_text" if voice_text and not text.strip() else
                     "text" if text.strip() else
                     ("media" if media_files else "other")),
            "media_files": media_files,
            "quoted_id": quoted_id,
            "quoted_text": quoted_text,
            "quoted_partial": quoted_partial,
            "quoted_partial_text": quoted_partial_text,
            "quoted_media_files": quoted_media_files,
            "quoted_resolved_from": quoted_resolved_from,
            "received_at": _now_beijing().isoformat(),
        }
        _atomic_write_json(rt.queue_dir / "inbox" / f"{msg_id}.json", inbox)
        queued_batch.append((msg_id, inbox))
        _atomic_write_json(rt.state_dir / "last_context.json", {
            "pair_id": rt.pair_id,
            "from_user_id": inbox["from_user_id"],
            "context_token": inbox["context_token"],
            "message_id": msg_id,
            "received_at": inbox["received_at"],
        })
        rt.start_typing(msg.get("from_user_id") or "",
                        msg.get("context_token") or "", msg_id,
                        get_config_fn)
        with rt.lock:
            rt.inbox_version += 1
        rt.notify_event.set()

    try:
        rt.write_daemon_ack(queued_batch, turn_start_inbox,
                            turn_start_running_empty)
    except Exception:
        print("DAEMON_ACK_FAILED", flush=True)
    with rt.lock:
        seen_snapshot = sorted(rt.seen)[-5000:]
    _atomic_write_json(rt.state_dir / "seen_ids.json", seen_snapshot)
    return len(queued_batch)


def poller_thread(rt, stop_event, fetch_fn=None, deps=None,
                   pause_check_interval=10):
    """Poll loop for ONE pair. fetch_fn(token, baseurl, cursor)
    defaults to ilink getupdates. Never exits on a per-poll error."""
    fetch = fetch_fn or _default_fetch
    backoff = 5
    poll_fail_since = None
    while not stop_event.is_set():
        if rt.paused.is_set():
            time.sleep(pause_check_interval)
            continue
        rt.recover_stuck_workers()
        try:
            data = fetch(rt.token, rt.baseurl, rt.cursor)
        except Exception as exc:
            if poll_fail_since is None:
                poll_fail_since = time.time()
            elif time.time() - poll_fail_since >= POLL_ALERT_AFTER_SECONDS:
                rt.alert_set("poll_failed", type(exc).__name__)
            print(f"POLL_ERROR pair={rt.pair_id} {type(exc).__name__} "
                  f"backoff={backoff}", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        backoff = 5
        if poll_fail_since is not None:
            rt.alert_clear("poll_failed")
            poll_fail_since = None
        result = process_batch(rt, data, deps)
        if result == "session_expired":
            # 1-hour internal pause (chunked so stop still works).
            rt.paused.set()
            for _ in range(360):
                if stop_event.is_set():
                    break
                time.sleep(pause_check_interval)
            (rt.state_dir / "session_expired.json").unlink(missing_ok=True)
            rt.paused.clear()
            rt.alert_clear("session_expired")
            print(f"SESSION_EXPIRED_RESUME pair={rt.pair_id}", flush=True)
            continue
        # Empty poll: getupdates long-poll already waited server
        # side; loop at once (daemon behaviour) -- the HTTP timeout
        # in ilink getupdates bounds the rate.
