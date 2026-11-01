"""WeChat bridge daemon: echo (P1, legacy) and queue (P2, current).

Queue mode (P2 final plan, pairs/primary first):
  python3 bridge/daemon.py --mode queue --pair primary
  Pair state lives in pairs/<pair_id>/state|queue|logs, never in the
  legacy top-level state/queue/logs (those were migrated to
  pairs/primary/ on Tokyo at P2 switch-over).
  Four threads in one process, single iLink sender:
  - poll: getupdates long-poll, writes queue/inbox/*.json, updates
    cursor/seen, signals notify, starts typing for new messages.
    On -14 it does NOT exit: it cancels typing, writes the expired
    marker, pauses polling internally for 1 hour, then resumes.
  - notify: HTTP server bound ONLY to the pair's tailnet IP
    (primary: <BIND_HOST>, port <PAIR_PORT>), GET /inbox/wait?timeout=30,
    waits on the notify event and returns {"pair_id": ..., "new": [...]}
    with message IDs only -- never text, context_token, bot_token
    or user IDs in the response or logs.
  - sender: scans queue/outbox/ every 2s, chunks replies (900 chars),
    per-token budget 9, sends in order, moves to done/ or failed/.
    -2 is unclassified send failure, not auto-labelled quota.
  - typing: getconfig ticket per user (cached 24h, 0600), sendtyping
    status=1 every 5s while a message is queued/running, status=2
    when its outbox is sent or fails.
  Worker (Muse hook agent) moves inbox -> running -> done/failed
  and writes outbox; this daemon never executes tasks.

  2026-11-01 reliability batch (cf. gfydfhcf/wechat-ilink-muse-bridge):
  - send ledger: every send is recorded (message id + client id +
    status) before/after the call, so a daemon crash after the
    server accepted a message but before done/ does not cause a
    duplicate resend on restart; empty or "--"-prefixed reply text
    is rejected, never sent (their --key incident).
  - merge wait: the sender waits up to MERGE_WAIT_SECONDS after an
    outbox appears for a second outbox for the same user; if one
    arrives, the later reply supersedes the earlier one (earlier
    outbox is moved to done/ as superseded, not sent). This is
    the daemon-side half of consecutive-message merging; the
    worker-side half is a warm marker (see hook script): after
    replying, the worker's hook definition tells it to re-check
    the inbox once after a short wait instead of exiting at once,
    so a follow-up arriving within seconds is handled without a
    full cold start. A full always-on warm worker is NOT possible
    in the Muse hook model (a worker agent turn ends), so the
    gain is limited to that one re-check, not a persistent session.
  - poll two-phase commit: cursor/seen are advanced only after
    the batch's inbox files are atomically on disk; a
    commit-pending ledger retries a failed cursor write before
    the next poll, and a corrupt queue file is quarantined, never
    treated as empty.
  - alert loop: token expiry, poll failure >=10 min, and worker
    stuck >=10 min are written to alert_state.json; the notify
    endpoint surfaces them (ids/counts only) so the Muse hook can
    report them to the user, and recovery clears them.
  - owner whitelist: if owner_ids.json exists, only its
    from_user_ids are queued; others are recorded in
    unknown_senders.json and dropped before any media download.
    If the file is absent, the first inbound sender becomes the
    owner (bind window), matching the reference project's bind.
  - media prune: every 24h, media/in files older than 30 days
    are removed, then oldest-first until total <= 500MB. Queue,
    state and logs are never pruned.

  Logs record time, direction, message id, ret and byte counts
  only -- never tokens, tickets, or message text.
"""

import argparse
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, str(Path(__file__).resolve().parent))

from chunking import chunk_text  # noqa: E402
from ilink_client import (  # noqa: E402
    STATE_DIR as LEGACY_STATE_DIR,
    _atomic_write_json,
    extract_quote,
    extract_text,
    get_config,
    getupdates,
    send_text,
    send_typing,
)

MAX_PER_TOKEN = 9
TAILNET_IP = os.environ.get("MUCHAT_BIND_HOST", "127.0.0.1")
PAIR_PORTS = {"primary": 3810}
# Quote cache cap raised 2026-11-01 (was 500): a quote can only be
# resolved from this cache when the server sends an id-only ref
# (newer clients omit quoted text, see RefMessage comment in the
# official types: "Server message ID used when newer clients omit
# the quoted content"). 500 entries covered only the last few
# hours after a restart, so quoting anything older always missed.
QUOTE_CACHE_MAX = 5000
MERGE_WAIT_SECONDS = 5
# BURST_IDLE_SECONDS: formerly the gfydfhcf burst-collection idle
# window (MERGE_WINDOW=25s). UNUSED since the 2026-11-01 user decision
# to stay aligned with official cursor order: holding the cursor back
# for burst collection contradicts official order, so burst merging is
# done by the worker's pre-outbox supplement re-check instead.
BURST_IDLE_SECONDS = 25
MEDIA_PRUNE_MAX_AGE_DAYS = 30
MEDIA_PRUNE_MAX_BYTES = 500 * 1024 * 1024
POLL_ALERT_AFTER_SECONDS = 600
# Verbatim principle (2026-11-01, user decision): the daemon only
# transports bytes. The 2026-11-01 extraction + summary-reuse index
# were REMOVED: the worker reads originals itself, in parallel, and
# no intermediate interpretation layer remains. Daemon first-ack and
# text passthrough (notify texts) are transport, not interpretation,
# and stay.
WORKER_STUCK_AFTER_SECONDS = 600
ALERT_REPEAT_SECONDS = 3600


def _validate_reply_text(reply: str) -> str | None:
    """Outbox validation (reference --key incident guard).

    Returns an error string, or None when the reply may be sent.
    Empty replies and replies starting with "--" are never sent.
    """
    if reply is None:
        return "empty_reply"
    if not str(reply).strip():
        return "empty_reply"
    if str(reply).lstrip().startswith("--"):
        return "flag_like_reply"
    return None


def _send_ledger_load(ctx: "PairCtx") -> dict:
    return _load_json(ctx.state_dir / "send_ledger.json", {})


def _send_ledger_save(ctx: "PairCtx", ledger: dict) -> None:
    _atomic_write_json(ctx.state_dir / "send_ledger.json", ledger)


def _send_ledger_mark(ctx: "PairCtx", key: str, status: str, client_id: str = "") -> None:
    """Record a send attempt. 'sent' entries are never re-sent: if a
    previous run recorded 'sent' for the same outbox key, the sender
    skips it (server accepted, ack lost on crash) instead of
    duplicating the message to the user."""
    with ctx.lock:
        ledger = _send_ledger_load(ctx)
        ledger[key] = {"status": status, "client_id": client_id,
                       "at": _now_beijing().isoformat()}
        while len(ledger) > 5000:
            ledger.pop(next(iter(ledger)))
        _send_ledger_save(ctx, ledger)


def _send_ledger_status(ctx: "PairCtx", key: str) -> str:
    with ctx.lock:
        return _send_ledger_load(ctx).get(key, {}).get("status", "")


def _alert_set(ctx: "PairCtx", kind: str, detail: str = "") -> None:
    with ctx.lock:
        state = _load_json(ctx.state_dir / "alert_state.json", {})
        now = time.time()
        prev = state.get(kind, {})
        if prev.get("active") and now - prev.get("reported_at", 0) < ALERT_REPEAT_SECONDS:
            return
        state[kind] = {"active": True, "detail": detail,
                       "reported_at": now, "first_at": prev.get("first_at", now)}
        _atomic_write_json(ctx.state_dir / "alert_state.json", state)


def _alert_clear(ctx: "PairCtx", kind: str) -> None:
    with ctx.lock:
        state = _load_json(ctx.state_dir / "alert_state.json", {})
        if state.get(kind, {}).get("active"):
            state[kind] = {"active": False, "detail": "",
                           "reported_at": time.time(),
                           "first_at": state[kind].get("first_at", 0)}
            _atomic_write_json(ctx.state_dir / "alert_state.json", state)


def _alert_active(ctx: "PairCtx") -> dict:
    with ctx.lock:
        state = _load_json(ctx.state_dir / "alert_state.json", {})
    return {k: v for k, v in state.items() if v.get("active")}


def _owner_ids(ctx: "PairCtx") -> list:
    data = _load_json(ctx.state_dir / "owner_ids.json", {})
    if isinstance(data, dict):
        return data.get("owner_ids", []) or []
    return []


def _note_unknown_sender(ctx: "PairCtx", from_id: str) -> None:
    if not from_id:
        return
    with ctx.lock:
        data = _load_json(ctx.state_dir / "unknown_senders.json", {})
        entry = data.get(from_id, {"count": 0})
        entry["count"] = int(entry.get("count", 0)) + 1
        entry["last_at"] = _now_beijing().isoformat()
        data[from_id] = entry
        _atomic_write_json(ctx.state_dir / "unknown_senders.json", data)


def _is_owner_or_bind(ctx: "PairCtx", from_id: str) -> bool:
    """Whitelist check. No owner file yet: bind the first sender as
    owner (reference bind window, single-pair) and allow them. With
    a file: only listed ids pass; others are recorded as unknown."""
    if not from_id:
        return False
    owners = _owner_ids(ctx)
    if not owners:
        with ctx.lock:
            _atomic_write_json(ctx.state_dir / "owner_ids.json",
                               {"owner_ids": [from_id]})
        return True
    return from_id in owners


def _nth_index_of(text: str, value: str, occurrence: int, from_index: int = 0) -> int:
    if not value or not isinstance(occurrence, int) or occurrence < 0:
        return -1
    pos = from_index
    for current in range(occurrence + 1):
        pos = text.find(value, pos)
        if pos < 0:
            return -1
        if current < occurrence:
            pos += value.__len__()
    return pos


def resolve_partial_quote(full_text: str, partial: dict) -> str | None:
    """Mirror official partial-quote.ts resolvePartialQuote in Python.

    partial: {start, end, startindex, endindex, quotemd5}. Try the
    global and the relative end-index interpretations; when quotemd5
    is present, only a candidate whose MD5 matches counts. Returns
    the resolved substring, or None when it cannot be resolved.
    """
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
    import hashlib
    for cand in candidates:
        if hashlib.md5(cand.encode("utf-8")).hexdigest() == quotemd5:
            return cand
    return None


def _cache_quote_entry(ctx: "PairCtx", key: str, text: str, media_path: str = "") -> None:
    """Append one message to the pair's quote cache (thread-safe).

    The cache is the ONLY way to resolve an id-only quote: the
    server sends no content for it, so both inbound messages (poll
    loop) and outbound bot replies (sender loop, keyed by the server
    message_id returned by sendmessage) must be recorded here.
    Never touches credentials or tokens; only id -> text/media path.
    """
    if not key:
        return
    cache_path = ctx.state_dir / "quote_cache.json"
    with ctx.lock:
        cache = _load_json(cache_path, {})
        cache[key] = {"text": text, "media_path": media_path}
        while len(cache) > QUOTE_CACHE_MAX:
            cache.pop(next(iter(cache)))
        _atomic_write_json(cache_path, cache)


def _lookup_quote_cache(ctx: "PairCtx", quoted_id: str) -> dict:
    with ctx.lock:
        cache = _load_json(ctx.state_dir / "quote_cache.json", {})
    return cache.get(quoted_id) or {}


def _now_beijing():
    return datetime.now(timezone(timedelta(hours=8)))


def _load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default
    return default


class PairCtx:
    def __init__(self, pair_id: str):
        bridge_dir = Path(__file__).resolve().parent.parent
        self.pair_id = pair_id
        self.pair_dir = bridge_dir / "pairs" / pair_id
        self.state_dir = self.pair_dir / "state"
        self.queue_dir = self.pair_dir / "queue"
        self.log_dir = self.pair_dir / "logs"
        self.log_path = self.log_dir / "bridge.log"
        self.port = PAIR_PORTS.get(pair_id, 3810)
        for sub in ("inbox", "running", "done", "outbox", "failed"):
            (self.queue_dir / sub).mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        creds_path = self.state_dir / "credentials.json"
        if not creds_path.exists():
            raise SystemExit(f"NO_CREDENTIALS pair={pair_id} path={creds_path}")
        self.creds = json.loads(creds_path.read_text(encoding="utf-8"))
        self.token = self.creds["bot_token"]
        self.baseurl = self.creds["baseurl"]
        self.lock = threading.Lock()
        self.cursor = _load_json(self.state_dir / "cursor.json", {}).get(
            "get_updates_buf", "")
        self.seen = set(_load_json(self.state_dir / "seen_ids.json", []))
        self.sent_count = _load_json(self.state_dir / "sent_count.json", {})
        self.typing_tickets = _load_json(
            self.state_dir / "typing_tickets.json", {})
        # active_typing: user_id -> {"ticket": ..., "msg_ids": set(...)}
        self.active_typing = {}
        self.inbox_version = 0
        self.notify_event = threading.Event()
        self.paused = threading.Event()  # set while -14 pause is active
        self.stop = threading.Event()

    def log_line(self, direction: str, msg_id: str, ret, nbytes: int) -> None:
        ts = _now_beijing().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(f"{ts} {direction} id={msg_id} ret={ret} bytes={nbytes}\n")

    def inbox_ids(self):
        return sorted(p.stem for p in (self.queue_dir / "inbox").glob("*.json"))

    def start_typing(self, user_id: str, context_token: str, msg_id: str) -> None:
        if not user_id:
            return
        with self.lock:
            entry = self.active_typing.get(user_id)
            if entry is None:
                ticket_info = self.typing_tickets.get(user_id, {})
                ticket = ticket_info.get("ticket")
                fetched_at = ticket_info.get("fetched_at", 0)
                if not ticket or time.time() - fetched_at > 24 * 3600:
                    try:
                        cfg = get_config(
                            self.token, self.baseurl, user_id, context_token)
                    except Exception as exc:
                        print(f"TYPING_CONFIG_ERROR {type(exc).__name__}", flush=True)
                        return
                    if cfg.get("ret") not in (0, None) or not cfg.get("typing_ticket"):
                        print(f"TYPING_CONFIG_RET={cfg.get('ret')}", flush=True)
                        return
                    ticket = cfg["typing_ticket"]
                    self.typing_tickets[user_id] = {
                        "ticket": ticket, "fetched_at": time.time()}
                    _atomic_write_json(
                        self.state_dir / "typing_tickets.json", self.typing_tickets)
                entry = {"ticket": ticket, "msg_ids": set()}
                self.active_typing[user_id] = entry
            entry["msg_ids"].add(msg_id)

    def stop_typing(self, user_id: str, msg_id: str) -> None:
        if not user_id:
            return
        with self.lock:
            entry = self.active_typing.get(user_id)
            if not entry:
                return
            entry["msg_ids"].discard(msg_id)
            if entry["msg_ids"]:
                return
            ticket = entry["ticket"]
            del self.active_typing[user_id]
        try:
            res = send_typing(self.token, self.baseurl, user_id, ticket, 2)
            print(f"TYPING_CANCEL ret={res.get('ret')}", flush=True)
        except Exception as exc:
            print(f"TYPING_CANCEL_ERROR {type(exc).__name__}", flush=True)

    def cancel_all_typing(self) -> None:
        with self.lock:
            items = [(u, e["ticket"], set(e["msg_ids"]))
                     for u, e in self.active_typing.items()]
            self.active_typing.clear()
        for user_id, ticket, _msg_ids in items:
            try:
                send_typing(self.token, self.baseurl, user_id, ticket, 2)
            except Exception:
                pass


def _make_notify_handler(ctx: PairCtx):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # keep HTTP logs out of stdout
            pass

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path != "/inbox/wait":
                self.send_response(404)
                self.end_headers()
                return
            qs = parse_qs(parsed.query)
            try:
                timeout = float(qs.get("timeout", ["30"])[0])
            except ValueError:
                timeout = 30.0
            timeout = min(max(timeout, 0), 60)
            with ctx.lock:
                start_version = ctx.inbox_version
            ctx.notify_event.wait(timeout=timeout)
            # Clear only if no newer version arrived during wait.
            with ctx.lock:
                if ctx.inbox_version == start_version:
                    ctx.notify_event.clear()
                ids = ctx.inbox_ids()
            # Text passthrough (2026-11-01, user-approved, after the
            # 19:46 round's 52 s weather reply): for TEXT messages
            # with no media, include the verbatim text here so the
            # worker can answer without an SSH round-trip to fetch
            # the inbox file first. This travels only over the
            # tailnet notify channel to the hook on this same pair.
            # Media files are never inlined (the worker still
            # fetches those via SSH), and no token or ticket is
            # ever included. The worker must still move the inbox
            # file to running/ before writing its outbox, so the
            # queue state machine is unchanged.
            texts = {}
            for _mid in ids:
                try:
                    _ib = json.loads(
                        (ctx.queue_dir / "inbox" / f"{_mid}.json").read_text(
                            encoding="utf-8"))
                except Exception:
                    continue
                if (_ib.get("kind") == "text" and not _ib.get("media_files")
                        and _ib.get("text")):
                    texts[_mid] = _ib["text"]
            body = json.dumps(
                {"pair_id": ctx.pair_id, "new": ids, "texts": texts,
                 "alerts": sorted(_alert_active(ctx).keys())},
                ensure_ascii=False).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
    return Handler


def _recover_stuck_workers(ctx: PairCtx) -> None:
    """Worker-stuck recovery (reference 10-min rule): a running file
    untouched for >=10 min is moved back to inbox so it is not lost
    when a worker dies, and an alert is set (hourly repeat at most).
    The recovered file's mtime is refreshed so it is not
    immediately re-flagged on the next pass."""
    now = time.time()
    for path in (ctx.queue_dir / "running").glob("*.json"):
        try:
            age = now - path.stat().st_mtime
        except Exception:
            continue
        if age < WORKER_STUCK_AFTER_SECONDS:
            continue
        _alert_set(ctx, "worker_stuck", path.stem)
        try:
            dest = ctx.queue_dir / "inbox" / path.name
            path.rename(dest)
            import os as _os
            _os.utime(dest, None)
            with ctx.lock:
                ctx.inbox_version += 1
            ctx.notify_event.set()
            print(f"WORKER_STUCK_RECOVERED id={path.stem}", flush=True)
        except Exception:
            pass


def _write_daemon_ack(ctx, _queued_batch, _turn_start_inbox_stems, _turn_start_running_empty):
    """Daemon first-ack for one queued batch. NEVER raises:
    any failure is logged and the caller (poll_loop) keeps
    polling -- an ack bug must never kill the poll thread
    again (2026-11-01 incident: a tuple/dict mix-up in the
    ack code crashed poll_loop and stopped all polling).
    The ack block below is wrapped by its caller too."""
    try:
        # Gate uses the TURN-START snapshot passed in by the
        # caller (see poll_loop), NOT the inbox/running state at
        # ack time: files arriving during the wait or during the
        # download window belong to this turn and must NOT
        # suppress the ack.
        _batch_has_files = any(
            _me.get("file_name")
            for _mid, _ib in _queued_batch
            for _me in (_ib.get("media_files") or []))
        # Wait only when the batch is text-only (2 s); a batch
        # that already has files needs no wait at all.
        if not _batch_has_files:
            time.sleep(2)
        _ack_names = []
        for _mid, _ib in _queued_batch:
            for _me in _ib.get("media_files") or []:
                if _me.get("file_name"):
                    _ack_names.append(_me["file_name"])
        # Re-scan inbox for files that arrived during the wait
        # (same turn, queued after this batch was built).
        try:
            for _f in sorted((ctx.queue_dir / "inbox").glob("*.json")):
                if _f.stem in {m for m, _ in _queued_batch}:
                    continue
                _ib2 = json.loads(_f.read_text(encoding="utf-8"))
                for _me in _ib2.get("media_files") or []:
                    if _me.get("file_name"):
                        _ack_names.append(_me["file_name"])
        except Exception:
            pass
        try:
            _inbox_empty_before = not any(
                (ctx.queue_dir / "inbox").glob("*.json"))
            # Gate: this batch started a new turn only if, at
            # turn start, inbox was empty and running was empty.
            # Any pre-existing inbox file means an older turn was
            # still pending -> suppress (one ack per turn).
            if not _turn_start_inbox_stems and _turn_start_running_empty:
                _first_id, _first_inbox = _queued_batch[0]
                _fnames = _ack_names
                if _fnames:
                    _fnames = list(dict.fromkeys(_fnames))
                    _listed = "、".join(_fnames[:3])
                    if len(_fnames) > 3:
                        _listed += "等%d个文件" % len(_fnames)
                    _ack_reply = "收到%s，正在阅读，稍后发汇总。" % _listed
                else:
                    _ack_reply = "收到消息，正在处理，稍后回复。"
                _atomic_write_json(
                    ctx.queue_dir / "outbox" / f"{_first_id}-ack.json", {
                        "message_id": f"{_first_id}-ack",
                        "pair_id": ctx.pair_id,
                        "from_user_id": _first_inbox.get("from_user_id", ""),
                        "context_token": _first_inbox.get("context_token", ""),
                        "reply": _ack_reply,
                        "daemon_ack": True,
                        "skip_merge_wait": True,
                    })
        except Exception:
            print("DAEMON_ACK_FAILED", flush=True)
    except Exception:
        print("DAEMON_ACK_FAILED", flush=True)


def poll_loop(ctx: PairCtx) -> None:
    backoff = 5
    poll_fail_since = None
    while not ctx.stop.is_set():
        if ctx.paused.is_set():
            time.sleep(10)
            continue
        _recover_stuck_workers(ctx)
        try:
            data = getupdates(ctx.token, ctx.baseurl, ctx.cursor)
        except Exception as exc:
            if poll_fail_since is None:
                poll_fail_since = time.time()
            elif time.time() - poll_fail_since >= POLL_ALERT_AFTER_SECONDS:
                _alert_set(ctx, "poll_failed", type(exc).__name__)
            print(f"POLL_ERROR {type(exc).__name__} backoff={backoff}", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        backoff = 5
        if poll_fail_since is not None:
            _alert_clear(ctx, "poll_failed")
            poll_fail_since = None
        ret, errcode = data.get("ret"), data.get("errcode")
        if ret == -14 or errcode == -14:
            ctx.cancel_all_typing()
            _atomic_write_json(ctx.state_dir / "session_expired.json",
                               {"status": "session_expired"})
            _alert_set(ctx, "session_expired", "token_expired")
            print("SESSION_EXPIRED_PAUSE_3600", flush=True)
            ctx.paused.set()
            time.sleep(3600)
            (ctx.state_dir / "session_expired.json").unlink(missing_ok=True)
            ctx.paused.clear()
            _alert_clear(ctx, "session_expired")
            print("SESSION_EXPIRED_RESUME", flush=True)
            continue
        if ret not in (0, None):
            print(f"POLL_RET={ret} errcode={errcode}", flush=True)
            time.sleep(5)
            continue
        # Two-phase commit: persist the batch's inbox files FIRST,
        # then advance cursor/seen. A crash between fetch and
        # persist re-fetches the same batch next poll (dedup by
        # seen ids); a failed cursor write leaves a commit-pending
        # ledger that is retried before the next poll, never lost.
        _commit_pending = ctx.state_dir / "commit_pending.json"
        if _commit_pending.exists():
            try:
                pending = json.loads(_commit_pending.read_text(encoding="utf-8"))
                _atomic_write_json(ctx.state_dir / "cursor.json",
                                   {"get_updates_buf": pending.get("get_updates_buf", "")})
                with ctx.lock:
                    ctx.cursor = pending.get("get_updates_buf", ctx.cursor)
                _commit_pending.unlink(missing_ok=True)
            except Exception:
                _commit_pending.rename(
                    ctx.state_dir / f"commit_pending.corrupt-{int(time.time())}")
                print("COMMIT_PENDING_CORRUPT quarantined", flush=True)
        new_cursor = data.get("get_updates_buf")
        _cursor_before = ctx.cursor
        _pending_cursor = None
        # Official-order cursor sync (2026-11-01, monitor.ts, user
        # decision 2026-11-01: stay aligned with official): persist
        # the new cursor immediately after a successful response,
        # BEFORE processing messages. Trade-off, stated plainly: a
        # crash mid-batch after this commit loses the not-yet-
        # processed messages in that batch (cursor already passed
        # them). That is the same trade-off official monitor.ts
        # makes. The gfydfhcf handle-then-commit + burst-idle
        # collection was tried 2026-11-01 and reverted by user
        # decision in favour of official order; burst merging is
        # therefore done by the worker's pre-outbox supplement
        # re-check, not by holding the cursor back.
        _msgs_pre = data.get("msgs") or []
        if new_cursor and new_cursor != ctx.cursor:
            try:
                _atomic_write_json(ctx.state_dir / "cursor.json",
                                   {"get_updates_buf": new_cursor})
                with ctx.lock:
                    ctx.cursor = new_cursor
            except Exception:
                print("CURSOR_EARLY_COMMIT_FAILED", flush=True)
        # Poll audit (2026-11-01, burst-loss investigation): log the raw
        # batch BEFORE any filtering, so a message that never reaches
        # inbox can be attributed to the server batch or to a daemon
        # filter. Only ids/seq/types are logged, never content/tokens.
        _msgs = data.get("msgs") or []
        if _msgs:
            print("POLL_BATCH count=%d cursor_changed=%s ids=%s" % (
                len(_msgs),
                bool(new_cursor and new_cursor != _cursor_before),
                ",".join("%s:%s:%s" % (
                    str(m.get("message_id", "")),
                    str(m.get("seq", "")),
                    ",".join(str(i.get("type")) for i in (m.get("item_list") or [])))
                    for m in _msgs)), flush=True)
            try:
                with open(ctx.state_dir / "poll_audit.jsonl", "a", encoding="utf-8") as _af:
                    _af.write(json.dumps({
                        "ts": _now_beijing().isoformat(),
                        "count": len(_msgs),
                        "cursor_changed": bool(new_cursor and new_cursor != _cursor_before),
                        "msgs": [{
                            "message_id": str(m.get("message_id", "")),
                            "seq": m.get("seq"),
                            "types": [i.get("type") for i in (m.get("item_list") or [])],
                            "file_names": [ (i.get("file_item") or {}).get("file_name", "")
                                            for i in (m.get("item_list") or [])
                                            if i.get("type") == 4 ],
                        } for m in _msgs],
                    }, ensure_ascii=False) + "\n")
            except Exception:
                print("POLL_AUDIT_WRITE_FAILED", flush=True)
        _queued_batch = []
        # Turn-start snapshot for the ack gate (2026-11-01 round 2
        # fix): taken BEFORE this batch queues anything. The ack
        # block runs after all downloads, by which time the worker
        # may already have moved early messages to running/ -- that
        # must NOT suppress this turn's ack. A non-empty snapshot
        # means an older turn was still pending when this batch
        # started, and only then is the ack suppressed.
        _turn_start_inbox_stems = {f.stem for f in (ctx.queue_dir / "inbox").glob("*.json")}
        _turn_start_running_empty = not any((ctx.queue_dir / "running").glob("*.json"))
        for msg in _msgs:
            msg_id = str(msg.get("message_id", ""))
            if not msg_id:
                print("POLL_SKIP_EMPTY_ID seq=%s types=%s" % (
                    str(msg.get("seq", "")),
                    ",".join(str(i.get("type")) for i in (msg.get("item_list") or []))), flush=True)
                continue
            with ctx.lock:
                if msg_id in ctx.seen:
                    continue
                ctx.seen.add(msg_id)
            # Owner whitelist: unknown senders are recorded and
            # dropped BEFORE any media download or inbox write.
            if not _is_owner_or_bind(ctx, msg.get("from_user_id") or ""):
                _note_unknown_sender(ctx, msg.get("from_user_id") or "")
                print(f"UNKNOWN_SENDER_DROPPED id={msg_id}", flush=True)
                continue
            ctx.log_line("in", msg_id, ret, len(json.dumps(msg, ensure_ascii=False)))
            text = extract_text(msg)
            # Voice transcription (2026-11-01 fix): extract_text only
            # concatenates type-1 text, so a voice message's server
            # transcription (voice_item.text, when present) was lost.
            # Keep it as its own field; the worker must use it as the
            # voice content and say it is a transcription when it does.
            # If no transcription is present, voice_text stays "" and
            # the worker must say the voice cannot be understood (SILK
            # audio has no local transcription tool), not guess it.
            voice_texts = []
            voice_playtime_ms = None
            for _item in msg.get("item_list") or []:
                if _item.get("type") == 3 and _item.get("voice_item"):
                    _vt = (_item["voice_item"].get("text") or "").strip()
                    if _vt:
                        voice_texts.append(_vt)
                    if _item["voice_item"].get("playtime") is not None:
                        voice_playtime_ms = _item["voice_item"].get("playtime")
            voice_text = " ".join(voice_texts)
            # P3 inbound media: capture non-text items, download + decrypt
            # via CDN (pair-local, 0600 inbox file only, never logged).
            # If download fails, the item metadata is still saved so the
            # worker can report the failure instead of claiming no media.
            media_files = []
            for _item_idx, item in enumerate(msg.get("item_list") or []):
                itype = item.get("type")
                if itype == 1:
                    continue
                sub = None
                for key in ("image_item", "voice_item", "file_item", "video_item"):
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
                         "has_encrypt_param": bool(media.get("encrypt_query_param"))}
                try:
                    from ilink_client import download_media as _dl
                    raw = _dl(media, sub.get("aeskey"))
                    # File extension (2026-11-01 fix): previously every
                    # file item was saved as .bin, losing its real type
                    # so the worker could not open PDFs/Word/text files
                    # by extension. Use file_name's extension when present
                    # (sanitized to alphanumerics, max 10 chars); fall back
                    # to the old mapping otherwise.
                    if itype == 4 and sub.get("file_name"):
                        _fn = str(sub["file_name"]).rsplit(".", 1)
                        _ext = _fn[1].lower() if len(_fn) == 2 else ""
                        _ext = "".join(c for c in _ext if c.isalnum())[:10]
                        ext = _ext or "bin"
                    else:
                        ext = {2: "png", 3: "silk", 4: "bin", 5: "mp4"}.get(itype, "bin")
                    media_dir = ctx.pair_dir / "media" / "in"
                    media_dir.mkdir(parents=True, exist_ok=True)
                    out_file = media_dir / f"{msg_id}-{itype}-{_item_idx}.{ext}"
                    out_file.write_bytes(raw)
                    entry["path"] = str(out_file)
                    entry["size"] = len(raw)
                    # Magic check (2026-11-01 fix): a wrong-key ECB
                    # decrypt used to be saved as if it were the real
                    # file, so the worker later said "cannot open"
                    # without knowing why. Record whether the bytes
                    # match the extension's magic so the worker can
                    # tell the user the download/decrypt failed instead
                    # of guessing at the content.
                    _magic = {
                        "pdf": raw[:4] == b"%PDF",
                        "png": raw[:8] == b"\x89PNG\r\n\x1a\n",
                        "jpg": raw[:2] == b"\xff\xd8",
                        "jpeg": raw[:2] == b"\xff\xd8",
                        "mp4": len(raw) > 8 and raw[4:8] == b"ftyp",
                        "docx": raw[:2] == b"PK",
                        "xlsx": raw[:2] == b"PK",
                        "pptx": raw[:2] == b"PK",
                        "zip": raw[:2] == b"PK",
                    }
                    if ext in _magic:
                        entry["magic_ok"] = _magic[ext]
                    elif ext in ("txt", "md", "json", "xml", "csv", "log"):
                        try:
                            raw.decode("utf-8")
                            entry["magic_ok"] = True
                        except Exception:
                            entry["magic_ok"] = False
                    # Video thumbnail (2026-11-01 fix): when a video item
                    # carries thumb_media, download it as an image too so
                    # the worker can at least view the first-frame thumb
                    # (full video viewing is not available to the worker).
                    if itype == 5 and sub.get("thumb_media"):
                        try:
                            _traw = _dl(sub["thumb_media"], sub.get("aeskey"))
                            _tfile = media_dir / f"{msg_id}-5-thumb.png"
                            _tfile.write_bytes(_traw)
                            entry["thumb_path"] = str(_tfile)
                            entry["thumb_size"] = len(_traw)
                            entry["thumb_magic_ok"] = (
                                _traw[:8] == b"\x89PNG\r\n\x1a\n"
                                or _traw[:2] == b"\xff\xd8")
                        except Exception:
                            pass
                except Exception as exc:
                    entry["error"] = type(exc).__name__
                media_files.append(entry)
            # Quote (引用) handling, 2026-11-01: previously extract_text
            # only concatenated type-1 text, so a quoted message's
            # ref_msg was silently dropped and the worker saw only the
            # new sentence. Mirror the official plugin (inbound.ts):
            # the quoting item carries ref_msg {svr_id, title,
            # message_item, partial_text}; its inline body is the
            # quoted content. If the server sent only an id, fall
            # back to the local inbound quote cache below; if that
            # also misses, quoted_text stays "" and the worker must
            # say so instead of guessing the quoted content.
            quote = extract_quote(msg)
            quoted_id = quoted_text = ""
            quoted_partial = None
            quoted_partial_text = ""
            quoted_media_files = []
            quoted_resolved_from = ""
            if quote:
                quoted_id = quote["quoted_id"]
                quoted_text = quote["quoted_text"]
                quoted_partial = quote["quoted_partial"]
                qitem = quote["quoted_item"] or {}
                qtype = qitem.get("type")
                if qtype in (2, 3, 4, 5):
                    qsub = None
                    for key in ("image_item", "voice_item", "file_item", "video_item"):
                        if qitem.get(key):
                            qsub = qitem[key]
                            break
                    qentry = {"type": qtype, "quoted": True}
                    if qsub:
                        try:
                            from ilink_client import download_media as _dl
                            qraw = _dl(qsub.get("media") or {}, qsub.get("aeskey"))
                            if qtype == 4 and qsub.get("file_name"):
                                _qfn = str(qsub["file_name"]).rsplit(".", 1)
                                _qext = _qfn[1].lower() if len(_qfn) == 2 else ""
                                _qext = "".join(c for c in _qext if c.isalnum())[:10]
                                qext = _qext or "bin"
                            else:
                                qext = {2: "png", 3: "silk", 4: "bin", 5: "mp4"}.get(qtype, "bin")
                            qdir = ctx.pair_dir / "media" / "in"
                            qdir.mkdir(parents=True, exist_ok=True)
                            qfile = qdir / f"{msg_id}-quote-{qtype}-{len(quoted_media_files)}.{qext}"
                            qfile.write_bytes(qraw)
                            qentry["path"] = str(qfile)
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
                    hit = _lookup_quote_cache(ctx, quoted_id)
                    if hit.get("text"):
                        quoted_text = hit["text"]
                        quoted_resolved_from = "cache"
                        if quoted_partial:
                            resolved = resolve_partial_quote(quoted_text, quoted_partial)
                            if resolved:
                                quoted_partial_text = resolved
                    else:
                        quoted_resolved_from = "unresolved"
            # Inbound quote cache (text + first media path only, no
            # tokens): lets a later id-only quote of an earlier
            # inbound message resolve. Outbound (bot) replies are
            # cached separately by the sender loop (see below), so
            # quoting a bot reply no longer depends on the server
            # sending inline content.
            _cache_quote_entry(
                ctx, msg_id, text or voice_text,
                media_files[0].get("path") if media_files else "")
            for item in msg.get("item_list") or []:
                if item.get("msg_id"):
                    _cache_quote_entry(ctx, item["msg_id"], text or voice_text,
                                       media_files[0].get("path") if media_files else "")
            inbox = {
                "message_id": msg_id,
                "pair_id": ctx.pair_id,
                "from_user_id": msg.get("from_user_id") or "",
                "context_token": msg.get("context_token") or "",
                "text": text,
                "voice_text": voice_text,
                "voice_playtime_ms": voice_playtime_ms,
                "kind": ("voice_text" if voice_text and not text.strip() else
                         "text" if text.strip() else ("media" if media_files else "other")),
                "media_files": media_files,
                "quoted_id": quoted_id,
                "quoted_text": quoted_text,
                "quoted_partial": quoted_partial,
                "quoted_partial_text": quoted_partial_text,
                "quoted_media_files": quoted_media_files,
                "quoted_resolved_from": quoted_resolved_from,
                "received_at": _now_beijing().isoformat(),
            }
            _atomic_write_json(ctx.queue_dir / "inbox" / f"{msg_id}.json", inbox)
            _queued_batch.append((msg_id, inbox))
            # P4 readiness (not used by P2): remember the most recent
            # context_token per pair, so a future proactive task can
            # check the ~24h window before writing an outbox. P2 never
            # sends using last_context automatically.
            _atomic_write_json(ctx.state_dir / "last_context.json", {
                "pair_id": ctx.pair_id,
                "from_user_id": inbox["from_user_id"],
                "context_token": inbox["context_token"],
                "message_id": msg_id,
                "received_at": inbox["received_at"],
            })
            ctx.start_typing(msg.get("from_user_id") or "",
                             msg.get("context_token") or "", msg_id)
            with ctx.lock:
                ctx.inbox_version += 1
            ctx.notify_event.set()
        # Speed-up (2026-11-01, ack timing revised twice by user
        # decision): the ack lists filenames when files are known.
        # Rule (2026-11-01, after the 19:46 text-only round, which
        # paid a full 5 s wait for files that never came): if this
        # batch already contains files, send the ack AT ONCE -- no
        # wait, the list is already complete enough. Only a
        # text-only first batch waits, and only 2 s (not 5 s), for
        # files arriving just after the text; if none arrive, the
        # generic fallback text is correct, not a flaw. Trade-off,
        # user-set: a file arriving more than 2 s after a text-only
        # batch misses the ack list but is still covered by the
        # worker's supplement merge into the same summary. Only
        # when this batch starts a new turn (inbox AND running
        # were both empty before it), so a burst split across
        # polls does not ack twice. The ack outbox uses a distinct
        # -ack id so the sender ledger (keyed by message_id) never
        # confuses it with the worker's later summary outbox, and
        # skip_merge_wait so the sender does not hold it for a
        # later outbox. Cost, stated plainly: the ack consumes 1
        # unit of the per-token budget (9), leaving 8 for the reply.
        try:
            _write_daemon_ack(ctx, _queued_batch, _turn_start_inbox_stems, _turn_start_running_empty)
        except Exception:
            print("DAEMON_ACK_FAILED", flush=True)
        # End of batch: cursor was already committed at the top of
        # this batch (official order). Persist the seen snapshot
        # only, so a later replay is still filtered.
        with ctx.lock:
            _seen_snapshot = sorted(ctx.seen)[-5000:]
        _atomic_write_json(ctx.state_dir / "seen_ids.json", _seen_snapshot)

def sender_loop(ctx: PairCtx) -> None:
    while not ctx.stop.is_set():
        if ctx.paused.is_set():
            time.sleep(10)
            continue
        for out_path in sorted((ctx.queue_dir / "outbox").glob("*.json")):
            try:
                out = json.loads(out_path.read_text(encoding="utf-8"))
            except Exception:
                out_path.rename(ctx.queue_dir / "failed" / out_path.name)
                continue
            msg_id = out.get("message_id", out_path.stem)
            to_user = out.get("from_user_id", "")
            context_token = out.get("context_token", "")
            reply = out.get("reply", "")
            if out.get("pair_id") and out["pair_id"] != ctx.pair_id:
                out_path.rename(ctx.queue_dir / "failed" / out_path.name)
                print(f"PAIR_MISMATCH id={msg_id}", flush=True)
                continue
            if not to_user or not context_token or (not reply and not (out.get("kind") in ("image", "image_plain") and out.get("media_path"))):
                out["error"] = "missing_route_or_reply"
                _atomic_write_json(ctx.queue_dir / "failed" / out_path.name, out)
                out_path.unlink(missing_ok=True)
                ctx.stop_typing(to_user, msg_id)
                continue
            # Outbox validation: never send an empty or flag-like
            # reply (reference --key incident). Image outboxes have
            # no text reply, so they skip this check.
            if out.get("kind") not in ("image", "image_plain"):
                _verr = _validate_reply_text(reply)
                if _verr:
                    out["error"] = _verr
                    _atomic_write_json(ctx.queue_dir / "failed" / out_path.name, out)
                    out_path.unlink(missing_ok=True)
                    ctx.stop_typing(to_user, msg_id)
                    continue
            # Send ledger: a previous run that already recorded
            # 'sent' for this outbox must not resend it (server
            # accepted, ack lost on crash -> duplicate to user).
            if _send_ledger_status(ctx, msg_id) == "sent":
                out["sent_at"] = _now_beijing().isoformat()
                out["note"] = "skipped_duplicate_ledger_sent"
                _atomic_write_json(ctx.queue_dir / "done" / out_path.name, out)
                out_path.unlink(missing_ok=True)
                ctx.stop_typing(to_user, msg_id)
                continue
            # Daemon acks skip the merge wait: they are meant to
            # arrive at once, not be held for a later outbox.
            _skip_merge = bool(out.get("skip_merge_wait"))
            # Merge wait: if the worker writes a second outbox for
            # the same user within MERGE_WAIT_SECONDS, the later
            # reply supersedes this one (this outbox is marked
            # superseded, not sent), so consecutive messages get
            # one reply to the latest context, not a stale reply
            # followed by a second reply. Single messages only
            # wait the 5s window, then send normally.
            _merge_deadline = time.time() + MERGE_WAIT_SECONDS
            _superseded = False
            while not _skip_merge and time.time() < _merge_deadline:
                _later = [p for p in sorted((ctx.queue_dir / "outbox").glob("*.json"))
                          if p != out_path and p.stem != msg_id]
                if _later:
                    _superseded = True
                    break
                time.sleep(0.5)
            if _superseded:
                out["note"] = "superseded_by_later_outbox"
                _atomic_write_json(ctx.queue_dir / "done" / out_path.name, out)
                out_path.unlink(missing_ok=True)
                ctx.stop_typing(to_user, msg_id)
                continue
            chunks = chunk_text(reply) if reply else []
            # P3 media outbox: kind=image + media_path (pair-local path on
            # Tokyo, e.g. pairs/<pair>/media/out/<file>) bypasses text
            # chunking and counts as 1 budget unit.
            if out.get("kind") in ("image", "image_plain") and out.get("media_path"):
                chunks = ["<media>"]
            with ctx.lock:
                used = int(ctx.sent_count.get(context_token, 0))
            if len(chunks) > MAX_PER_TOKEN - used:
                out["error"] = "over_token_budget"
                _atomic_write_json(ctx.queue_dir / "failed" / out_path.name, out)
                out_path.unlink(missing_ok=True)
                print(f"OVER_BUDGET id={msg_id} chunks={len(chunks)} used={used}", flush=True)
                ctx.stop_typing(to_user, msg_id)
                continue
            failed = False
            _sent_chunks = set(out.get("sent_chunks", []) or [])
            for _ci, chunk in enumerate(chunks):
                if _ci in _sent_chunks:
                    continue
                if ctx.paused.is_set():
                    failed = True
                    break
                try:
                    if chunk == "<media>":
                        if out.get("kind") == "image_plain":
                            from ilink_client import send_image_plain as _send_plain
                            _res = _send_plain(ctx.token, ctx.baseurl, to_user,
                                               context_token, out["media_path"])
                            res = _res.get("send", {}) if isinstance(_res, dict) else {}
                        else:
                            # Official-code image send (2026-11-01, verified openable):
                            # daemon writes a 0600 request file (token inside, never
                            # logged, deleted after the run) and calls the compiled
                            # official openclaw-weixin driver with Node 22. The
                            # custom Python send_image path is no longer used.
                            import subprocess as _sp
                            _req_path = ctx.state_dir / f"send_req_{msg_id}.json"
                            _atomic_write_json(_req_path, {
                                "baseUrl": ctx.baseurl, "token": ctx.token,
                                "to": to_user, "contextToken": context_token,
                                "filePath": out["media_path"],
                                "caption": out.get("caption", "") or out.get("reply", ""),
                            })
                            try:
                                _run = _sp.run(
                                    [os.environ.get("MUCHAT_NODE_BIN", "node"),
                                     os.environ.get("MUCHAT_SEND_DRIVER", "official-send-dist/driver.mjs"),
                                     str(_req_path)],
                                    capture_output=True, text=True, timeout=60)
                                _out = json.loads(_run.stdout.strip().splitlines()[-1]) \
                                    if _run.stdout.strip() else {}
                                res = {"ret": 0} if _out.get("ok") else {"ret": 1}
                            finally:
                                _req_path.unlink(missing_ok=True)
                    else:
                        res = send_text(ctx.token, ctx.baseurl, to_user,
                                        context_token, chunk)
                except Exception as exc:
                    print(f"SEND_ERROR id={msg_id} {type(exc).__name__}", flush=True)
                    failed = True
                    break
                if res.get("ret") == -14 or res.get("errcode") == -14:
                    ctx.cancel_all_typing()
                    _atomic_write_json(ctx.state_dir / "session_expired.json",
                                       {"status": "session_expired"})
                    print("SESSION_EXPIRED_PAUSE_3600", flush=True)
                    ctx.paused.set()
                    failed = True
                    break
                if res.get("ret") not in (0, None):
                    print(f"SEND_RET id={msg_id} ret={res.get('ret')}", flush=True)
                    failed = True
                    break
                # Outbound quote cache (fix 2026-11-01): record each
                # successfully sent chunk under the server message_id
                # returned by sendmessage, so a later id-only quote
                # of a bot reply resolves from cache instead of
                # failing. Without this, only inbound messages were
                # cached and quoting any bot reply was unresolvable.
                # Image sends via the official driver return only an
                # ok flag (no server id in daemon's res), so they
                # are not cached here; quoting a bot image still
                # relies on the server's inline media item, as before.
                server_id = ""
                if isinstance(res, dict):
                    for _key in ("message_id", "msg_id", "svr_id"):
                        if res.get(_key):
                            server_id = str(res[_key])
                            break
                    if not server_id:
                        for _nest in ("data", "msg"):
                            _sub = res.get(_nest)
                            if isinstance(_sub, dict) and _sub.get("message_id"):
                                server_id = str(_sub["message_id"])
                                break
                if server_id and chunk != "<media>":
                    _cache_quote_entry(ctx, server_id, chunk)
                elif chunk == "<media>":
                    # Image outbox quote cache (batch 2, best effort):
                    # the official driver returns only ok, no server
                    # id, so cache the image under the outbox id and
                    # caption so a quote of it at least resolves to
                    # the caption text instead of unresolved. A quote
                    # by server id still relies on inline media.
                    _cache_quote_entry(ctx, msg_id,
                                       out.get("caption", "") or out.get("reply", ""),
                                       out.get("media_path", ""))
                with ctx.lock:
                    used += 1
                    ctx.sent_count[context_token] = used
                _atomic_write_json(ctx.state_dir / "sent_count.json", ctx.sent_count)
                _sent_chunks.add(_ci)
                out["sent_chunks"] = sorted(_sent_chunks)
                try:
                    _atomic_write_json(out_path, out)
                except Exception:
                    pass
                ctx.log_line("out", msg_id, res.get("ret"), len(chunk))
                time.sleep(1)
            if failed:
                out["error"] = "send_failed"
                _atomic_write_json(ctx.queue_dir / "failed" / out_path.name, out)
                out_path.unlink(missing_ok=True)
            else:
                out["sent_at"] = _now_beijing().isoformat()
                _send_ledger_mark(ctx, msg_id, "sent")
                _atomic_write_json(ctx.queue_dir / "done" / out_path.name, out)
                out_path.unlink(missing_ok=True)
                print(f"SENT id={msg_id} chunks={len(chunks)}", flush=True)
            ctx.stop_typing(to_user, msg_id)
        time.sleep(2)


def typing_loop(ctx: PairCtx) -> None:
    while not ctx.stop.is_set():
        if ctx.paused.is_set():
            time.sleep(10)
            continue
        with ctx.lock:
            items = [(u, e["ticket"]) for u, e in ctx.active_typing.items()]
        for user_id, ticket in items:
            try:
                res = send_typing(ctx.token, ctx.baseurl, user_id, ticket, 1)
                if res.get("ret") not in (0, None):
                    print(f"TYPING_RET ret={res.get('ret')}", flush=True)
            except Exception as exc:
                print(f"TYPING_ERROR {type(exc).__name__}", flush=True)
        time.sleep(5)


def prune_media_dir(ctx: PairCtx) -> tuple[int, int]:
    """Media prune (batch 3): remove media/in files older than 30
    days, then oldest-first until total <= 500MB. Returns
    (removed_count, remaining_bytes). Never touches queue/state."""
    media_dir = ctx.pair_dir / "media" / "in"
    if not media_dir.exists():
        return (0, 0)
    files = []
    for p in media_dir.iterdir():
        if p.is_file():
            try:
                files.append((p.stat().st_mtime, p.stat().st_size, p))
            except Exception:
                pass
    now = time.time()
    removed = 0
    for mtime, _size, p in files:
        if now - mtime > MEDIA_PRUNE_MAX_AGE_DAYS * 86400:
            try:
                p.unlink()
                removed += 1
            except Exception:
                pass
    files = []
    for p in media_dir.iterdir():
        if p.is_file():
            try:
                files.append((p.stat().st_mtime, p.stat().st_size, p))
            except Exception:
                pass
    total = sum(s for _m, s, _p in files)
    for _mtime, _size, p in sorted(files):
        if total <= MEDIA_PRUNE_MAX_BYTES:
            break
        try:
            total -= p.stat().st_size
            p.unlink()
            removed += 1
        except Exception:
            pass
    return (removed, total)


def prune_loop(ctx: PairCtx) -> None:
    while not ctx.stop.is_set():
        try:
            removed, _total = prune_media_dir(ctx)
            if removed:
                print(f"MEDIA_PRUNED removed={removed}", flush=True)
        except Exception:
            pass
        for _ in range(24 * 360):
            if ctx.stop.is_set():
                return
            time.sleep(10)


def run_queue(pair_id: str) -> int:
    ctx = PairCtx(pair_id)
    if (ctx.state_dir / "session_expired.json").exists():
        print("SESSION_EXPIRED_MARKER_PRESENT (will resume after pause)", flush=True)
    print(f"QUEUE_DAEMON_STARTED pair={pair_id} port={ctx.port}", flush=True)
    server = ThreadingHTTPServer(
        (TAILNET_IP, ctx.port), _make_notify_handler(ctx))
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    threads = [
        threading.Thread(target=poll_loop, args=(ctx,), daemon=True),
        threading.Thread(target=sender_loop, args=(ctx,), daemon=True),
        threading.Thread(target=typing_loop, args=(ctx,), daemon=True),
        threading.Thread(target=prune_loop, args=(ctx,), daemon=True),
    ]
    for t in threads:
        t.start()
    try:
        while True:
            time.sleep(10)
            if not any(t.is_alive() for t in threads):
                print("QUEUE_THREADS_DEAD", flush=True)
                return 1
    except KeyboardInterrupt:
        ctx.stop.set()
        return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="echo", choices=["echo", "queue"])
    parser.add_argument("--pair", default="primary")
    args = parser.parse_args()
    if args.mode == "queue":
        return run_queue(args.pair)
    # Echo mode (P1 legacy): uses legacy top-level state/, only for
    # rollback before the P2 pair migration. Not used in P2.
    from ilink_client import load_credentials
    creds = load_credentials()
    token, baseurl = creds["bot_token"], creds["baseurl"]
    cursor_path = LEGACY_STATE_DIR / "cursor.json"
    seen_path = LEGACY_STATE_DIR / "seen_ids.json"
    cursor = _load_json(cursor_path, {}).get("get_updates_buf", "")
    seen = set(_load_json(seen_path, []))
    print("ECHO_DAEMON_STARTED", flush=True)
    while True:
        try:
            data = getupdates(token, baseurl, cursor)
        except Exception as exc:
            print(f"POLL_ERROR {type(exc).__name__}", flush=True)
            time.sleep(5)
            continue
        ret = data.get("ret")
        if ret == -14 or data.get("errcode") == -14:
            print("SESSION_EXPIRED", flush=True)
            return 2
        if ret not in (0, None):
            time.sleep(5)
            continue
        new_cursor = data.get("get_updates_buf")
        if new_cursor:
            cursor = new_cursor
            _atomic_write_json(cursor_path, {"get_updates_buf": cursor})
        for msg in data.get("msgs") or []:
            msg_id = str(msg.get("message_id", ""))
            if msg_id in seen:
                continue
            seen.add(msg_id)
            _atomic_write_json(seen_path, sorted(seen)[-5000:])
            text = extract_text(msg).strip()
            if text.lower() != "ping":
                continue
            to_user = msg.get("from_user_id") or ""
            context_token = msg.get("context_token") or ""
            if not to_user or not context_token:
                continue
            now = _now_beijing().strftime("%Y-%m-%d %H:%M:%S")
            try:
                out = send_text(token, baseurl, to_user, context_token,
                                f"pong {now}")
            except Exception:
                continue
            print(f"PONG_SENT ret={out.get('ret')}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
