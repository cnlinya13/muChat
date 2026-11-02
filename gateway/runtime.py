"""Per-pair runtime state for the gateway.

Mirrors the live daemon's queue/state layout and semantics, but
parameterized for many pairs under one gateway root:

  <root>/pairs/<pair_id>/queue/{inbox,running,done,outbox,failed}
  <root>/pairs/<pair_id>/state/{cursor,seen_ids,sent_count,
      typing_tickets,alert_state,quote_cache,last_context,
      send_ledger,running_meta,session_expired}
  <root>/pairs/<pair_id>/logs/bridge.log
  <root>/pairs/<pair_id>/media/{in,out}

Differences vs the live daemon, all deliberate:
- No PAIR_PORTS: pairs are addressed by pair_id + hook token, never
  by port. The gateway serves ONE port for all pairs.
- Stuck recovery uses TAKE-TIME, not file mtime: the live daemon's
  mtime rule falsely flags a file that waited >=10 min in the serial
  queue the moment a worker takes it (2026-11-02 false positive).
  Here running_meta.json records when a file was FIRST SEEN in
  running/; a file is stuck only 10 min after that sighting, and
  the meta entry is dropped when the file leaves running/, so a
  re-taken file gets a fresh take-time.
- Credentials are decrypted into memory at gateway start (never
  read from a plaintext credentials.json by this module).
- Logs record time, direction, message id, ret and byte counts
  only -- never tokens, tickets, or message text.
"""

import json
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

MAX_PER_TOKEN = 9
WORKER_STUCK_AFTER_SECONDS = 600
ALERT_REPEAT_SECONDS = 3600
QUOTE_CACHE_MAX = 5000
MEDIA_PRUNE_MAX_AGE_DAYS = 30
MEDIA_PRUNE_MAX_BYTES = 500 * 1024 * 1024
POLL_ALERT_AFTER_SECONDS = 600
GENERIC_ACK_TEXT = "收到消息，正在处理，稍后回复。"


def _now_beijing():
    return datetime.now(timezone.utc).astimezone(
        timezone(timedelta(hours=8)))


def _atomic_write_json(path: Path, data, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2),
                   encoding="utf-8")
    os.chmod(tmp, mode)
    os.replace(tmp, path)


def _load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


class PairRuntime:
    def __init__(self, root: Path, pair_id: str, creds: dict):
        self.root = Path(root)
        self.pair_id = pair_id
        self.pair_dir = self.root / "pairs" / pair_id
        self.state_dir = self.pair_dir / "state"
        self.queue_dir = self.pair_dir / "queue"
        self.log_dir = self.pair_dir / "logs"
        self.media_dir = self.pair_dir / "media"
        self.log_path = self.log_dir / "bridge.log"
        for sub in ("inbox", "running", "done", "outbox", "failed"):
            (self.queue_dir / sub).mkdir(parents=True, exist_ok=True)
        for sub in ("state", "logs", "media/in", "media/out"):
            (self.pair_dir / sub).mkdir(parents=True, exist_ok=True)
        self.creds = creds
        self.token = creds["bot_token"]
        self.baseurl = creds["baseurl"]
        self.lock = threading.Lock()
        self.cursor = _load_json(
            self.state_dir / "cursor.json", {}).get("get_updates_buf", "")
        self.seen = set(_load_json(self.state_dir / "seen_ids.json", []))
        self.sent_count = _load_json(self.state_dir / "sent_count.json", {})
        self.typing_tickets = _load_json(
            self.state_dir / "typing_tickets.json", {})
        self.active_typing = {}  # user_id -> {"ticket": ..., "msg_ids": set}
        self.inbox_version = 0
        self.notify_event = threading.Event()
        self.paused = threading.Event()
        self.stop = threading.Event()

    # -- logging (ids/counts only) -----------------------------------
    def log_line(self, direction: str, msg_id: str, ret, nbytes: int) -> None:
        ts = _now_beijing().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(f"{ts} {direction} id={msg_id} ret={ret} bytes={nbytes}\n")

    def inbox_ids(self):
        return sorted(p.stem for p in (self.queue_dir / "inbox").glob("*.json"))

    # -- alerts --------------------------------------------------------
    def alert_set(self, kind: str, detail: str = "") -> None:
        with self.lock:
            state = _load_json(self.state_dir / "alert_state.json", {})
            now = time.time()
            prev = state.get(kind, {})
            if prev.get("active") and \
                    now - prev.get("reported_at", 0) < ALERT_REPEAT_SECONDS:
                return
            state[kind] = {"active": True, "detail": detail,
                           "reported_at": now,
                           "first_at": prev.get("first_at", now)}
            _atomic_write_json(self.state_dir / "alert_state.json", state)

    def alert_clear(self, kind: str) -> None:
        with self.lock:
            state = _load_json(self.state_dir / "alert_state.json", {})
            if state.get(kind, {}).get("active"):
                state[kind] = {"active": False, "detail": "",
                               "reported_at": time.time(),
                               "first_at": state[kind].get("first_at", 0)}
                _atomic_write_json(
                    self.state_dir / "alert_state.json", state)

    def alert_active(self) -> dict:
        with self.lock:
            state = _load_json(self.state_dir / "alert_state.json", {})
        return {k: v for k, v in state.items() if v.get("active")}

    # -- owner whitelist (same semantics as the live daemon) ----------
    def owner_ids(self) -> list:
        data = _load_json(self.state_dir / "owner_ids.json", {})
        if isinstance(data, dict):
            return data.get("owner_ids", []) or []
        return []

    def note_unknown_sender(self, from_id: str) -> None:
        if not from_id:
            return
        with self.lock:
            data = _load_json(self.state_dir / "unknown_senders.json", {})
            entry = data.get(from_id, {"count": 0})
            entry["count"] = int(entry.get("count", 0)) + 1
            entry["last_at"] = _now_beijing().isoformat()
            data[from_id] = entry
            _atomic_write_json(
                self.state_dir / "unknown_senders.json", data)

    def is_owner_or_bind(self, from_id: str) -> bool:
        if not from_id:
            return False
        owners = self.owner_ids()
        if not owners:
            with self.lock:
                _atomic_write_json(self.state_dir / "owner_ids.json",
                                   {"owner_ids": [from_id]})
            return True
        return from_id in owners

    # -- quote cache (ids -> text/media path only) ---------------------
    def cache_quote(self, key: str, text: str, media_path: str = "") -> None:
        if not key:
            return
        with self.lock:
            cache = _load_json(self.state_dir / "quote_cache.json", {})
            cache[key] = {"text": text, "media_path": media_path}
            while len(cache) > QUOTE_CACHE_MAX:
                cache.pop(next(iter(cache)))
            _atomic_write_json(self.state_dir / "quote_cache.json", cache)

    def lookup_quote(self, quoted_id: str) -> dict:
        cache = _load_json(self.state_dir / "quote_cache.json", {})
        return cache.get(quoted_id, {})

    # -- send ledger ----------------------------------------------------
    def ledger_status(self, key: str) -> str:
        return _load_json(
            self.state_dir / "send_ledger.json", {}).get(key, {}).get(
                "status", "")

    def ledger_mark(self, key: str, status: str, client_id: str = "") -> None:
        with self.lock:
            ledger = _load_json(self.state_dir / "send_ledger.json", {})
            ledger[key] = {"status": status, "client_id": client_id,
                           "at": _now_beijing().isoformat()}
            while len(ledger) > 5000:
                ledger.pop(next(iter(ledger)))
            _atomic_write_json(self.state_dir / "send_ledger.json", ledger)

    # -- typing tickets --------------------------------------------------
    def start_typing(self, user_id: str, context_token: str, msg_id: str,
                     get_config_fn) -> None:
        """get_config_fn(token, baseurl, user_id, context_token) is
        injected so tests never touch the network."""
        if not user_id:
            return
        with self.lock:
            entry = self.active_typing.get(user_id)
            if entry is None:
                info = self.typing_tickets.get(user_id, {})
                ticket = info.get("ticket")
                fetched_at = info.get("fetched_at", 0)
                if not ticket or time.time() - fetched_at > 24 * 3600:
                    try:
                        cfg = get_config_fn(self.token, self.baseurl,
                                            user_id, context_token)
                    except Exception as exc:
                        print(f"TYPING_CONFIG_ERROR {type(exc).__name__}",
                              flush=True)
                        return
                    if cfg.get("ret") not in (0, None) or \
                            not cfg.get("typing_ticket"):
                        print(f"TYPING_CONFIG_RET={cfg.get('ret')}",
                              flush=True)
                        return
                    ticket = cfg["typing_ticket"]
                    self.typing_tickets[user_id] = {
                        "ticket": ticket, "fetched_at": time.time()}
                    _atomic_write_json(self.state_dir / "typing_tickets.json",
                                       self.typing_tickets)
                entry = {"ticket": ticket, "msg_ids": set()}
                self.active_typing[user_id] = entry
            entry["msg_ids"].add(msg_id)

    def stop_typing(self, user_id: str, msg_id: str, send_typing_fn) -> None:
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
            res = send_typing_fn(self.token, self.baseurl, user_id, ticket, 2)
            print(f"TYPING_CANCEL ret={res.get('ret')}", flush=True)
        except Exception as exc:
            print(f"TYPING_CANCEL_ERROR {type(exc).__name__}", flush=True)

    def cancel_all_typing(self, send_typing_fn) -> None:
        with self.lock:
            items = [(u, e["ticket"], set(e["msg_ids"]))
                     for u, e in self.active_typing.items()]
            self.active_typing.clear()
        for user_id, ticket, _mids in items:
            try:
                send_typing_fn(self.token, self.baseurl, user_id, ticket, 2)
            except Exception:
                pass

    # -- stuck recovery, TAKE-TIME (fix vs live daemon mtime bug) -------
    def recover_stuck_workers(self) -> None:
        """A running file is stuck only 10 min after it was FIRST
        SEEN in running/ (take-time), never by mtime. The meta entry
        is dropped when the file leaves running/, so a re-taken file
        gets a fresh take-time instead of an instant false flag."""
        now = time.time()
        meta_path = self.state_dir / "running_meta.json"
        with self.lock:
            meta = _load_json(meta_path, {})
            changed = False
            live = set()
            for path in (self.queue_dir / "running").glob("*.json"):
                live.add(path.stem)
                first_seen = meta.get(path.stem)
                if first_seen is None:
                    meta[path.stem] = now
                    changed = True
                    continue
                if now - first_seen < WORKER_STUCK_AFTER_SECONDS:
                    continue
                self.alert_set_locked(meta, "worker_stuck", path.stem)
                try:
                    dest = self.queue_dir / "inbox" / path.name
                    path.rename(dest)
                    del meta[path.stem]
                    changed = True
                    self.inbox_version += 1
                    print(f"WORKER_STUCK_RECOVERED id={path.stem}",
                          flush=True)
                except Exception:
                    pass
            for stem in [s for s in meta if s not in live]:
                del meta[stem]
                changed = True
            if changed:
                _atomic_write_json(meta_path, meta)
        if changed:
            self.notify_event.set()

    def alert_set_locked(self, _meta, kind: str, detail: str = "") -> None:
        """alert_set variant for use while runtime.lock is held."""
        state = _load_json(self.state_dir / "alert_state.json", {})
        now = time.time()
        prev = state.get(kind, {})
        if prev.get("active") and \
                now - prev.get("reported_at", 0) < ALERT_REPEAT_SECONDS:
            return
        state[kind] = {"active": True, "detail": detail,
                       "reported_at": now,
                       "first_at": prev.get("first_at", now)}
        _atomic_write_json(self.state_dir / "alert_state.json", state)

    # -- generic daemon ack (never lists filenames) ----------------------
    def write_daemon_ack(self, queued_batch, turn_start_inbox_stems,
                         turn_start_running_empty) -> None:
        """NEVER raises: an ack bug must never kill a poll thread
        (2026-11-01 live incident). Same gate as the live daemon:
        only when this batch started a new turn (inbox AND running
        were both empty at turn start)."""
        try:
            if not queued_batch:
                return
            batch_has_files = any(
                me.get("file_name")
                for _mid, ib in queued_batch
                for me in (ib.get("media_files") or []))
            if not batch_has_files:
                time.sleep(2)
            try:
                if not turn_start_inbox_stems and turn_start_running_empty:
                    first_id, first_inbox = queued_batch[0]
                    _atomic_write_json(
                        self.queue_dir / "outbox" / f"{first_id}-ack.json",
                        {"message_id": f"{first_id}-ack",
                         "pair_id": self.pair_id,
                         "from_user_id": first_inbox.get("from_user_id", ""),
                         "context_token": first_inbox.get("context_token", ""),
                         "reply": GENERIC_ACK_TEXT,
                         "daemon_ack": True,
                         "skip_merge_wait": True})
            except Exception:
                print("DAEMON_ACK_FAILED", flush=True)
        except Exception:
            print("DAEMON_ACK_FAILED", flush=True)


def prune_media_dir(pair_dir: Path) -> tuple[int, int]:
    """Same 30-day / 500MB rule as the live daemon. Never touches
    queue or state."""
    media_dir = pair_dir / "media" / "in"
    if not media_dir.exists():
        return (0, 0)
    now = time.time()

    def _scan():
        out = []
        for p in media_dir.iterdir():
            if p.is_file():
                try:
                    out.append((p.stat().st_mtime, p.stat().st_size, p))
                except Exception:
                    pass
        return out

    removed = 0
    for mtime, _size, p in _scan():
        if now - mtime > MEDIA_PRUNE_MAX_AGE_DAYS * 86400:
            try:
                p.unlink()
                removed += 1
            except Exception:
                pass
    files = _scan()
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
