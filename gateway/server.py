"""Single-gateway HTTP server + process wiring.

One port for all pairs. Hooks poll GET /inbox/wait with their
pair_id and hook token; the gateway returns ONLY that pair's
messages. No per-pair ports, no per-pair processes.

Endpoints (bind: tailnet IP only, same as the live daemons):
- GET /health -> {"ok": true, "pairs": [pair_id...], "uptime_s": n}
  (ids and status only; no tokens, no message text, no counts
  that could leak content -- counts are intentionally omitted).
- GET /inbox/wait?pair_id=<id>&timeout=<0..60>
  Headers: Authorization: Bearer <hook token>  OR
           X-Hook-Token: <hook token>
  Responses: 200 {pair_id, new: [ids], texts: {id: text for
    text-only messages}, alerts: [active kinds]}
    400 missing pair_id, 401 bad/missing token,
    403 token does not belong to this pair, 404 unknown pair.
  The response never contains tokens, tickets, or media.

Process wiring (one gateway process):
- HTTP server thread (ThreadingHTTPServer).
- Poller pool: N threads (default 2), sharded by pair_id hash.
  Each thread runs poller_thread() for its pairs in turn.
- One unified sender thread for all pairs.
- One typing thread: sendtyping status=1 every 5s for every
  pair's active typing users.
- One prune thread: 24h cycle, per-pair media prune.
- Reloader thread (5s): reloads the registry from disk and
  starts a poller for each active pair that gained complete
  decryptable credentials after startup, so a freshly bound pair
  goes live with NO gateway restart. The reloader ONLY ADDS
  runtimes, never removes them: disabling or deleting a pair
  still needs a restart to take effect (documented limit).
- Watchdog: if all worker threads die, exit 1 (systemd restarts).

Run: python3 server.py --root ~/wechat-bridge-gateway \
         --host YOUR_GATEWAY_TAILNET_IP --port 3820 [--pollers 2] \
         [--dry-run-sender]
"""

import argparse
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, str(Path(__file__).resolve().parent))
import os as _os
# Bridge dir: on the dev VM the gateway lives inside wechat-bridge/
# (parent/bridge); on the server it lives in wechat-bridge-gateway/,
# so the unit sets MUCHAT_BRIDGE_DIR=/home/ubuntu/wechat-bridge/bridge.
_BRIDGE_DIR = _os.environ.get(
    "MUCHAT_BRIDGE_DIR",
    str(Path(__file__).resolve().parent.parent / "bridge"))
sys.path.insert(0, _BRIDGE_DIR)

from poll import poller_thread  # noqa: E402
from registry import Registry  # noqa: E402
from runtime import PairRuntime, prune_media_dir  # noqa: E402
from send import sender_thread  # noqa: E402


def _default_send_typing(token, baseurl, user_id, ticket, status):
    from ilink_client import send_typing
    return send_typing(token, baseurl, user_id, ticket, status)


def make_handler(runtimes, registry, runtimes_lock=None):
    """runtimes: {pair_id: PairRuntime} (live dict, shared with
    the reloader). registry: Registry (for token verification).
    The handler never sees a plaintext token beyond the request
    header it is verifying."""

    def _runtimes_get(pid):
        if runtimes_lock is not None:
            with runtimes_lock:
                return runtimes.get(pid)
        return runtimes.get(pid)

    def _runtimes_keys():
        if runtimes_lock is not None:
            with runtimes_lock:
                return sorted(runtimes.keys())
        return sorted(runtimes.keys())

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _send_json(self, code, obj):
            body = json.dumps(obj, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self._send_json(200, {
                    "ok": True,
                    "pairs": _runtimes_keys(),
                    "uptime_s": int(time.time() - STARTED_AT),
                })
                return
            if parsed.path != "/inbox/wait":
                self._send_json(404, {"error": "not_found"})
                return
            qs = parse_qs(parsed.query)
            pair_id = (qs.get("pair_id", [""])[0] or "")
            if not pair_id:
                self._send_json(400, {"error": "missing_pair_id"})
                return
            # Registry first, runtimes second. An unknown pair is
            # 404 regardless of token (no existence oracle); a
            # registered pair with a bad token is 403; a registered
            # pair with a good token but no runtime yet (not yet
            # bound, or bound after this process started) polls a
            # clean empty set, never 404, so a pre-binding hook
            # dry_run is not an error.
            # One reload on miss: the on-disk registry may be newer
            # than this process's copy (a pair created after
            # startup, before the reloader's next 5s cycle). Without
            # this, a just-created pair 404s for up to 5s -- the
            # exact dry_run window the fix must keep clean. Known
            # pairs take the fast path with no reload.
            if registry.get(pair_id) is None:
                registry.reload()
            if registry.get(pair_id) is None:
                self._send_json(404, {"error": "unknown_pair"})
                return
            token = ""
            auth = self.headers.get("Authorization", "")
            if auth.startswith("Bearer "):
                token = auth[7:].strip()
            if not token:
                token = (self.headers.get("X-Hook-Token", "") or "").strip()
            if not token:
                self._send_json(401, {"error": "missing_token"})
                return
            if not registry.verify_token(pair_id, token):
                self._send_json(403, {"error": "token_pair_mismatch"})
                return
            rt = _runtimes_get(pair_id)
            if rt is None:
                self._send_json(200, {
                    "pair_id": pair_id,
                    "new": [],
                    "texts": {},
                    "alerts": [],
                })
                return
            try:
                timeout = float(qs.get("timeout", ["30"])[0])
            except ValueError:
                timeout = 30.0
            timeout = min(max(timeout, 0), 60)
            with rt.lock:
                start_version = rt.inbox_version
            rt.notify_event.wait(timeout=timeout)
            with rt.lock:
                if rt.inbox_version == start_version:
                    rt.notify_event.clear()
                ids = rt.inbox_ids()
            # Text passthrough (same user-approved rule as the live
            # daemon): verbatim text for text-only messages only,
            # over this tailnet channel, never tokens or media.
            texts = {}
            for mid in ids:
                try:
                    ib = json.loads(
                        (rt.queue_dir / "inbox" / f"{mid}.json")
                        .read_text(encoding="utf-8"))
                except Exception:
                    continue
                if (ib.get("kind") == "text" and not ib.get("media_files")
                        and ib.get("text")):
                    texts[mid] = ib["text"]
            self._send_json(200, {
                "pair_id": rt.pair_id,
                "new": ids,
                "texts": texts,
                "alerts": sorted(rt.alert_active().keys()),
            })

    return Handler


STARTED_AT = time.time()


def _snapshot(runtimes, runtimes_lock):
    """Fresh per-sweep copy of the LIVE runtimes dict, so
    hot-added pairs are covered. Callers pass the dict, never a
    startup snapshot list."""
    if runtimes_lock is not None:
        with runtimes_lock:
            return list(runtimes.values())
    return list(runtimes.values())


def typing_thread(runtimes, stop_event, runtimes_lock=None):
    send_typing_fn = _default_send_typing
    while not stop_event.is_set():
        for rt in _snapshot(runtimes, runtimes_lock):
            if stop_event.is_set():
                break
            if rt.paused.is_set():
                continue
            with rt.lock:
                items = [(u, e["ticket"])
                         for u, e in rt.active_typing.items()]
            for user_id, ticket in items:
                try:
                    res = send_typing_fn(rt.token, rt.baseurl, user_id,
                                         ticket, 1)
                    if res.get("ret") not in (0, None):
                        print(f"TYPING_RET pair={rt.pair_id} "
                              f"ret={res.get('ret')}", flush=True)
                except Exception as exc:
                    print(f"TYPING_ERROR pair={rt.pair_id} "
                          f"{type(exc).__name__}", flush=True)
        for _ in range(5):
            if stop_event.is_set():
                break
            time.sleep(1)


def prune_thread(runtimes, stop_event, runtimes_lock=None):
    while not stop_event.is_set():
        try:
            for rt in _snapshot(runtimes, runtimes_lock):
                removed, _total = prune_media_dir(rt.pair_dir)
                if removed:
                    print(f"MEDIA_PRUNED pair={rt.pair_id} "
                          f"removed={removed}", flush=True)
        except Exception:
            pass
        for _ in range(24 * 360):
            if stop_event.is_set():
                return
            time.sleep(10)


def build_runtimes(root, registry):
    """Build PairRuntime objects for active pairs with decryptable
    credentials. A pair that fails decryption is SKIPPED with a
    printed kind (never its secret) and does not start polling."""
    runtimes = {}
    for pair_id, info in sorted(registry.data["pairs"].items()):
        if info.get("status") != "active":
            continue
        try:
            creds = registry.load_credentials(pair_id)
            if not creds.get("bot_token") or not creds.get("baseurl"):
                raise ValueError("incomplete credentials")
        except Exception as exc:
            print(f"PAIR_SKIP pair={pair_id} {type(exc).__name__}",
                  flush=True)
            continue
        runtimes[pair_id] = PairRuntime(root, pair_id, creds)
    return runtimes


def reloader_thread(root, registry, runtimes, runtimes_lock, stop_event,
                    pool, poller_fn=None):
    """Hot-activation: every 5s reload the registry from disk and
    start a poller for each ACTIVE pair that has complete
    decryptable credentials but no runtime yet. This is what makes
    a freshly bound pair come online WITHOUT a gateway restart.

    The reloader ONLY ADDS runtimes, never removes them:
    disabling or deleting a pair still needs a gateway restart to
    take effect (documented limit; a restart is cheap and rare,
    while a wrongly removed live runtime would drop messages).
    Hot-added pairs get their own dedicated poller thread (the
    startup pool sharding is for startup pairs only). A pair whose
    credentials are missing, incomplete, or undecryptable is
    skipped silently (no log spam every 5s); only a successful
    hot-add is logged. This thread never exits on a per-cycle
    error, so a bad registry read cannot kill activation."""
    poller_fn = poller_fn or poller_thread
    while not stop_event.is_set():
        try:
            registry.reload()
            for pair_id, info in sorted(registry.data["pairs"].items()):
                if info.get("status") != "active":
                    continue
                with runtimes_lock:
                    if pair_id in runtimes:
                        continue
                try:
                    creds = registry.load_credentials(pair_id)
                    if not creds.get("bot_token") or not creds.get("baseurl"):
                        raise ValueError("incomplete credentials")
                except Exception:
                    continue
                rt = PairRuntime(root, pair_id, creds)
                with runtimes_lock:
                    runtimes[pair_id] = rt
                t = threading.Thread(target=poller_fn,
                                     args=(rt, stop_event), daemon=True)
                t.start()
                pool.append(t)
                print(f"PAIR_HOT_ADDED pair={pair_id}", flush=True)
        except Exception as exc:
            print(f"RELOADER_ERROR {type(exc).__name__}", flush=True)
        stop_event.wait(5)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--host", default="YOUR_GATEWAY_TAILNET_IP")
    ap.add_argument("--port", type=int, default=3820)
    ap.add_argument("--pollers", type=int, default=2)
    ap.add_argument("--dry-run-sender", action="store_true")
    args = ap.parse_args()

    global STARTED_AT
    STARTED_AT = time.time()
    root = Path(args.root)
    registry = Registry(root)
    runtimes = build_runtimes(root, registry)
    runtimes_lock = threading.Lock()
    print(f"GATEWAY_STARTED pairs={sorted(runtimes.keys())} "
          f"port={args.port} pollers={args.pollers} "
          f"dry_run_sender={args.dry_run_sender}", flush=True)

    stop = threading.Event()
    server = ThreadingHTTPServer(
        (args.host, args.port),
        make_handler(runtimes, registry, runtimes_lock))
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()

    # Poller pool: shard pairs by STABLE hash across N threads
    # (builtin hash() is randomized per process; sha256 keeps a
    # pair on the same shard across restarts).
    import hashlib
    pair_ids = sorted(runtimes.keys())
    pool = []
    for shard in range(max(1, args.pollers)):
        shard_pairs = [pid for pid in pair_ids
                       if (int(hashlib.sha256(pid.encode()).hexdigest(), 16)
                           % max(1, args.pollers)) == shard]
        for pid in shard_pairs:
            t = threading.Thread(target=poller_thread,
                                 args=(runtimes[pid], stop),
                                 daemon=True)
            t.start()
            pool.append(t)
    workers = [
        threading.Thread(target=sender_thread,
                         args=(runtimes, stop),
                         kwargs={"dry_run": args.dry_run_sender,
                                 "runtimes_lock": runtimes_lock},
                         daemon=True),
        threading.Thread(target=typing_thread,
                         args=(runtimes, stop, runtimes_lock),
                         daemon=True),
        threading.Thread(target=prune_thread,
                         args=(runtimes, stop, runtimes_lock),
                         daemon=True),
    ]
    for t in workers:
        t.start()
    pool.extend(workers)
    reloader = threading.Thread(target=reloader_thread,
                                args=(root, registry, runtimes, runtimes_lock,
                                      stop, pool),
                                daemon=True)
    reloader.start()
    pool.append(reloader)
    try:
        while True:
            time.sleep(10)
            if not any(t.is_alive() for t in pool):
                print("GATEWAY_THREADS_DEAD", flush=True)
                return 1
    except KeyboardInterrupt:
        stop.set()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
