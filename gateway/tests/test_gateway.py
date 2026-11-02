"""Gateway self-tests (stdlib + venv python, no Tencent, no live pairs).

Run: ~/workspace/wechat-bridge/.venv/bin/python tests/test_gateway.py
Covers:
  1. Registry: pair creation format, token issue/verify (good, bad,
     cross-pair, unknown pair), encrypted credentials round-trip,
     no plaintext token in the registry file.
  2. HTTP routing isolation: two test pairs on one gateway port;
     token A sees only A's messages, token B only B's; wrong token
     -> 403, missing token -> 401, unknown pair -> 404; text
     passthrough only for text-only messages.
  3. Poller with canned fetch/download: text + file messages land
     in the right inbox with the right schema, cursor commits,
     generic ack is written, replay dedups, -14 yields the marker.
  4. Sender dry-run: right-pair outbox -> done (dry_run_no_send),
     cross-pair outbox -> failed, empty reply -> failed,
     ledger-sent outbox -> skipped_duplicate.
  5. Stuck recovery take-time fix: a running file with an OLD mtime
     is NOT flagged (take-time is fresh); only a file first seen in
     running/ >=10 min ago is recycled.
  6. Auto-activation: a registered pair with a good token but no
     runtime polls 200 with an empty set (never 404); a reloader
     thread hot-adds a runtime once credentials appear, without
     any restart; disabled and unbound pairs are skipped; a
     registry reload picks up externally written pairs.

All credentials and tokens here are SYNTHETIC test values.
"""

import json
import os
import sys
import tempfile
import threading
import time
import urllib.request
import urllib.error
from http.server import ThreadingHTTPServer
from pathlib import Path

GW = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(GW))
sys.path.insert(0, str(GW.parent / "bridge"))

from registry import PAIR_ID_RE, Registry  # noqa: E402
from runtime import PairRuntime  # noqa: E402
from poll import process_batch  # noqa: E402
from send import send_one_outbox  # noqa: E402
from server import make_handler, reloader_thread  # noqa: E402

FAKE_CREDS = {"bot_token": "SYNTHETIC", "baseurl": "http://127.0.0.1:9",
              "ilink_bot_id": "synthetic-bot",
              "ilink_user_id": "synthetic-user"}


def check(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    if not cond:
        raise AssertionError(name)


def test_registry(tmp):
    reg = Registry(Path(tmp) / "gw")
    pid = reg.create_pair("Test A", "chat-a")
    check("pair id format", bool(PAIR_ID_RE.match(pid)))
    token = reg.issue_token(pid)
    check("token verifies", reg.verify_token(pid, token))
    check("bad token rejected", not reg.verify_token(pid, token + "x"))
    check("empty token rejected", not reg.verify_token(pid, ""))
    check("unknown pair rejected", not reg.verify_token("pair-00000000",
                                                        token))
    pid2 = reg.create_pair("Test B", "chat-b")
    token2 = reg.issue_token(pid2)
    check("cross-pair token rejected",
          not reg.verify_token(pid, token2)
          and not reg.verify_token(pid2, token))
    reg.store_credentials(pid, FAKE_CREDS)
    check("creds round-trip",
          reg.load_credentials(pid)["bot_token"] == "SYNTHETIC")
    raw = (Path(tmp) / "gw" / "registry" / "registry.json").read_text()
    check("no plaintext token in registry", "SYNTHETIC" not in raw)
    return reg, pid, token, pid2, token2


def _get(base, pair_id, token=None, timeout="0"):
    url = f"{base}/inbox/wait?pair_id={pair_id}&timeout={timeout}"
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def test_http_routing(tmp, reg, pid_a, tok_a, pid_b, tok_b):
    root = Path(tmp) / "gw"
    rt_a = PairRuntime(root, pid_a, FAKE_CREDS)
    rt_b = PairRuntime(root, pid_b, FAKE_CREDS)
    runtimes = {pid_a: rt_a, pid_b: rt_b}
    srv = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(runtimes, reg))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        # Inject one text message per pair + one media-kind file for A.
        for rt, mid, text, kind in (
                (rt_a, "msg-a1", "hello A", "text"),
                (rt_b, "msg-b1", "hello B", "text")):
            (rt.queue_dir / "inbox" / f"{mid}.json").write_text(
                json.dumps({"message_id": mid, "pair_id": rt.pair_id,
                            "kind": kind, "text": text,
                            "media_files": []}), encoding="utf-8")
        (rt_a.queue_dir / "inbox" / "msg-a2.json").write_text(
            json.dumps({"message_id": "msg-a2", "pair_id": pid_a,
                        "kind": "media", "text": "",
                        "media_files": [{"type": 4}]}), encoding="utf-8")
        code, body = _get(base, pid_a, tok_a)
        check("A sees only A ids",
              code == 200 and body["new"] == ["msg-a1", "msg-a2"]
              and body["pair_id"] == pid_a)
        check("texts only for text-only",
              body["texts"] == {"msg-a1": "hello A"})
        code, body = _get(base, pid_b, tok_b)
        check("B sees only B ids",
              code == 200 and body["new"] == ["msg-b1"])
        check("cross-pair token -> 403",
              _get(base, pid_b, tok_a)[0] == 403)
        check("wrong token -> 403",
              _get(base, pid_a, tok_a + "x")[0] == 403)
        check("missing token -> 401",
              _get(base, pid_a, None)[0] == 401)
        check("unknown pair -> 404",
              _get(base, "pair-00000000", tok_a)[0] == 404)
        code, body = _get(base, pid_a, tok_a)
        check("health lists pairs",
              urllib.request.urlopen(base + "/health",
                                     timeout=10).read().decode()
              .find(pid_a) >= 0)
    finally:
        srv.shutdown()


def test_poller(tmp):
    root = Path(tmp) / "gw2"
    rt = PairRuntime(root, "pair-11111111", FAKE_CREDS)
    seen_fetch = []

    def fake_fetch(token, baseurl, cursor):
        seen_fetch.append(cursor)
        return {
            "ret": 0, "get_updates_buf": "cursor-2",
            "msgs": [
                {"message_id": "m1", "seq": 1, "from_user_id": "user-1",
                 "context_token": "ctx-1",
                 "item_list": [{"type": 1,
                                "text_item": {"text": "ping"}}]},
                {"message_id": "m2", "seq": 2, "from_user_id": "user-1",
                 "context_token": "ctx-1",
                 "item_list": [{"type": 4, "file_item": {
                     "file_name": "report.pdf",
                     "media": {"full_url": "http://example.invalid/x"},
                     "aeskey": None}}]},
            ]}

    def fake_download(media, aeskey):
        return b"fake-bytes"

    def fake_config(token, baseurl, user_id, context_token):
        return {"ret": 0, "typing_ticket": "SYNTHETIC-TICKET"}

    deps = {"download_fn": fake_download, "get_config_fn": fake_config}
    from poll import poller_thread  # noqa: E402 - structure check only
    n = process_batch(rt, fake_fetch("t", "b", ""), deps)
    check("poller queues 2", n == 2)
    ib1 = json.loads((rt.queue_dir / "inbox" / "m1.json").read_text())
    check("text inbox schema",
          ib1["text"] == "ping" and ib1["kind"] == "text"
          and ib1["pair_id"] == "pair-11111111")
    ib2 = json.loads((rt.queue_dir / "inbox" / "m2.json").read_text())
    check("file inbox schema",
          ib2["media_files"] and ib2["media_files"][0]["file_name"]
          == "report.pdf" and ib2["media_files"][0]["size"] == 10)
    check("cursor committed",
          json.loads((rt.state_dir / "cursor.json").read_text())
          ["get_updates_buf"] == "cursor-2")
    ack = rt.queue_dir / "outbox" / "m1-ack.json"
    check("generic ack written",
          ack.exists() and json.loads(ack.read_text())["reply"]
          == "收到消息，正在处理，稍后回复。")
    n2 = process_batch(rt, fake_fetch("t", "b", "cursor-2"), deps)
    check("replay dedups", n2 == 0)
    r = process_batch(rt, {"ret": -14}, deps)
    check("-14 marker",
          r == "session_expired"
          and (rt.state_dir / "session_expired.json").exists())
    (rt.state_dir / "session_expired.json").unlink(missing_ok=True)


def test_sender_dryrun(tmp):
    root = Path(tmp) / "gw3"
    rt_a = PairRuntime(root, "pair-22222222", FAKE_CREDS)
    rt_b = PairRuntime(root, "pair-33333333", FAKE_CREDS)
    out_a = {"message_id": "o1", "pair_id": "pair-22222222",
             "from_user_id": "user-1", "context_token": "ctx-1",
             "reply": "hello back"}
    p = rt_a.queue_dir / "outbox" / "o1.json"
    p.write_text(json.dumps(out_a), encoding="utf-8")
    st = send_one_outbox(rt_a, p, dry_run=True)
    done = json.loads((rt_a.queue_dir / "done" / "o1.json").read_text())
    check("dry-run routes right pair",
          st == "dry_run" and done["note"] == "dry_run_no_send")
    cross = {"message_id": "o2", "pair_id": "pair-33333333",
             "from_user_id": "user-1", "context_token": "ctx-1",
             "reply": "wrong pair"}
    p2 = rt_a.queue_dir / "outbox" / "o2.json"
    p2.write_text(json.dumps(cross), encoding="utf-8")
    check("cross-pair outbox failed",
          send_one_outbox(rt_a, p2, dry_run=True) == "failed"
          and (rt_a.queue_dir / "failed" / "o2.json").exists())
    empty = {"message_id": "o3", "pair_id": "pair-22222222",
             "from_user_id": "user-1", "context_token": "ctx-1",
             "reply": "   "}
    p3 = rt_a.queue_dir / "outbox" / "o3.json"
    p3.write_text(json.dumps(empty), encoding="utf-8")
    check("empty reply failed",
          send_one_outbox(rt_a, p3, dry_run=True) == "failed")
    rt_a.ledger_mark("o4", "sent")
    dup = {"message_id": "o4", "pair_id": "pair-22222222",
           "from_user_id": "user-1", "context_token": "ctx-1",
           "reply": "again"}
    p4 = rt_a.queue_dir / "outbox" / "o4.json"
    p4.write_text(json.dumps(dup), encoding="utf-8")
    check("ledger-sent skipped",
          send_one_outbox(rt_a, p4, dry_run=True) == "skipped_duplicate")


def test_stuck_take_time(tmp):
    root = Path(tmp) / "gw4"
    rt = PairRuntime(root, "pair-44444444", FAKE_CREDS)
    f = rt.queue_dir / "running" / "old.json"
    f.write_text(json.dumps({"message_id": "old"}), encoding="utf-8")
    # Old mtime (20 min ago) must NOT flag: take-time is now.
    os.utime(f, (time.time() - 1200, time.time() - 1200))
    rt.recover_stuck_workers()
    check("old-mtime file NOT flagged (take-time fix)",
          f.exists() and not (rt.queue_dir / "inbox" / "old.json").exists())
    # Now fake a first-seen 20 min ago -> must recycle.
    meta = rt.state_dir / "running_meta.json"
    meta.write_text(json.dumps({"old": time.time() - 1200}),
                    encoding="utf-8")
    rt.recover_stuck_workers()
    check("take-time-old file recycled",
          (rt.queue_dir / "inbox" / "old.json").exists()
          and not f.exists())


def test_handler_unbound_pair(tmp):
    root = Path(tmp) / "gw-unbound"
    reg = Registry(root)
    pid = reg.create_pair("Unbound", "chat-u")
    token = reg.issue_token(pid)
    # No credentials, no runtime: registered but unbound.
    runtimes = {}
    srv = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(runtimes, reg))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        code, body = _get(base, pid, token)
        check("registered-no-runtime polls 200 empty",
              code == 200 and body["new"] == [] and body["texts"] == {}
              and body["alerts"] == [] and body["pair_id"] == pid)
        check("registered-no-runtime bad token -> 403",
              _get(base, pid, token + "x")[0] == 403)
        check("registered-no-runtime missing token -> 401",
              _get(base, pid, None)[0] == 401)
        check("unknown pair still 404 with any token",
              _get(base, "pair-00000000", token)[0] == 404)
        check("unknown pair still 404 without token",
              _get(base, "pair-00000000", None)[0] == 404)
    finally:
        srv.shutdown()


def test_handler_stale_registry_reload(tmp):
    # Reproduces the live race: the handler's registry copy was
    # loaded BEFORE the pair was created (as with a CLI-created
    # pair on a running gateway). The first poll must still return
    # 200 empty, not 404, because the handler reloads on miss.
    root = Path(tmp) / "gw-stale"
    reg = Registry(root)  # handler's copy, loaded before pair exists
    reg_cli = Registry(root)  # simulates the CLI writing to disk
    pid = reg_cli.create_pair("Stale", "chat-s")
    token = reg_cli.issue_token(pid)
    check("stale registry unaware before poll", reg.get(pid) is None)
    runtimes = {}
    srv = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(runtimes, reg))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        code, body = _get(base, pid, token)
        check("stale registry poll returns 200 empty",
              code == 200 and body["new"] == [] and body["texts"] == {}
              and body["alerts"] == [])
    finally:
        srv.shutdown()


def test_registry_reload(tmp):
    root = Path(tmp) / "gw-rl"
    reg = Registry(root)
    reg2 = Registry(root)
    pid = reg2.create_pair("External", "chat-e")
    check("reload: external pair invisible before reload",
          reg.get(pid) is None)
    reg.reload()
    check("reload: external pair visible after reload",
          reg.get(pid) is not None)


def test_reloader(tmp):
    root = Path(tmp) / "gw-reload"
    reg = Registry(root)
    pid = reg.create_pair("Reload", "chat-r")
    reg.issue_token(pid)
    # No credentials yet: the reloader must NOT add a runtime.
    runtimes = {}
    lock = threading.Lock()
    stop = threading.Event()
    pool = []
    added = []

    def fake_poller(rt, stop_event):
        added.append(rt.pair_id)

    t = threading.Thread(target=reloader_thread,
                         args=(root, reg, runtimes, lock, stop, pool),
                         kwargs={"poller_fn": fake_poller}, daemon=True)
    t.start()
    time.sleep(6)
    check("reloader skips unbound pair", pid not in runtimes)
    # Disabled pair with credentials must also be skipped.
    pid2 = reg.create_pair("Disabled", "chat-d")
    reg.issue_token(pid2)
    reg.store_credentials(pid2, FAKE_CREDS)
    reg.set_status(pid2, "disabled")
    time.sleep(6)
    check("reloader skips disabled pair", pid2 not in runtimes)
    # Now store credentials for the first pair: hot-added, no restart.
    reg.store_credentials(pid, FAKE_CREDS)
    deadline = time.time() + 12
    while pid not in runtimes and time.time() < deadline:
        time.sleep(0.5)
    check("reloader hot-adds bound pair",
          pid in runtimes and pid in added and len(pool) >= 1)
    stop.set()


def main():
    with tempfile.TemporaryDirectory() as tmp:
        reg, pa, ta, pb, tb = test_registry(tmp)
        test_http_routing(tmp, reg, pa, ta, pb, tb)
        test_poller(tmp)
        test_sender_dryrun(tmp)
        test_stuck_take_time(tmp)
        test_handler_unbound_pair(tmp)
        test_handler_stale_registry_reload(tmp)
        test_registry_reload(tmp)
        test_reloader(tmp)
    print("ALL GATEWAY TESTS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
