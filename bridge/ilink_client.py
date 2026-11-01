"""iLink protocol layer for the WeChat ClawBot bridge (P1).

Self-written, following Tencent's official openclaw-weixin protocol.md
(current doc, read 2026-11-01), NOT the PyPI wechat-clawbot package:
the brief's queue / single-writer design needs this exact client, and
running two protocol implementations in parallel was explicitly ruled out.

Decisions recorded in P1 (official wins over the brief where they differ):
- QR creation is POST, not GET: POST /ilink/bot/get_bot_qrcode?bot_type=3
  with body {"local_token_list": []}. QR status polling is GET.
- channel_version comes from the current official doc example (2.4.8),
  not the brief's 2.0.0 / 1.0.2 examples. iLink-App-ClientVersion is
  that version encoded as 0x00MMNNPP, sent as a decimal string:
  2.4.8 -> 2*65536 + 4*256 + 8 = 132104.
- Headers: every JSON POST carries Content-Type, AuthorizationType,
  X-WECHAT-UIN (base64 of a fresh random uint32 decimal string) and
  iLink-App-Id / iLink-App-ClientVersion. Authenticated POSTs also carry
  Authorization: Bearer <bot_token> and base_info. QR creation carries
  the POST headers but NO Authorization and NO base_info. QR status GET
  carries only the iLink app headers.
- base_info: {"channel_version": "2.4.8", "bot_agent": "Muse-WeChat-Bridge/1.0"}
  bot_agent is observability-only per the official doc, not auth/routing.
- sendmessage: from_user_id "", message_type 2, message_state 2,
  client_id "openclaw-weixin-<random hex>" (unique per message),
  context_token echoed verbatim from the inbound message.
  to_user_id: the inbound sender id is used as-is when it already
  contains "@"; otherwise "@im.wechat" is appended. The official
  protocol example shows only "<target user id>", so this suffix rule
  is a P1 to-verify item against live inbound messages, not a claim.
- Return codes: HTTP 200 is NOT success; body ret/errcode must be
  checked. ret -14 = session expired (pause, do not tight-retry).
  ret -2 is NOT auto-classified as quota here: the brief calls it
  quota exhaustion, wechatbot.dev calls it parameter error, and the
  official doc does not define it as quota. Callers must log the raw
  body and treat -2 as an unclassified send failure until observed.

Security: this module is the ONLY place that ever reads bot_token
(in daemon/login via credentials.json). It never logs tokens, full
message text, qrcode values, or context_tokens.
"""

import base64
import json
import os
import random
import secrets
import time
from pathlib import Path

import requests

DEFAULT_BASE_URL = "https://ilinkai.weixin.qq.com"
DEFAULT_CDN_BASE_URL = "https://novac2c.cdn.weixin.qq.com/c2c"
CHANNEL_VERSION = "2.4.8"
CLIENT_VERSION_DECIMAL = "132104"  # 0x00020408
BOT_AGENT = "Muse-WeChat-Bridge/1.0"

BRIDGE_DIR = Path(__file__).resolve().parent.parent
STATE_DIR = BRIDGE_DIR / "state"
LOG_DIR = BRIDGE_DIR / "logs"


def _random_uin() -> str:
    """base64(String(random_uint32)), fresh for every request."""
    return base64.b64encode(str(random.randint(0, 2**32 - 1)).encode()).decode()


def _app_headers() -> dict:
    return {
        "iLink-App-Id": "bot",
        "iLink-App-ClientVersion": CLIENT_VERSION_DECIMAL,
    }


def _post_headers(token: str | None = None) -> dict:
    headers = {
        "Content-Type": "application/json",
        "AuthorizationType": "ilink_bot_token",
        "X-WECHAT-UIN": _random_uin(),
    }
    headers.update(_app_headers())
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def base_info() -> dict:
    return {"channel_version": CHANNEL_VERSION, "bot_agent": BOT_AGENT}


def _atomic_write_json(path: Path, data: dict, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.chmod(tmp, mode)
    os.replace(tmp, path)


def load_credentials() -> dict:
    path = STATE_DIR / "credentials.json"
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------- QR login

def get_bot_qrcode(base_url: str = DEFAULT_BASE_URL, timeout: int = 30) -> dict:
    """POST get_bot_qrcode. Returns the raw JSON dict (qrcode + img content)."""
    url = base_url.rstrip("/") + "/ilink/bot/get_bot_qrcode?bot_type=3"
    resp = requests.post(
        url,
        json={"local_token_list": []},
        headers=_post_headers(token=None),
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


def get_qrcode_status(qrcode: str, base_url: str = DEFAULT_BASE_URL,
                      timeout: int = 40) -> dict:
    """GET get_qrcode_status. App headers only, no auth/UIN/base_info."""
    from urllib.parse import quote

    url = (
        base_url.rstrip("/")
        + "/ilink/bot/get_qrcode_status?qrcode="
        + quote(qrcode, safe="")
    )
    resp = requests.get(url, headers=_app_headers(), timeout=timeout)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------- messaging

def getupdates(token: str, base_url: str, cursor: str, timeout: int = 50) -> dict:
    url = base_url.rstrip("/") + "/ilink/bot/getupdates"
    resp = requests.post(
        url,
        json={"get_updates_buf": cursor, "base_info": base_info()},
        headers=_post_headers(token),
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


def send_text(token: str, base_url: str, to_user_id: str,
              context_token: str, text: str, timeout: int = 30) -> dict:
    """Send one text message. Returns the raw JSON dict; caller checks ret."""
    if "@" not in to_user_id:
        to_user_id = to_user_id + "@im.wechat"
    url = base_url.rstrip("/") + "/ilink/bot/sendmessage"
    body = {
        "msg": {
            "from_user_id": "",
            "to_user_id": to_user_id,
            "client_id": "openclaw-weixin-" + secrets.token_hex(16),
            "message_type": 2,
            "message_state": 2,
            "context_token": context_token,
            "item_list": [{"type": 1, "text_item": {"text": text}}],
        },
        "base_info": base_info(),
    }
    resp = requests.post(url, json=body, headers=_post_headers(token), timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def extract_text(msg: dict) -> str:
    parts = []
    for item in msg.get("item_list") or []:
        if item.get("type") == 1 and item.get("text_item"):
            parts.append(item["text_item"].get("text", ""))
    return "".join(parts)


_MEDIA_LABELS = {2: "[图片]", 3: "[语音]", 4: "[文件]", 5: "[视频]"}


def _item_body(item: dict) -> str:
    """One-line body for a single MessageItem, following the official
    openclaw-weixin inbound.ts bodyFromItemList: text verbatim, voice
    transcription if present, otherwise a media label."""
    if not isinstance(item, dict):
        return ""
    itype = item.get("type")
    if itype == 1 and item.get("text_item"):
        return item["text_item"].get("text", "") or ""
    if itype == 3 and item.get("voice_item"):
        return item["voice_item"].get("text", "") or ""
    return _MEDIA_LABELS.get(itype, "")


def extract_quote(msg: dict) -> dict | None:
    """Quoted-message context from a getupdates message, mirroring the
    official plugin's ref_msg handling (inbound.ts: findReferenceItem /
    applyInlineQuoteContext).

    Returns None when the message quotes nothing. Otherwise returns
    {"quoted_id", "quoted_text" (may be ""), "quoted_item",
    "quoted_partial"} where quoted_item is the raw ref.message_item
    (so the caller can download quoted media), and quoted_partial is
    the ref.partial_text dict when the server sent only a substring
    hint. Callers must NOT treat an empty quoted_text as "no quote":
    it means the server sent only an id (svr_id) with no inline
    content, which can only be resolved from a local message cache.
    """
    for item in msg.get("item_list") or []:
        ref = item.get("ref_msg")
        if not ref:
            continue
        quoted_item = ref.get("message_item") or {}
        parts = []
        title = (ref.get("title") or "").strip()
        if title:
            parts.append(title)
        body = _item_body(quoted_item)
        if body:
            parts.append(body)
        quoted_id = (ref.get("svr_id") or "").strip() or \
            (quoted_item.get("msg_id") or "").strip()
        return {
            "quoted_id": quoted_id,
            "quoted_text": " | ".join(parts),
            "quoted_item": quoted_item,
            "quoted_partial": ref.get("partial_text"),
        }
    return None


# ---------------------------------------------------------------- media (P3)
# Official mirror (backend-api) states the CDN upload flow and the
# getUploadUrl fields, but gives NO worked JSON example for an
# image/video/file item in sendmessage. The item shape below is
# therefore an implementation choice to be live-tested, not a doc fact.
# media_type for getUploadUrl: 1 IMAGE, 2 VIDEO, 3 FILE, 4 VOICE
# (different numbering from MessageItem.type: 1 TEXT, 2 IMAGE,
# 3 VOICE, 4 FILE, 5 VIDEO). Do not reuse one numbering for the other.

def _aes_ecb_encrypt(data: bytes, key: bytes) -> bytes:
    from Crypto.Cipher import AES
    pad = 16 - (len(data) % 16)
    return AES.new(key, AES.MODE_ECB).encrypt(data + bytes([pad]) * pad)


def _aes_ecb_decrypt(data: bytes, key: bytes) -> bytes:
    """Strict AES-128-ECB decrypt: raises on bad length or bad PKCS7
    padding. This matters because ECB decryption with a wrong key never
    raises on its own -- without strict padding validation a wrong key
    silently returns garbage and the caller never tries the next key.
    That was the file/video download bug: the first candidate key was
    wrong, its garbage was returned as if it were the file, and the
    correct second key was never tried."""
    from Crypto.Cipher import AES
    if not data or len(data) % 16 != 0:
        raise ValueError("ciphertext length not a multiple of 16")
    out = AES.new(key, AES.MODE_ECB).decrypt(data)
    pad = out[-1]
    if not (1 <= pad <= 16) or out[-pad:] != bytes([pad]) * pad:
        raise ValueError("bad PKCS7 padding")
    return out[:-pad]


def _normalize_key(value: str) -> bytes | None:
    """Normalize an iLink AES key to 16 bytes, per the official
    pic-decrypt.ts parseAesKey (three forms, in order):
    1. 32 hex chars directly (image_item.aeskey) -> hex-decode.
    2. Base64 of 32 ASCII hex chars (common for file/voice/video
       media.aes_key) -> base64-decode, THEN hex-decode. This was the
       file/video bug: the old code returned the first 16 ASCII bytes
       of the hex string itself, which is not the key.
    3. Base64 of 16 raw bytes (common for images) -> use directly.
    Returns None if no form matches."""
    if not value:
        return None
    try:
        if len(value) == 32 and all(c in "0123456789abcdefABCDEF" for c in value):
            return bytes.fromhex(value)
    except Exception:
        pass
    try:
        raw = base64.b64decode(value, validate=False)
    except Exception:
        return None
    try:
        if len(raw) == 32 and all(c in b"0123456789abcdefABCDEF" for c in raw):
            return bytes.fromhex(raw.decode("ascii"))
    except Exception:
        pass
    if len(raw) >= 16:
        return raw[:16]
    return None


def download_media(media: dict, aeskey: str | None = None, timeout: int = 30) -> bytes:
    """Download a CDN media object and AES-128-ECB decrypt if a key is
    available. full_url first, else /download + encrypt_query_param
    (secondary, unverified). Tries each candidate key in order
    (item aeskey, then media aes_key) with strict padding validation and
    returns the first that decrypts cleanly. If no key decrypts, returns
    the raw bytes unchanged: that covers genuinely unencrypted media,
    but callers must NOT assume the result is valid plaintext -- check
    the file magic (e.g. %PDF, PK zip, ftyp) before claiming success.
    Raises on download failure."""
    url = media.get("full_url") if isinstance(media, dict) else None
    if not url and media.get("encrypt_query_param"):
        url = DEFAULT_CDN_BASE_URL.rstrip("/") + "/download" + media["encrypt_query_param"]
    if not url:
        raise ValueError("no media url")
    # Range-resume download, borrowed from gfydfhcf
    # (_http_get_range / download_media_item): up to 4 attempts,
    # resume from the bytes already received with a Range header,
    # verify against Content-Length / Content-Range when the server
    # provides one. An incomplete download after retries RAISES
    # (caller records a media error, message itself is still queued),
    # it is never returned as if it were the complete file.
    data = b""
    total = None
    for _attempt in range(4):
        headers = {}
        if data:
            headers["Range"] = f"bytes={len(data)}-"
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
        except Exception:
            time.sleep(2 * (_attempt + 1))
            continue
        cr = resp.headers.get("Content-Range")
        if cr and "/" in cr:
            try:
                total = int(cr.rsplit("/", 1)[1])
            except Exception:
                pass
        if total is None and resp.headers.get("Content-Length"):
            try:
                total = int(resp.headers["Content-Length"]) + (
                    len(data) if resp.status_code == 206 else 0)
            except Exception:
                pass
        chunk = resp.content
        if resp.status_code == 206 and data:
            data += chunk
        else:
            data = chunk
        if total is not None and len(data) >= total:
            break
        if not data:
            time.sleep(2 * (_attempt + 1))
            continue
        # No length info but a complete read: accept it; otherwise
        # retry (with Range resume) until attempts run out.
        if total is None:
            break
        time.sleep(2 * (_attempt + 1))
    if total is not None and len(data) < total:
        raise ValueError(f"incomplete download {len(data)}/{total}")
    if not data:
        raise ValueError("empty download")
    keys = []
    if aeskey:
        keys.append(aeskey)
    if media.get("aes_key"):
        keys.append(media["aes_key"])
    for k in keys:
        key = _normalize_key(k)
        if key:
            try:
                return _aes_ecb_decrypt(data, key)
            except Exception:
                continue
    return data


def get_upload_url(token: str, base_url: str, to_user_id: str,
                   media_type: int, rawsize: int, rawmd5: str,
                   filesize: int, aeskey_hex: str, timeout: int = 30) -> dict:
    url = base_url.rstrip("/") + "/ilink/bot/getuploadurl"
    body = {
        "filekey": secrets.token_hex(16),
        "media_type": media_type,
        "to_user_id": to_user_id,
        "rawsize": rawsize,
        "rawfilemd5": rawmd5,
        "filesize": filesize,
        "no_need_thumb": True,
        "aeskey": aeskey_hex,
        "base_info": base_info(),
    }
    resp = requests.post(url, json=body, headers=_post_headers(token), timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def send_image(token: str, base_url: str, to_user_id: str,
               context_token: str, file_path, timeout: int = 60) -> dict:
    """Upload an image to CDN (AES-128-ECB) then send it. Returns raw dicts.
    Raises on missing upload URL so the caller can log and move to failed
    without claiming a send. Item shape is live-test pending."""
    import hashlib
    if "@" not in to_user_id:
        to_user_id = to_user_id + "@im.wechat"
    data = Path(file_path).read_bytes()
    key = secrets.token_bytes(16)
    cipher = _aes_ecb_encrypt(data, key)
    up = get_upload_url(token, base_url, to_user_id, 1, len(data),
                        hashlib.md5(data).hexdigest(), len(cipher), key.hex())
    upload_url = up.get("upload_full_url")
    if not upload_url and up.get("upload_param"):
        upload_url = DEFAULT_CDN_BASE_URL.rstrip("/") + "/upload" + up["upload_param"]
    if not upload_url:
        raise ValueError(f"no upload url, keys={sorted(up.keys())}")
    resp = None
    for _attempt in range(3):
        resp = requests.post(upload_url, data=cipher,
                             headers={"Content-Type": "application/octet-stream"},
                             timeout=timeout)
        if resp.status_code == 200:
            break
        if 400 <= resp.status_code < 500:
            break
        time.sleep(2)
    if resp is None:
        raise ValueError("no CDN upload response")
    resp.raise_for_status()
    enc_param = resp.headers.get("x-encrypted-param") or resp.headers.get("X-Encrypted-Param")
    if not enc_param:
        raise ValueError("no x-encrypted-param from CDN upload")
    url = base_url.rstrip("/") + "/ilink/bot/sendmessage"
    body = {
        "msg": {
            "from_user_id": "",
            "to_user_id": to_user_id,
            "client_id": "openclaw-weixin-" + secrets.token_hex(16),
            "message_type": 2,
            "message_state": 2,
            "context_token": context_token,
            "item_list": [{"type": 2, "image_item": {
                "media": {"encrypt_query_param": enc_param,
                          "aes_key": base64.b64encode(key).decode(),
                          "encrypt_type": 1},
                "mid_size": len(cipher),
            }}],
        },
        "base_info": base_info(),
    }
    r2 = requests.post(url, json=body, headers=_post_headers(token), timeout=timeout)
    r2.raise_for_status()
    return {"upload": up, "send": r2.json()}


def send_image_plain(token: str, base_url: str, to_user_id: str,
                     context_token: str, file_path, timeout: int = 60) -> dict:
    """Test-only variant per the user's experience hypothesis: server is
    only a relay, do NOT add our own AES layer. Upload the original
    bytes to the CDN (getUploadUrl still requires an aeskey field, so
    a random key is supplied for the upload call only, but the bytes
    uploaded are plaintext), then sendmessage with image_item.url
    pointing at the CDN upload URL, no media.aes_key, no aeskey.
    Returns raw dicts; caller checks server ret and client receipt.
    This is a live-test hypothesis, not a confirmed protocol rule."""
    import hashlib
    if "@" not in to_user_id:
        to_user_id = to_user_id + "@im.wechat"
    data = Path(file_path).read_bytes()
    key = secrets.token_bytes(16)
    up = get_upload_url(token, base_url, to_user_id, 1, len(data),
                        hashlib.md5(data).hexdigest(), len(data), key.hex())
    upload_url = up.get("upload_full_url")
    if not upload_url and up.get("upload_param"):
        upload_url = DEFAULT_CDN_BASE_URL.rstrip("/") + "/upload" + up["upload_param"]
    if not upload_url:
        raise ValueError(f"no upload url, keys={sorted(up.keys())}")
    resp = requests.post(upload_url, data=data,
                         headers={"Content-Type": "application/octet-stream"},
                         timeout=timeout)
    resp.raise_for_status()
    url = base_url.rstrip("/") + "/ilink/bot/sendmessage"
    body = {
        "msg": {
            "from_user_id": "",
            "to_user_id": to_user_id,
            "client_id": "openclaw-weixin-" + secrets.token_hex(16),
            "message_type": 2,
            "message_state": 2,
            "context_token": context_token,
            "item_list": [{"type": 2, "image_item": {
                "url": upload_url,
                "mid_size": len(data),
            }}],
        },
        "base_info": base_info(),
    }
    r2 = requests.post(url, json=body, headers=_post_headers(token), timeout=timeout)
    r2.raise_for_status()
    return {"upload": up, "send": r2.json(), "upload_url": upload_url}


# ---------------------------------------------------------------- proactive (P4)
# A proactive writer must NOT call send_text/send_image directly.
# It writes an outbox file; the daemon sender checks window + budget
# before sending. See bridge/proactive_write.py for the writer script.

def check_proactive_window(last_context: dict, now_iso: str = "") -> dict:
    """Return {ok: bool, reason: str} for a proactive send.
    Window ~24h and budget 9 are brief/mirror figures, not verified
    official constants, so they are labelled as such in callers."""
    from datetime import datetime, timezone
    try:
        received = datetime.fromisoformat(last_context["received_at"])
        if received.tzinfo is None:
            received = received.replace(tzinfo=timezone.utc)
        now = datetime.fromisoformat(now_iso) if now_iso else datetime.now(timezone.utc)
        age_h = (now - received).total_seconds() / 3600.0
    except Exception:
        return {"ok": False, "reason": "bad_last_context"}
    if age_h > 24:
        return {"ok": False, "reason": f"window_expired age_h={age_h:.1f}"}
    return {"ok": True, "reason": f"age_h={age_h:.1f}"}


# Correction to P2 draft (2026-11-01): typing DOES exist in the official
# protocol mirror (openclaw-weixin backend-api, getConfig/sendTyping
# sections). The earlier P2 claim "no typing endpoint found" was wrong
# -- the endpoint list was read but the getConfig/sendTyping sections
# were missed. Verified shape:
#   POST <baseurl>/ilink/bot/getconfig
#     body: {"ilink_user_id": ..., "context_token": ... (optional),
#            "base_info": {...}}  -> {"ret": 0, "typing_ticket": "..."}
#   POST <baseurl>/ilink/bot/sendtyping
#     body: {"ilink_user_id": ..., "typing_ticket": ..., "status": 1|2,
#            "base_info": {...}}  -> {"ret": 0}
#   status 1 = typing, 2 = cancel. Community implementations cache
#   the ticket per user for 24h and re-send status 1 every ~5s
#   because the client-side indicator expires in 5-10s; that refresh
#   interval is community-observed, not in the official doc, so it
#   is marked as such wherever it is used.

def get_config(token: str, base_url: str, user_id: str,
               context_token: str | None = None, timeout: int = 30) -> dict:
    body = {"ilink_user_id": user_id, "base_info": base_info()}
    if context_token:
        body["context_token"] = context_token
    url = base_url.rstrip("/") + "/ilink/bot/getconfig"
    resp = requests.post(url, json=body, headers=_post_headers(token), timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def send_typing(token: str, base_url: str, user_id: str,
                typing_ticket: str, status: int, timeout: int = 30) -> dict:
    body = {
        "ilink_user_id": user_id,
        "typing_ticket": typing_ticket,
        "status": status,
        "base_info": base_info(),
    }
    url = base_url.rstrip("/") + "/ilink/bot/sendtyping"
    resp = requests.post(url, json=body, headers=_post_headers(token), timeout=timeout)
    resp.raise_for_status()
    return resp.json()
