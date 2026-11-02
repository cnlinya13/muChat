"""Pair registry for the single-gateway bridge.

Schema (registry.json, mode 0600, under the gateway root):
  {
    "pairs": {
      "pair-1a2b3c4d": {
        "display_name": "human label, never an identifier",
        "muse_chat_id": "<Muse chat id the hook delivers to>",
        "status": "active | disabled",
        "credential_ref": "secrets/pair-1a2b3c4d.enc",
        "hook_token_sha256": "<hex sha256 of the hook token>",
        "created_at": "<iso>",
        "token_issued_at": "<iso>"
      }
    }
  }

Security rules (hard):
- The registry NEVER holds a plaintext token, bot_token,
  context_token, or ticket. It holds a sha256 of the hook token
  and a path reference to an encrypted credential file.
- Credential files are AES-256-CBC (openssl CLI, PBKDF2) encrypted
  with a master key stored in a separate 0600 file (gateway.key)
  under the gateway root. Honest limit, stated plainly: a
  same-host key protects against accidental exposure (a leaked
  registry/queue copy reveals nothing) but NOT against an attacker
  who already has root on the host. A future SaaS step is a real
  KMS/vault; that is out of scope for this alongside build.
- Hook tokens are generated with secrets.token_urlsafe(32) and
  printed ONCE by the CLI at issuance. The operator copies the
  token into the hook's environment; it is never written to any
  file by this module. Verification uses hmac.compare_digest.
- pair_id format: pair-xxxxxxxx, 8 lowercase hex chars, generated
  at pair creation with secrets.token_hex(4). Display names are
  separate and are never used as identifiers.
"""

import hashlib
import hmac
import json
import os
import re
import secrets
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

PAIR_ID_RE = re.compile(r"^pair-[0-9a-f]{8}$")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_write_json(path: Path, data: dict, mode: int = 0o600) -> None:
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


class Registry:
    """Loads/saves registry.json and owns token + secret crypto."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.path = self.root / "registry" / "registry.json"
        self.key_path = self.root / "gateway.key"
        self.data = _load_json(self.path, {"pairs": {}})
        if not isinstance(self.data, dict) or "pairs" not in self.data:
            self.data = {"pairs": {}}

    # -- persistence -------------------------------------------------
    def save(self) -> None:
        _atomic_write_json(self.path, self.data, mode=0o600)

    def reload(self) -> None:
        """Re-read registry.json from disk. Only replaces the
        in-memory data when the file parses and carries a pairs
        map; a missing or corrupt file keeps the previous data, so
        a torn read can never wipe the registry. CLI writes are
        atomic (os.replace), so a torn read is not expected, but
        the guard costs nothing. Used by the gateway reloader to
        notice pairs bound after startup."""
        data = _load_json(self.path, None)
        if isinstance(data, dict) and isinstance(data.get("pairs"), dict):
            self.data = data

    def list_pairs(self) -> dict:
        """Ids + status only. No hashes, no refs, no secrets."""
        return {
            pid: {
                "display_name": p.get("display_name", ""),
                "status": p.get("status", ""),
                "muse_chat_id": p.get("muse_chat_id", ""),
            }
            for pid, p in self.data["pairs"].items()
        }

    def get(self, pair_id: str) -> dict | None:
        return self.data["pairs"].get(pair_id)

    # -- pair lifecycle ----------------------------------------------
    def create_pair(self, display_name: str, muse_chat_id: str) -> str:
        for _ in range(100):
            pair_id = "pair-" + secrets.token_hex(4)
            if pair_id not in self.data["pairs"]:
                break
        else:  # pragma: no cover - vanishingly unlikely
            raise RuntimeError("pair id collision")
        self.data["pairs"][pair_id] = {
            "display_name": display_name,
            "muse_chat_id": muse_chat_id,
            "status": "active",
            "credential_ref": "",
            "hook_token_sha256": "",
            "created_at": _now_iso(),
            "token_issued_at": "",
        }
        self.save()
        return pair_id

    def set_status(self, pair_id: str, status: str) -> None:
        pair = self.data["pairs"].get(pair_id)
        if pair is None:
            raise KeyError(f"unknown pair {pair_id}")
        if status not in ("active", "disabled"):
            raise ValueError(f"bad status {status}")
        pair["status"] = status
        self.save()

    # -- hook tokens ---------------------------------------------------
    def issue_token(self, pair_id: str) -> str:
        """Generate a token, store only its sha256, return the
        plaintext ONCE. Callers must not log or persist it."""
        pair = self.data["pairs"].get(pair_id)
        if pair is None:
            raise KeyError(f"unknown pair {pair_id}")
        token = secrets.token_urlsafe(32)
        pair["hook_token_sha256"] = hashlib.sha256(
            token.encode("utf-8")).hexdigest()
        pair["token_issued_at"] = _now_iso()
        self.save()
        return token

    def verify_token(self, pair_id: str, token: str) -> bool:
        """True only when the token matches THIS pair's stored hash.
        A token for pair A never verifies for pair B (isolation)."""
        if not pair_id or not token:
            return False
        pair = self.data["pairs"].get(pair_id)
        if pair is None:
            return False
        stored = pair.get("hook_token_sha256") or ""
        if not stored:
            return False
        given = hashlib.sha256(token.encode("utf-8")).hexdigest()
        return hmac.compare_digest(given, stored)

    # -- credential secrets (openssl AES-256-CBC, PBKDF2) ------------
    def _ensure_key(self) -> Path:
        if not self.key_path.exists():
            self.key_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.key_path.with_suffix(".tmp")
            tmp.write_text(secrets.token_hex(32), encoding="utf-8")
            os.chmod(tmp, 0o600)
            os.replace(tmp, self.key_path)
        return self.key_path

    def store_credentials(self, pair_id: str, creds: dict) -> str:
        """Encrypt a credential dict (bot_token, baseurl, ids) and
        record only the file reference in the registry."""
        pair = self.data["pairs"].get(pair_id)
        if pair is None:
            raise KeyError(f"unknown pair {pair_id}")
        key = self._ensure_key()
        secrets_dir = self.root / "secrets"
        secrets_dir.mkdir(parents=True, exist_ok=True)
        plain = secrets_dir / f".{pair_id}.plain.tmp"
        cipher = secrets_dir / f"{pair_id}.enc"
        try:
            plain.write_text(json.dumps(creds, ensure_ascii=False),
                             encoding="utf-8")
            os.chmod(plain, 0o600)
            subprocess.run(
                ["openssl", "enc", "-aes-256-cbc", "-pbkdf2",
                 "-pass", f"file:{key}", "-in", str(plain),
                 "-out", str(cipher)],
                check=True, capture_output=True, timeout=30)
            os.chmod(cipher, 0o600)
        finally:
            plain.unlink(missing_ok=True)
        pair["credential_ref"] = f"secrets/{pair_id}.enc"
        self.save()
        return pair["credential_ref"]

    def load_credentials(self, pair_id: str) -> dict:
        """Decrypt a pair's credentials into memory only. Never
        log or print the result; callers hold it in memory."""
        pair = self.data["pairs"].get(pair_id)
        if pair is None:
            raise KeyError(f"unknown pair {pair_id}")
        ref = pair.get("credential_ref") or ""
        if not ref:
            raise ValueError(f"no credentials stored for {pair_id}")
        cipher = self.root / ref
        key = self._ensure_key()
        out = subprocess.run(
            ["openssl", "enc", "-d", "-aes-256-cbc", "-pbkdf2",
             "-pass", f"file:{key}", "-in", str(cipher)],
            check=True, capture_output=True, timeout=30)
        return json.loads(out.stdout.decode("utf-8"))


def main() -> int:  # small self-check, no secrets printed
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        reg = Registry(Path(td))
        pid = reg.create_pair("test pair", "chat-test")
        assert PAIR_ID_RE.match(pid), pid
        token = reg.issue_token(pid)
        assert reg.verify_token(pid, token)
        assert not reg.verify_token(pid, token + "x")
        assert not reg.verify_token("pair-00000000", token)
        reg.store_credentials(pid, {"bot_token": "SYNTHETIC",
                                    "baseurl": "http://127.0.0.1:9"})
        creds = reg.load_credentials(pid)
        assert creds["bot_token"] == "SYNTHETIC"
        raw = reg.path.read_text(encoding="utf-8")
        assert "SYNTHETIC" not in raw, "plaintext leaked into registry"
        assert "token_urlsafe" not in raw
    print("registry self-check OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
