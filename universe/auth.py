"""Offline SIWE authentication and durable Replay authorization state."""

from __future__ import annotations

import hashlib
import re
import secrets
import sqlite3
import threading
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

from eth_account import Account
from eth_account.messages import encode_defunct
from eth_utils import is_address, is_checksum_address, to_checksum_address

from universe.config import AuthConfig


SCHEMA_PATH = Path(__file__).with_name("schema") / "replay_auth.sql"
ADDRESS_RE = re.compile(r"0x[0-9a-fA-F]{40}\Z")
NONCE_RE = re.compile(r"[0-9a-f]{32}\Z")
TIMESTAMP_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\Z"
)
#: Upper bound on outstanding nonces. The nonce route is public, so the store is
#: capped rather than grown on demand; Caddy rate limiting is the first defence.
MAX_LIVE_NONCES = 500
ZERO_ADDRESS = "0x" + "0" * 40


@dataclass(frozen=True)
class Principal:
    address: str
    role: str


class AuthError(Exception):
    """An HTTP-safe authentication or authorization failure."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


class NonceStore:
    """Single-use SIWE nonces held in process memory.

    Freshness is the server's own issue time plus the configured TTL; a
    client's ``Issued At`` is never trusted for it. Nonces are lost on restart,
    which only means the user signs in again. This requires one Universe server
    process: a second worker or replica would need shared storage instead.
    """

    def __init__(self, ttl_seconds: int, capacity: int = MAX_LIVE_NONCES) -> None:
        self._ttl = ttl_seconds
        self._capacity = capacity
        self._expiry: dict[str, int] = {}
        self._lock = threading.Lock()

    def issue(self, now: int) -> tuple[str, int]:
        with self._lock:
            self._prune(now)
            if len(self._expiry) >= self._capacity:
                raise AuthError(503, "too many pending sign-ins; retry shortly")
            nonce = secrets.token_hex(16)
            expires = now + self._ttl
            self._expiry[nonce] = expires
            return nonce, expires

    def consume(self, nonce: str, now: int) -> bool:
        """Atomically remove ``nonce``; true only if it was live."""
        with self._lock:
            expires = self._expiry.pop(nonce, None)
            return expires is not None and now < expires

    def __len__(self) -> int:
        with self._lock:
            return len(self._expiry)

    def _prune(self, now: int) -> None:
        expired = [nonce for nonce, expires in self._expiry.items() if expires <= now]
        for nonce in expired:
            del self._expiry[nonce]


class AuthStore:
    def __init__(
        self,
        path: Path,
        config: AuthConfig,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self.config = config
        self._now = now or (lambda: datetime.now(timezone.utc))
        self.nonces = NonceStore(config.nonce_ttl_seconds)

    @property
    def configured(self) -> bool:
        """False while the shipped zero-address admin placeholder is in place."""
        return self.config.admin_address.lower() != ZERO_ADDRESS

    def _require_configured(self) -> None:
        if not self.configured:
            raise AuthError(503, "replay authentication is not configured")

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as connection:
            try:
                connection.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
                connection.commit()
            except sqlite3.DatabaseError as error:
                raise ValueError("database contains an invalid replay auth schema") from error
            expected = self._expected_objects()
            actual = self._schema_objects(connection)
            if any(actual.get(key) != sql for key, sql in expected.items()):
                raise ValueError("database contains an invalid replay auth schema")

    @staticmethod
    def _schema_objects(connection: sqlite3.Connection) -> dict[tuple[str, str], str]:
        return {
            (str(row[0]), str(row[1])): " ".join(str(row[2]).split())
            for row in connection.execute(
                "SELECT type, name, sql FROM sqlite_master "
                "WHERE type IN ('table','index','trigger') AND sql IS NOT NULL "
                "AND name NOT LIKE 'sqlite_%'"
            )
        }

    @classmethod
    def _expected_objects(cls) -> dict[tuple[str, str], str]:
        with closing(sqlite3.connect(":memory:")) as connection:
            connection.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
            return cls._schema_objects(connection)

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def create_nonce(self) -> dict[str, str]:
        self._require_configured()
        nonce, expires = self.nonces.issue(self._timestamp())
        return {"nonce": nonce, "expires_at": _format_timestamp(expires)}

    def verify_siwe(self, message: str, signature: str) -> dict[str, str]:
        self._require_configured()
        try:
            parsed = self._parse_siwe(message)
            recovered = Account.recover_message(
                encode_defunct(text=message), signature=signature
            )
        except Exception as error:
            raise AuthError(401, "authentication failed") from error
        if recovered.lower() != parsed["address"].lower():
            raise AuthError(401, "authentication failed")

        now = self._timestamp()
        # Only a correctly signed message spends its nonce.
        if not self.nonces.consume(parsed["nonce"], now):
            raise AuthError(401, "authentication failed")
        token = secrets.token_hex(32)
        digest = token_digest(token)
        address = parsed["address"].lower()
        with self._write() as connection:
            role = self._current_role(connection, address)
            if role is None:
                raise AuthError(401, "authentication failed")
            expires = now + self.config.session_ttl_seconds
            if parsed["expiration"] is not None:
                expires = min(expires, parsed["expiration"])
            if now >= expires:
                raise AuthError(401, "authentication failed")
            connection.execute(
                "INSERT INTO sessions(token_hash,address,role,created_at,expires_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (digest, address, role, now, expires),
            )
            self._prune(connection, now)
        return {
            "token": token,
            "address": checksum_address(address),
            "role": role,
            "expires_at": _format_timestamp(expires),
        }

    def authenticate(self, headers: Any) -> Principal | None:
        token = _bearer_token(headers)
        if token is None:
            return None
        now = self._timestamp()
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT address, role FROM sessions "
                "WHERE token_hash = ? AND revoked_at IS NULL AND expires_at > ?",
                (token_digest(token), now),
            ).fetchone()
            if row is None:
                return None
            role = self._current_role(connection, str(row["address"]))
            if role is None or role != row["role"]:
                return None
            return Principal(str(row["address"]), role)

    def require_member(self, headers: Any) -> Principal:
        principal = self.authenticate(headers)
        if principal is None:
            raise AuthError(401, "authentication required")
        return principal

    def require_admin(self, headers: Any) -> Principal:
        principal = self.require_member(headers)
        if principal.role != "admin":
            raise AuthError(403, "forbidden")
        return principal

    def logout(self, headers: Any) -> None:
        self.require_member(headers)
        token = _bearer_token(headers)
        assert token is not None
        now = self._timestamp()
        with self._write() as connection:
            connection.execute(
                "UPDATE sessions SET revoked_at = ? WHERE token_hash = ? AND revoked_at IS NULL",
                (now, token_digest(token)),
            )

    def list_members(self) -> list[dict[str, str]]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT address, note FROM allowlist ORDER BY address"
            ).fetchall()
        return [
            {"address": checksum_address(str(row["address"])), "note": str(row["note"])}
            for row in rows
        ]

    def add_member(self, address: str, note: str, actor_address: str) -> dict[str, str]:
        normalized = _lower_address(address)
        actor = _lower_address(actor_address)
        _validate_note(note)
        now = self._timestamp()
        with self._write() as connection:
            existing = connection.execute(
                "SELECT note FROM allowlist WHERE address = ?", (normalized,)
            ).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO allowlist(address,note,created_at,created_by) VALUES (?,?,?,?)",
                    (normalized, note, now, actor),
                )
                connection.execute(
                    "INSERT INTO allowlist_events(action,address,note,actor_address,created_at) "
                    "VALUES ('add',?,?,?,?)",
                    (normalized, note, actor, now),
                )
                stored_note = note
            else:
                stored_note = str(existing["note"])
        return {"address": checksum_address(normalized), "note": stored_note}

    def remove_member(self, address: str, actor_address: str) -> dict[str, str]:
        normalized = _lower_address(address)
        actor = _lower_address(actor_address)
        if normalized == self.config.admin_address.lower():
            raise AuthError(403, "configured admin cannot be removed")
        now = self._timestamp()
        with self._write() as connection:
            row = connection.execute(
                "SELECT note FROM allowlist WHERE address = ?", (normalized,)
            ).fetchone()
            if row is None:
                raise AuthError(404, "member not found")
            connection.execute("DELETE FROM allowlist WHERE address = ?", (normalized,))
            connection.execute(
                "UPDATE sessions SET revoked_at = ? WHERE address = ? AND revoked_at IS NULL",
                (now, normalized),
            )
            connection.execute(
                "INSERT INTO allowlist_events(action,address,note,actor_address,created_at) "
                "VALUES ('remove',?,?,?,?)",
                (normalized, str(row["note"]), actor, now),
            )
        return {"address": checksum_address(normalized)}

    def prune(self) -> None:
        with self._write() as connection:
            self._prune(connection, self._timestamp())

    @staticmethod
    def _prune(connection: sqlite3.Connection, now: int) -> None:
        connection.execute("DELETE FROM sessions WHERE expires_at <= ?", (now,))

    def _current_role(self, connection: sqlite3.Connection, address: str) -> str | None:
        if address == self.config.admin_address.lower():
            return "admin"
        row = connection.execute(
            "SELECT 1 FROM allowlist WHERE address = ?", (address,)
        ).fetchone()
        return "member" if row is not None else None

    def _parse_siwe(self, message: str) -> dict[str, Any]:
        if not isinstance(message, str) or "\r" in message or len(message) > 16_384:
            raise ValueError("invalid message")
        lines = message.split("\n")
        if len(lines) < 9 or lines[0] != f"{self.config.siwe_domain} wants you to sign in with your Ethereum account:":
            raise ValueError("invalid domain")
        address = lines[1]
        if not ADDRESS_RE.fullmatch(address) or not is_checksum_address(address) or lines[2] != "":
            raise ValueError("invalid address")
        # EIP-4361 with-statement layout; the statement is the configured one.
        if lines[3] != self.config.siwe_statement or lines[4] != "":
            raise ValueError("invalid statement")
        uri_index = 5
        fields: dict[str, str] = {}
        resources = False
        resource_count = 0
        order = {
            "URI": 0,
            "Version": 1,
            "Chain ID": 2,
            "Nonce": 3,
            "Issued At": 4,
            "Expiration Time": 5,
            "Not Before": 6,
            "Request ID": 7,
        }
        previous = -1
        for line in lines[uri_index:]:
            if resources:
                if not line.startswith("- ") or len(line) == 2:
                    raise ValueError("invalid resource")
                resource_count += 1
                continue
            if line == "Resources:":
                resources = True
                continue
            if ": " not in line:
                raise ValueError("invalid field")
            key, value = line.split(": ", 1)
            if key in fields or key not in order or order[key] <= previous:
                raise ValueError("invalid field")
            previous = order[key]
            fields[key] = value
        required = {"URI", "Version", "Chain ID", "Nonce", "Issued At"}
        if not required.issubset(fields) or fields["URI"] != self.config.siwe_uri:
            raise ValueError("invalid uri")
        if resources and resource_count == 0:
            raise ValueError("invalid resources")
        if fields["Version"] != "1" or fields["Chain ID"] != str(self.config.chain_id):
            raise ValueError("invalid network")
        if not NONCE_RE.fullmatch(fields["Nonce"]):
            raise ValueError("invalid nonce")
        # Issued At must be well formed but is not compared with the server clock:
        # freshness is the server-side nonce TTL, so browser clock drift is harmless.
        issued = _parse_timestamp(fields["Issued At"])
        expiration = (
            _parse_timestamp(fields["Expiration Time"])
            if "Expiration Time" in fields
            else None
        )
        now = self._timestamp()
        if "Not Before" in fields and _parse_timestamp(fields["Not Before"]) > now:
            raise ValueError("not before")
        if expiration is not None and now >= expiration:
            raise ValueError("expired")
        if expiration is not None and expiration <= issued:
            raise ValueError("invalid expiration")
        return {
            "address": address,
            "nonce": fields["Nonce"],
            "expiration": expiration,
        }

    def _timestamp(self) -> int:
        value = self._now()
        if value.tzinfo is None:
            raise ValueError("auth clock must be timezone-aware")
        return int(value.timestamp())


def checksum_address(address: str) -> str:
    if not is_address(address):
        raise ValueError("invalid Ethereum address")
    return to_checksum_address(address)


def token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("ascii")).hexdigest()


def _lower_address(address: str) -> str:
    if not isinstance(address, str) or not ADDRESS_RE.fullmatch(address):
        raise ValueError("address must be a 20-byte Ethereum address")
    return address.lower()


def _validate_note(note: str) -> None:
    if (
        not isinstance(note, str)
        or len(note) > 256
        or any(ord(character) < 32 or ord(character) > 126 for character in note)
    ):
        raise ValueError("note must contain at most 256 printable ASCII characters")


def _bearer_token(headers: Any) -> str | None:
    if hasattr(headers, "get_all"):
        values = headers.get_all("Authorization", [])
    elif isinstance(headers, Mapping):
        matches = [value for key, value in headers.items() if str(key).lower() == "authorization"]
        values = matches[0] if len(matches) == 1 else matches
        if isinstance(values, str):
            values = [values]
    else:
        return None
    if len(values) != 1 or not isinstance(values[0], str):
        return None
    match = re.fullmatch(r"Bearer ([0-9a-f]{64})", values[0])
    return match.group(1) if match else None


def _parse_timestamp(value: str) -> int:
    if not isinstance(value, str) or not TIMESTAMP_RE.fullmatch(value):
        raise ValueError("timestamp must be UTC")
    parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("timestamp must be UTC")
    return int(parsed.timestamp())


def _format_timestamp(value: int) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")
