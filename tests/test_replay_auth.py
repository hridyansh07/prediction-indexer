from __future__ import annotations

import hashlib
import http.client
import json
import socket
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from eth_account import Account
from eth_account.messages import encode_defunct

from universe.auth import AuthError, AuthStore, NonceStore, Principal, checksum_address
from universe.api import build_server
from universe.config import AuthConfig, UniverseConfigError, load_config


UTC = timezone.utc
NOW = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
STATEMENT = "Sign in to Prediction Indexer."
ADMIN = Account.from_key("0x" + "11" * 32)
MEMBER = Account.from_key("0x" + "22" * 32)
OTHER = Account.from_key("0x" + "33" * 32)


def auth_config(*, admin_address: str = ADMIN.address) -> AuthConfig:
    return AuthConfig(
        siwe_domain="universe.example",
        siwe_uri="https://universe.example/login",
        siwe_statement=STATEMENT,
        chain_id=1,
        admin_address=admin_address,
        nonce_ttl_seconds=300,
        session_ttl_seconds=43_200,
    )


def siwe_message(
    address: str,
    nonce: str,
    *,
    expiration: datetime | None = None,
    issued_at: datetime = NOW,
    statement: str = STATEMENT,
) -> str:
    lines = [
        "universe.example wants you to sign in with your Ethereum account:",
        address,
        "",
        statement,
        "",
        "URI: https://universe.example/login",
        "Version: 1",
        "Chain ID: 1",
        f"Nonce: {nonce}",
        f"Issued At: {issued_at.isoformat().replace('+00:00', 'Z')}",
    ]
    if expiration is not None:
        lines.append(f"Expiration Time: {expiration.isoformat().replace('+00:00', 'Z')}")
    return "\n".join(lines)


class ReplayAuthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "jobs.sqlite3"
        self.now = NOW
        self.store = AuthStore(self.path, auth_config(), now=lambda: self.now)
        self.store.initialize()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def login(self, account=MEMBER) -> dict[str, str]:
        nonce = self.store.create_nonce()["nonce"]
        message = siwe_message(account.address, nonce)
        signature = Account.sign_message(encode_defunct(text=message), account.key).signature.hex()
        return self.store.verify_siwe(message, signature)

    def test_config_v3_is_closed_and_resolves_replay_database(self) -> None:
        source = Path(self.temporary.name) / "config.json"
        document = {
            "event_universe_config_version": 3,
            "database_path": "universe.sqlite3",
            "api": {"host": "127.0.0.1", "port": 8080},
            "backfill": {"temporary_directory": "tmp", "generated_start": None, "generated_end": None},
            "backup": {"directory": "backups", "object_prefix": "universe/backups"},
            "replay": {
                "database_path": "jobs.sqlite3",
                "auth": {
                    "siwe_domain": "universe.example",
                    "siwe_uri": "https://universe.example/login",
                    "siwe_statement": STATEMENT,
                    "chain_id": 1,
                    "admin_address": ADMIN.address,
                    "nonce_ttl_seconds": 300,
                    "session_ttl_seconds": 43200,
                },
                "jobs": {
                    "runner_config_path": "replay_runner.json",
                    "max_active_jobs_total": 100,
                    "max_active_jobs_per_submitter": 4,
                    "max_queued_jobs_total": 64,
                },
            },
        }
        source.write_text(json.dumps(document), encoding="utf-8")
        config = load_config(source)
        self.assertEqual(
            config.replay.database_path.resolve(), (source.parent / "jobs.sqlite3").resolve()
        )
        self.assertEqual(config.replay.auth.siwe_statement, STATEMENT)
        self.assertEqual(config.replay.auth.admin_address, ADMIN.address)
        self.assertEqual(
            config.replay.jobs.runner_config_path,
            (source.parent / "replay_runner.json").resolve(),
        )
        document["replay"]["auth"]["extra"] = True
        source.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaisesRegex(UniverseConfigError, "replay.auth fields"):
            load_config(source)

    def test_old_config_fails_actionably(self) -> None:
        source = Path(self.temporary.name) / "old.json"
        source.write_text(json.dumps({"event_universe_config_version": 1}), encoding="utf-8")
        with self.assertRaisesRegex(UniverseConfigError, "version 3"):
            load_config(source)

    def test_config_rejects_invalid_security_values(self) -> None:
        source = Path(self.temporary.name) / "config.json"
        base = {
            "event_universe_config_version": 3,
            "database_path": "universe.sqlite3",
            "api": {"host": "127.0.0.1", "port": 8080},
            "backfill": {"temporary_directory": "tmp", "generated_start": None, "generated_end": None},
            "backup": {"directory": "backups", "object_prefix": "universe/backups"},
            "replay": {
                "database_path": "jobs.sqlite3",
                "auth": {
                    "siwe_domain": "universe.example",
                    "siwe_uri": "https://universe.example/login",
                    "siwe_statement": STATEMENT,
                    "chain_id": 1,
                    "admin_address": ADMIN.address,
                    "nonce_ttl_seconds": 300,
                    "session_ttl_seconds": 43200,
                },
                "jobs": {
                    "runner_config_path": "replay_runner.json",
                    "max_active_jobs_total": 100,
                    "max_active_jobs_per_submitter": 4,
                    "max_queued_jobs_total": 64,
                },
            },
        }
        cases = (
            ("siwe_domain", "https://universe.example"),
            ("siwe_uri", "https://user@universe.example/login"),
            ("siwe_uri", "https://universe.example/login?token=x"),
            ("siwe_statement", ""),
            ("siwe_statement", "two\nlines"),
            ("siwe_statement", "x" * 257),
            ("chain_id", True),
            ("chain_id", 0),
            ("admin_address", ADMIN.address.lower()),
            ("nonce_ttl_seconds", 0),
            ("session_ttl_seconds", 604_801),
        )
        for field, value in cases:
            document = json.loads(json.dumps(base))
            document["replay"]["auth"][field] = value
            source.write_text(json.dumps(document), encoding="utf-8")
            with self.subTest(field=field, value=value):
                with self.assertRaises(UniverseConfigError):
                    load_config(source)

    def test_schema_is_idempotent_and_rejects_tampering(self) -> None:
        self.store.initialize()
        with sqlite3.connect(self.path) as connection:
            connection.execute("DROP TRIGGER allowlist_events_no_delete")
            connection.execute(
                "CREATE TRIGGER allowlist_events_no_delete BEFORE DELETE ON allowlist_events "
                "BEGIN SELECT RAISE(ABORT, 'wrong'); END"
            )
        with self.assertRaisesRegex(ValueError, "invalid replay auth schema"):
            self.store.initialize()

    def test_schema_constraints_reject_invalid_auth_rows(self) -> None:
        with sqlite3.connect(self.path) as connection:
            invalid_statements = (
                (
                    "INSERT INTO sessions VALUES (?,?,?,?,?,NULL)",
                    ("0" * 64, "0x" + "g" * 40, "member", 1, 2),
                ),
                (
                    "INSERT INTO allowlist VALUES (?,?,?,?)",
                    (MEMBER.address.lower(), "bad\nnote", 1, ADMIN.address.lower()),
                ),
            )
            for statement, values in invalid_statements:
                with self.subTest(statement=statement):
                    with self.assertRaises(sqlite3.IntegrityError):
                        connection.execute(statement, values)

    def test_offline_login_hashes_token_and_returns_lowercase_principal(self) -> None:
        self.store.add_member(MEMBER.address, "member", ADMIN.address)
        result = self.login()
        token = result["token"]
        principal = self.store.authenticate({"Authorization": f"Bearer {token}"})
        self.assertEqual(principal, Principal(MEMBER.address.lower(), "member"))
        self.assertEqual(result["address"], checksum_address(MEMBER.address))
        with sqlite3.connect(self.path) as connection:
            row = connection.execute("SELECT token_hash FROM sessions").fetchone()
            dump = "\n".join(connection.iterdump())
        self.assertEqual(row[0], hashlib.sha256(token.encode("ascii")).hexdigest())
        self.assertNotIn(token, dump)
        self.assertNotIn(STATEMENT, dump)
        self.assertNotIn("signature", dump)

    def test_bad_signature_does_not_consume_nonce(self) -> None:
        self.store.add_member(MEMBER.address, "member", ADMIN.address)
        nonce = self.store.create_nonce()["nonce"]
        message = siwe_message(MEMBER.address, nonce)
        bad = Account.sign_message(encode_defunct(text=message), ADMIN.key).signature.hex()
        with self.assertRaises(AuthError):
            self.store.verify_siwe(message, bad)
        good = Account.sign_message(encode_defunct(text=message), MEMBER.key).signature.hex()
        self.assertIn("token", self.store.verify_siwe(message, good))

    def test_siwe_verification_matrix_rejects_wrong_contract_fields(self) -> None:
        self.store.add_member(MEMBER.address, "member", ADMIN.address)
        mutations = (
            lambda value: value.replace("universe.example wants", "evil.example wants", 1),
            lambda value: value.replace("URI: https://universe.example/login", "URI: https://evil.example/login"),
            lambda value: value.replace("Version: 1", "Version: 2"),
            lambda value: value.replace("Chain ID: 1", "Chain ID: 2"),
            lambda value: value.replace(STATEMENT, "Sign in to something else."),
            lambda value: value.replace(f"\n{STATEMENT}\n", "\n"),
            lambda value: value.replace("Issued At: 2026-01-02T03:04:05Z", "Issued At: 2026-01-02 03:04:05"),
            lambda value: value + "\nNot Before: 2026-01-02T03:04:06Z",
            lambda value: value + "\nExpiration Time: 2026-01-02T03:04:05Z",
        )
        for mutate in mutations:
            nonce = self.store.create_nonce()["nonce"]
            valid = siwe_message(MEMBER.address, nonce)
            message = mutate(valid)
            signature = Account.sign_message(encode_defunct(text=message), MEMBER.key).signature.hex()
            with self.subTest(message=message):
                with self.assertRaisesRegex(AuthError, "authentication failed"):
                    self.store.verify_siwe(message, signature)
                valid_signature = Account.sign_message(
                    encode_defunct(text=valid), MEMBER.key
                ).signature.hex()
                self.assertIn("token", self.store.verify_siwe(valid, valid_signature))

    def test_session_uses_signed_expiration_and_expires_at_boundary(self) -> None:
        self.store.add_member(MEMBER.address, "member", ADMIN.address)
        nonce = self.store.create_nonce()["nonce"]
        expiration = NOW + timedelta(seconds=30)
        message = siwe_message(MEMBER.address, nonce, expiration=expiration)
        signature = Account.sign_message(encode_defunct(text=message), MEMBER.key).signature.hex()
        result = self.store.verify_siwe(message, signature)
        self.assertEqual(result["expires_at"], "2026-01-02T03:04:35Z")
        self.now = expiration
        self.assertIsNone(
            self.store.authenticate({"Authorization": f"Bearer {result['token']}"})
        )

    def test_nonce_is_single_use_under_concurrency(self) -> None:
        self.store.add_member(MEMBER.address, "member", ADMIN.address)
        nonce = self.store.create_nonce()["nonce"]
        message = siwe_message(MEMBER.address, nonce)
        signature = Account.sign_message(encode_defunct(text=message), MEMBER.key).signature.hex()
        barrier = threading.Barrier(2)
        outcomes: list[bool] = []

        def attempt() -> None:
            barrier.wait()
            try:
                self.store.verify_siwe(message, signature)
                outcomes.append(True)
            except AuthError:
                outcomes.append(False)

        threads = [threading.Thread(target=attempt) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(outcomes), [False, True])

    def test_login_and_removal_serialize_without_a_live_session(self) -> None:
        for _ in range(8):
            self.store.add_member(MEMBER.address, "member", ADMIN.address)
            nonce = self.store.create_nonce()["nonce"]
            message = siwe_message(MEMBER.address, nonce)
            signature = Account.sign_message(
                encode_defunct(text=message), MEMBER.key
            ).signature.hex()
            barrier = threading.Barrier(2)
            tokens: list[str] = []

            def login() -> None:
                barrier.wait()
                try:
                    tokens.append(self.store.verify_siwe(message, signature)["token"])
                except AuthError:
                    pass

            def remove() -> None:
                barrier.wait()
                self.store.remove_member(MEMBER.address, ADMIN.address)

            threads = [threading.Thread(target=login), threading.Thread(target=remove)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            for token in tokens:
                self.assertIsNone(
                    self.store.authenticate({"Authorization": f"Bearer {token}"})
                )

    def test_logout_removal_expiry_and_admin_rotation(self) -> None:
        self.store.add_member(MEMBER.address, "member", ADMIN.address)
        member = self.login()
        self.store.logout({"Authorization": f"Bearer {member['token']}"})
        self.assertIsNone(self.store.authenticate({"Authorization": f"Bearer {member['token']}"}))
        member = self.login()
        self.store.remove_member(MEMBER.address, ADMIN.address)
        self.assertIsNone(self.store.authenticate({"Authorization": f"Bearer {member['token']}"}))

        admin = self.login(ADMIN)
        rotated = AuthStore(self.path, auth_config(admin_address=MEMBER.address), now=lambda: self.now)
        rotated.initialize()
        self.assertIsNone(rotated.authenticate({"Authorization": f"Bearer {admin['token']}"}))

    def test_allowlist_idempotency_audit_immutability_order_and_pruning(self) -> None:
        first = self.store.add_member(MEMBER.address, "hello", ADMIN.address)
        second = self.store.add_member(MEMBER.address, "hello", ADMIN.address)
        self.store.add_member(OTHER.address, "other", ADMIN.address)
        self.store.create_nonce()
        self.login()
        self.assertEqual(first, second)
        self.assertEqual(
            [row["address"].lower() for row in self.store.list_members()],
            sorted([MEMBER.address.lower(), OTHER.address.lower()]),
        )
        with sqlite3.connect(self.path) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM allowlist_events").fetchone()[0], 2)
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
            self.assertNotIn("nonces", tables)
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("DELETE FROM allowlist_events")
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("UPDATE allowlist_events SET action = 'remove'")
        self.now += timedelta(days=2)
        self.store.prune()
        with sqlite3.connect(self.path) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM allowlist_events").fetchone()[0], 2)
            self.assertEqual(connection.execute("SELECT count(*) FROM sessions").fetchone()[0], 0)

    def test_client_clock_drift_does_not_matter(self) -> None:
        """Freshness is the server-side nonce TTL, not the browser's Issued At."""
        self.store.add_member(MEMBER.address, "member", ADMIN.address)
        for drift in (timedelta(hours=-3), timedelta(seconds=-2), timedelta(seconds=2), timedelta(hours=3)):
            with self.subTest(drift=drift):
                nonce = self.store.create_nonce()["nonce"]
                message = siwe_message(MEMBER.address, nonce, issued_at=NOW + drift)
                signature = Account.sign_message(encode_defunct(text=message), MEMBER.key).signature.hex()
                self.assertIn("token", self.store.verify_siwe(message, signature))

    def test_nonce_expires_by_server_ttl(self) -> None:
        self.store.add_member(MEMBER.address, "member", ADMIN.address)
        nonce = self.store.create_nonce()["nonce"]
        message = siwe_message(MEMBER.address, nonce)
        signature = Account.sign_message(encode_defunct(text=message), MEMBER.key).signature.hex()
        self.now = NOW + timedelta(seconds=300)
        with self.assertRaisesRegex(AuthError, "authentication failed"):
            self.store.verify_siwe(message, signature)

    def test_unknown_nonce_is_rejected(self) -> None:
        self.store.add_member(MEMBER.address, "member", ADMIN.address)
        message = siwe_message(MEMBER.address, "0" * 32)
        signature = Account.sign_message(encode_defunct(text=message), MEMBER.key).signature.hex()
        with self.assertRaisesRegex(AuthError, "authentication failed"):
            self.store.verify_siwe(message, signature)

    def test_restart_drops_outstanding_nonces(self) -> None:
        self.store.add_member(MEMBER.address, "member", ADMIN.address)
        nonce = self.store.create_nonce()["nonce"]
        message = siwe_message(MEMBER.address, nonce)
        signature = Account.sign_message(encode_defunct(text=message), MEMBER.key).signature.hex()
        restarted = AuthStore(self.path, auth_config(), now=lambda: self.now)
        restarted.initialize()
        with self.assertRaisesRegex(AuthError, "authentication failed"):
            restarted.verify_siwe(message, signature)
        with sqlite3.connect(self.path) as connection:
            self.assertNotIn(nonce, "\n".join(connection.iterdump()))

    def test_nonce_store_is_capped_and_prunes_expired(self) -> None:
        store = NonceStore(ttl_seconds=10, capacity=3)
        for _ in range(3):
            store.issue(100)
        with self.assertRaises(AuthError) as raised:
            store.issue(105)
        self.assertEqual(raised.exception.status, 503)
        store.issue(110)  # all three expired at 110 and were pruned
        self.assertEqual(len(store), 1)

    def test_nonce_consume_is_single_use_and_bounded_by_expiry(self) -> None:
        store = NonceStore(ttl_seconds=10)
        nonce, expires = store.issue(100)
        self.assertEqual(expires, 110)
        self.assertTrue(store.consume(nonce, 109))
        self.assertFalse(store.consume(nonce, 109))
        late, _ = store.issue(100)
        self.assertFalse(store.consume(late, 110))

    def test_placeholder_admin_disables_sign_in(self) -> None:
        store = AuthStore(self.path, auth_config(admin_address="0x" + "0" * 40), now=lambda: self.now)
        store.initialize()
        self.assertFalse(store.configured)
        for call in (store.create_nonce, lambda: store.verify_siwe("x", "y")):
            with self.assertRaises(AuthError) as raised:
                call()
            self.assertEqual(raised.exception.status, 503)

    def test_configured_admin_cannot_be_removed(self) -> None:
        with self.assertRaises(AuthError) as raised:
            self.store.remove_member(ADMIN.address, ADMIN.address)
        self.assertEqual(raised.exception.status, 403)

    def test_restart_preserves_membership_and_session(self) -> None:
        self.store.add_member(MEMBER.address, "member", ADMIN.address)
        result = self.login()
        restarted = AuthStore(self.path, auth_config(), now=lambda: self.now)
        restarted.initialize()
        self.assertEqual(restarted.list_members()[0]["address"], MEMBER.address)
        self.assertEqual(
            restarted.authenticate({"Authorization": f"Bearer {result['token']}"}),
            Principal(MEMBER.address.lower(), "member"),
        )

    def test_authorization_header_is_exact_and_roles_are_enforced(self) -> None:
        self.store.add_member(MEMBER.address, "member", ADMIN.address)
        token = self.login()["token"]
        for headers in ({}, {"Authorization": "Basic x"}, {"Authorization": [f"Bearer {token}", "Bearer x"]}):
            with self.subTest(headers=headers):
                with self.assertRaisesRegex(AuthError, "authentication required") as raised:
                    self.store.require_member(headers)
                self.assertEqual(raised.exception.status, 401)
        with self.assertRaises(AuthError) as raised:
            self.store.require_admin({"Authorization": f"Bearer {token}"})
        self.assertEqual(raised.exception.status, 403)


class _Database:
    def status(self) -> dict[str, bool]:
        return {"ok": True}


class ReplayAuthHTTPTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.now = NOW
        self.auth = AuthStore(
            Path(self.temporary.name) / "jobs.sqlite3",
            auth_config(),
            now=lambda: self.now,
        )
        self.auth.initialize()
        self.server = build_server(_Database(), self.auth, "127.0.0.1", 0)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.port = self.server.server_address[1]

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temporary.cleanup()

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        result = response.status, dict(response.getheaders()), response.read()
        connection.close()
        return result

    def raw(self, request: bytes) -> bytes:
        with socket.create_connection(("127.0.0.1", self.port), timeout=3) as client:
            client.sendall(request)
            client.shutdown(socket.SHUT_WR)
            chunks = []
            while True:
                chunk = client.recv(65536)
                if not chunk:
                    return b"".join(chunks)
                chunks.append(chunk)

    def admin_token(self) -> str:
        nonce = self.auth.create_nonce()["nonce"]
        message = siwe_message(ADMIN.address, nonce)
        signature = Account.sign_message(encode_defunct(text=message), ADMIN.key).signature.hex()
        return self.auth.verify_siwe(message, signature)["token"]

    def test_public_nonce_and_existing_get_are_no_store(self) -> None:
        status, headers, payload = self.request("GET", "/v1/auth/nonce")
        self.assertEqual(status, 200)
        self.assertRegex(json.loads(payload)["nonce"], r"^[0-9a-f]{32}$")
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertEqual(int(headers["Content-Length"]), len(payload))
        status, _, payload = self.request("GET", "/healthz")
        self.assertEqual((status, json.loads(payload)), (200, {"ok": True}))

    def test_framing_rejects_missing_duplicate_invalid_length_and_transfer_encoding(self) -> None:
        base = b"POST /v1/auth/logout HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n"
        cases = [
            (base + b"\r\n", b" 411 "),
            (base + b"Content-Length: 2\r\nContent-Length: 2\r\n\r\n{}", b" 400 "),
            (base + b"Content-Length: nope\r\n\r\n", b" 400 "),
            (base + b"Content-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n{}", b" 400 "),
        ]
        for request, expected in cases:
            with self.subTest(expected=expected):
                response = self.raw(request)
                self.assertIn(expected, response.split(b"\r\n", 1)[0])
                self.assertIn(b"Connection: close", response)

    def test_size_boundary_media_type_and_invalid_json(self) -> None:
        status, _, _ = self.request(
            "POST", "/v1/auth/logout", b"{}", {"Content-Type": "text/plain"}
        )
        self.assertEqual(status, 415)
        for payload in (b"[1]", b"{", b'{"x":1,"x":2}', b'{"unknown":1}'):
            status, _, _ = self.request(
                "POST", "/v1/auth/logout", payload, {"Content-Type": "application/json"}
            )
            self.assertEqual(status, 400)
        payload = b'{"x":"' + b"a" * 65_528 + b'"}'
        self.assertEqual(len(payload), 65_536)
        status, _, _ = self.request(
            "POST", "/v1/auth/logout", payload, {"Content-Type": "application/json"}
        )
        self.assertEqual(status, 400)
        status, _, _ = self.request(
            "POST", "/v1/auth/logout", payload + b" ", {"Content-Type": "application/json"}
        )
        self.assertEqual(status, 413)

    def test_admin_routes_logout_and_delete_body_contract(self) -> None:
        token = self.admin_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=UTF-8",
        }
        body = json.dumps({"address": MEMBER.address, "note": "member"}).encode()
        status, _, payload = self.request("POST", "/v1/admin/allowlist", body, headers)
        self.assertEqual((status, json.loads(payload)["address"]), (200, MEMBER.address))
        status, _, payload = self.request("GET", "/v1/admin/allowlist", headers=headers)
        self.assertEqual(json.loads(payload)["members"][0]["address"], MEMBER.address)
        status, _, _ = self.request(
            "DELETE", f"/v1/admin/allowlist/{MEMBER.address}", b'{"x":1}', headers
        )
        self.assertEqual(status, 400)
        status, _, _ = self.request(
            "DELETE", f"/v1/admin/allowlist/{MEMBER.address}", b"{}", headers
        )
        self.assertEqual(status, 200)
        status, response_headers, _ = self.request(
            "POST", "/v1/auth/logout", b"{}", headers
        )
        self.assertEqual(status, 200)
        status, response_headers, _ = self.request("GET", "/v1/admin/allowlist", headers=headers)
        self.assertEqual(status, 401)
        self.assertEqual(response_headers["WWW-Authenticate"], "Bearer")

    def test_siwe_route_performs_offline_login(self) -> None:
        self.auth.add_member(MEMBER.address, "member", ADMIN.address)
        status, _, payload = self.request("GET", "/v1/auth/nonce")
        nonce = json.loads(payload)["nonce"]
        message = siwe_message(MEMBER.address, nonce)
        signature = Account.sign_message(
            encode_defunct(text=message), MEMBER.key
        ).signature.hex()
        body = json.dumps({"message": message, "signature": signature}).encode()
        status, headers, payload = self.request(
            "POST", "/v1/auth/siwe", body, {"Content-Type": "application/json"}
        )
        document = json.loads(payload)
        self.assertEqual((status, document["address"], document["role"]), (200, MEMBER.address, "member"))
        self.assertEqual(headers["Cache-Control"], "no-store")

    def test_internal_error_is_redacted(self) -> None:
        original = self.auth.create_nonce

        def fail() -> dict[str, str]:
            raise RuntimeError("secret-signature-token")

        self.auth.create_nonce = fail  # type: ignore[method-assign]
        try:
            status, _, payload = self.request("GET", "/v1/auth/nonce")
        finally:
            self.auth.create_nonce = original  # type: ignore[method-assign]
        self.assertEqual(status, 500)
        self.assertEqual(json.loads(payload), {"error": "internal server error"})
        self.assertNotIn(b"secret", payload)


if __name__ == "__main__":
    unittest.main()
