from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, padding, rsa

from splices.kalshi.auth import (
    ENV_KEY_ID,
    ENV_PRIVATE_KEY_PATH,
    ENV_PRIVATE_KEY_PEM,
    WEBSOCKET_SIGNING_PATH,
    KalshiCredentials,
    KalshiCredentialsError,
    credentials_available,
    load_credentials,
    load_private_key,
    signing_string,
)


def _rsa_pem() -> tuple[bytes, rsa.RSAPrivateKey]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pem, key


class SigningStringTests(unittest.TestCase):
    def test_shape_is_timestamp_then_method_then_path(self) -> None:
        """Concatenated with no separators. Isolated so it can be diffed against a
        known-good example without a key — the first thing to check on a 401."""
        self.assertEqual(
            signing_string(1785267959274, "GET", WEBSOCKET_SIGNING_PATH),
            "1785267959274GET/trade-api/ws/v2",
        )

    def test_method_is_upcased(self) -> None:
        self.assertEqual(signing_string(1, "get", "/p"), "1GET/p")


class SignatureTests(unittest.TestCase):
    def setUp(self) -> None:
        pem, self.key = _rsa_pem()
        self.credentials = KalshiCredentials(key_id="abc-123", private_key=self.key)
        self.pem = pem

    def test_signature_verifies_under_the_parameters_kalshi_specifies(self) -> None:
        """RSA-PSS, SHA-256 digest and MGF1, salt length equal to the digest.

        Verifying against the public key proves the parameters are internally
        consistent, which is the most that can be established without Kalshi's
        servers — but it is exactly the part most likely to be wrong.
        """
        import base64

        message = signing_string(1785267959274, "GET", WEBSOCKET_SIGNING_PATH)
        signature = base64.b64decode(self.credentials.sign(message))

        self.key.public_key().verify(
            signature,
            message.encode("utf-8"),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32),
            hashes.SHA256(),
        )

    def test_the_common_default_salt_would_be_rejected(self) -> None:
        """Guards the one parameter most libraries default differently.

        `PSS.MAX_LENGTH` is the usual default, and a verifier expecting
        `DIGEST_LENGTH` rejects it with the same 401 as a wrong key — an hour spent
        debugging the wrong thing if our signer ever drifts to it.

        Asserted in this direction because `cryptography` recovers the salt length
        when *verifying* with `MAX_LENGTH`, so signing correctly and verifying
        loosely proves nothing. Signing loosely and verifying the way Kalshi
        states it does is the check that has teeth.
        """
        from cryptography.exceptions import InvalidSignature

        message = signing_string(1, "GET", "/p")
        loose = self.key.sign(
            message.encode("utf-8"),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
            hashes.SHA256(),
        )
        with self.assertRaises(InvalidSignature):
            self.key.public_key().verify(
                loose,
                message.encode("utf-8"),
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32),
                hashes.SHA256(),
            )

    def test_headers_carry_the_key_id_and_the_timestamp_that_was_signed(self) -> None:
        headers = self.credentials.signature_headers(
            "GET", WEBSOCKET_SIGNING_PATH, timestamp_ms=1785267959274
        )
        self.assertEqual(headers["KALSHI-ACCESS-KEY"], "abc-123")
        self.assertEqual(headers["KALSHI-ACCESS-TIMESTAMP"], "1785267959274")
        self.assertTrue(headers["KALSHI-ACCESS-SIGNATURE"])

    def test_signature_changes_with_the_timestamp(self) -> None:
        """PSS is randomised, so this cannot assert a fixed string — but a header
        set that ignored the timestamp would still produce equal signatures for
        equal inputs, and that is what is being ruled out."""
        first = self.credentials.signature_headers("GET", "/p", timestamp_ms=1)
        second = self.credentials.signature_headers("GET", "/p", timestamp_ms=2)
        self.assertNotEqual(first["KALSHI-ACCESS-TIMESTAMP"], second["KALSHI-ACCESS-TIMESTAMP"])
        self.assertNotEqual(first["KALSHI-ACCESS-SIGNATURE"], second["KALSHI-ACCESS-SIGNATURE"])


class KeyLoadingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.pem, _ = _rsa_pem()
        self._directory = tempfile.TemporaryDirectory()
        self.root = Path(self._directory.name)
        self.addCleanup(self._directory.cleanup)

    def test_a_non_rsa_key_is_refused_by_name(self) -> None:
        """Kalshi signs with RSA-PSS, so an Ed25519 key cannot work and should say
        so rather than failing later inside `sign`."""
        pem = ed25519.Ed25519PrivateKey.generate().private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        with self.assertRaises(KalshiCredentialsError) as caught:
            load_private_key(pem, "test.pem")
        self.assertIn("RSA-PSS", str(caught.exception))

    def test_an_unreadable_pem_names_the_source(self) -> None:
        with self.assertRaises(KalshiCredentialsError) as caught:
            load_private_key(b"not a pem", "somewhere.pem")
        self.assertIn("somewhere.pem", str(caught.exception))

    def test_loads_from_a_key_file(self) -> None:
        path = self.root / "kalshi.pem"
        path.write_bytes(self.pem)
        credentials = load_credentials(
            env={ENV_KEY_ID: "key-1", ENV_PRIVATE_KEY_PATH: str(path)}
        )
        self.assertEqual(credentials.key_id, "key-1")

    def test_loads_from_an_inline_pem_with_escaped_newlines(self) -> None:
        """A PEM carried through an environment variable normally arrives with
        literal backslash-n, and the parser's error for that is unhelpful."""
        inline = self.pem.decode().replace("\n", "\\n")
        credentials = load_credentials(env={ENV_KEY_ID: "key-2", ENV_PRIVATE_KEY_PEM: inline})
        self.assertEqual(credentials.key_id, "key-2")

    def test_a_missing_key_id_explains_the_setup(self) -> None:
        with self.assertRaises(KalshiCredentialsError) as caught:
            load_credentials(env={})
        message = str(caught.exception)
        self.assertIn(ENV_KEY_ID, message)
        self.assertIn(ENV_PRIVATE_KEY_PATH, message)

    def test_a_missing_key_material_explains_the_setup(self) -> None:
        with self.assertRaises(KalshiCredentialsError) as caught:
            load_credentials(env={ENV_KEY_ID: "key-3"})
        self.assertIn(ENV_PRIVATE_KEY_PEM, str(caught.exception))

    def test_a_key_path_that_does_not_exist_says_so(self) -> None:
        with self.assertRaises(KalshiCredentialsError) as caught:
            load_credentials(env={ENV_KEY_ID: "k", ENV_PRIVATE_KEY_PATH: "/nope/missing.pem"})
        self.assertIn("missing.pem", str(caught.exception))

    def test_dotenv_fills_in_what_the_environment_lacks(self) -> None:
        path = self.root / "kalshi.pem"
        path.write_bytes(self.pem)
        dotenv = self.root / ".env"
        dotenv.write_text(
            f"# comment\n{ENV_KEY_ID}=from-dotenv\n{ENV_PRIVATE_KEY_PATH}=\"{path}\"\n"
        )
        credentials = load_credentials(env={}, dotenv_path=dotenv)
        self.assertEqual(credentials.key_id, "from-dotenv")

    def test_the_environment_wins_over_dotenv(self) -> None:
        path = self.root / "kalshi.pem"
        path.write_bytes(self.pem)
        dotenv = self.root / ".env"
        dotenv.write_text(f"{ENV_KEY_ID}=from-dotenv\n{ENV_PRIVATE_KEY_PATH}={path}\n")
        credentials = load_credentials(
            env={ENV_KEY_ID: "from-env", ENV_PRIVATE_KEY_PATH: str(path)}, dotenv_path=dotenv
        )
        self.assertEqual(credentials.key_id, "from-env")

    def test_availability_check_never_raises(self) -> None:
        self.assertFalse(credentials_available(env={}))


if __name__ == "__main__":
    unittest.main()


class CredentialDiscoveryTests(unittest.TestCase):
    """The two conveniences added when the first real key arrived."""

    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.root = Path(self._directory.name)
        self.addCleanup(self._directory.cleanup)
        self.pem, _ = _rsa_pem()

    def test_the_short_key_id_spelling_is_accepted(self) -> None:
        """Kalshi's console labels this "API Key ID", so both names get used.

        A splice refusing to start while a valid credential sits in `.env` under
        a name one word different is a pure waste of an outage.
        """
        path = self.root / "k.pem"
        path.write_bytes(self.pem)
        credentials = load_credentials(
            env={"KALSHI_API_KEY": "short-name", ENV_PRIVATE_KEY_PATH: str(path)}
        )
        self.assertEqual(credentials.key_id, "short-name")

    def test_a_key_file_beside_the_dotenv_is_found(self) -> None:
        (self.root / "kalshi.key").write_bytes(self.pem)
        dotenv = self.root / ".env"
        dotenv.write_text("KALSHI_API_KEY=from-dotenv\n", encoding="utf-8")
        credentials = load_credentials(env={}, dotenv_path=dotenv)
        self.assertEqual(credentials.key_id, "from-dotenv")

    def test_discovery_never_runs_without_an_explicit_dotenv(self) -> None:
        """Otherwise a caller passing an explicit environment could still pick up
        a real key from whatever is sitting in the checkout."""
        with self.assertRaises(KalshiCredentialsError):
            load_credentials(env={ENV_KEY_ID: "k"})
