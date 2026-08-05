"""Kalshi request signing.

Kalshi is the only venue of the three that authenticates market data, and it
authenticates *every* connection — `orderbook_delta` is a private channel and the
handshake itself is signed. So this module is the difference between the splice
running and not running, and it is deliberately the only place credentials are
read or used.

The signature is RSA-PSS over `timestamp_ms + METHOD + path`, SHA-256 for both the
digest and MGF1, salt length equal to the digest (32 bytes), base64-encoded. For
the WebSocket the method is `GET` and the path is `/trade-api/ws/v2` — the path
that is *signed*, not the full URL, and not including the query string.

**Nothing here has been exercised against Kalshi's servers.** It is written from
the published specification because the credential needed to verify it does not
exist yet. Every other venue in this project has contradicted its own
documentation at least once, so treat a 401 on first run as expected rather than
alarming, and check `signing_string()` against a known-good example before
assuming the key is wrong.
"""

from __future__ import annotations

import base64
import os
import time
from dataclasses import dataclass
from pathlib import Path

WEBSOCKET_SIGNING_PATH = "/trade-api/ws/v2"

ENV_KEY_ID = "KALSHI_API_KEY_ID"
ENV_PRIVATE_KEY_PATH = "KALSHI_PRIVATE_KEY_PATH"
ENV_PRIVATE_KEY_PEM = "KALSHI_PRIVATE_KEY_PEM"

#: Kalshi's own console labels this value "API Key ID", so both spellings get
#: reached for. Accepting the shorter one is not alias sprawl for its own sake:
#: the failure it prevents is a splice that refuses to start while a perfectly
#: valid credential sits in `.env` under a name one word different.
ENV_KEY_ID_ALIASES = (ENV_KEY_ID, "KALSHI_API_KEY")

#: Looked for when no path variable is set. A key file beside the project is the
#: obvious place to put one, and requiring a variable to point at the obvious
#: place is friction with no safety benefit — `*.key` is already gitignored.
DEFAULT_PRIVATE_KEY_FILENAMES = ("kalshi.key", "kalshi.pem")

_SETUP_HELP = f"""\
Kalshi credentials are not configured. Create an API key in the Kalshi UI, then
either put the PEM on disk and set:

    {ENV_KEY_ID}=<the key id shown in the UI>
    {ENV_PRIVATE_KEY_PATH}=/absolute/path/to/kalshi-private-key.pem

or inline it (useful in a container):

    {ENV_KEY_ID}=<the key id>
    {ENV_PRIVATE_KEY_PEM}="-----BEGIN RSA PRIVATE KEY-----\\n..."

Both may also live in the project's .env file, which is gitignored."""


class KalshiCredentialsError(RuntimeError):
    """Credentials are missing or unusable. Carries setup instructions."""


@dataclass(frozen=True)
class KalshiCredentials:
    """A key id and the private key that goes with it."""

    key_id: str
    private_key: object

    def signature_headers(self, method: str, path: str, *, timestamp_ms: int | None = None
                          ) -> dict[str, str]:
        """The three headers Kalshi requires on a signed request.

        The timestamp is injectable so a test can assert exact bytes; leaving it
        to the clock would make the signature unreproducible and the only
        assertion available would be "it did not raise".
        """
        stamp = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
        message = signing_string(stamp, method, path)
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": self.sign(message),
            "KALSHI-ACCESS-TIMESTAMP": str(stamp),
        }

    def sign(self, message: str) -> str:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding

        signature = self.private_key.sign(  # type: ignore[attr-defined]
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                # DIGEST_LENGTH, not MAX_LENGTH. PSS defaults in most libraries
                # are MAX_LENGTH, which produces a signature Kalshi rejects with
                # the same 401 as a wrong key — an hour of debugging the wrong
                # thing if it is not pinned here.
                salt_length=hashes.SHA256().digest_size,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("ascii")


def signing_string(timestamp_ms: int, method: str, path: str) -> str:
    """`timestamp + METHOD + path`, concatenated with no separators.

    Isolated and public so it can be diffed against a known-good example without
    needing a key, which is the first thing to check if the server answers 401.
    """
    return f"{timestamp_ms}{method.upper()}{path}"


def load_credentials(
    *,
    env: dict[str, str] | None = None,
    dotenv_path: Path | None = None,
) -> KalshiCredentials:
    """Reads credentials from the environment, falling back to `.env`.

    Raises `KalshiCredentialsError` with setup instructions rather than a
    `KeyError`, because the most likely reader of this failure is someone who has
    just cloned the repo and wants to know what to put where.
    """
    values = dict(env if env is not None else os.environ)
    if dotenv_path is not None and dotenv_path.exists():
        for key, value in _read_dotenv(dotenv_path).items():
            values.setdefault(key, value)

    key_id = ""
    for name in ENV_KEY_ID_ALIASES:
        key_id = (values.get(name) or "").strip()
        if key_id:
            break
    pem_path = (values.get(ENV_PRIVATE_KEY_PATH) or "").strip()
    pem_inline = values.get(ENV_PRIVATE_KEY_PEM) or ""

    if not key_id:
        raise KalshiCredentialsError(
            f"{' or '.join(ENV_KEY_ID_ALIASES)} is not set.\n\n{_SETUP_HELP}"
        )

    if pem_inline.strip():
        # A PEM carried through an environment variable usually arrives with
        # literal backslash-n rather than real newlines, and the PEM parser's
        # error for that is unhelpful.
        pem_bytes = pem_inline.replace("\\n", "\n").encode("utf-8")
        source = f"${ENV_PRIVATE_KEY_PEM}"
    elif pem_path:
        path = Path(pem_path).expanduser()
        if not path.exists():
            raise KalshiCredentialsError(
                f"{ENV_PRIVATE_KEY_PATH} points at {path}, which does not exist."
            )
        pem_bytes = path.read_bytes()
        source = str(path)
    else:
        discovered = _default_private_key(dotenv_path)
        if discovered is None:
            raise KalshiCredentialsError(
                f"Neither {ENV_PRIVATE_KEY_PATH} nor {ENV_PRIVATE_KEY_PEM} is set, "
                f"and no {' or '.join(DEFAULT_PRIVATE_KEY_FILENAMES)} was found "
                f"beside the project.\n\n{_SETUP_HELP}"
            )
        pem_bytes = discovered.read_bytes()
        source = str(discovered)

    return KalshiCredentials(key_id=key_id, private_key=load_private_key(pem_bytes, source))


def _default_private_key(dotenv_path: Path | None) -> Path | None:
    """Finds a key file beside the `.env` this call was pointed at.

    Anchored to the given `.env` and nowhere else. Searching the installed
    package's own directory as well would make the result depend on whatever
    happens to be sitting in the checkout, so a caller passing an explicit
    environment — a test, or anything constructing credentials deliberately —
    could silently pick up a real key from disk. Discovery is opt-in: no
    `dotenv_path`, no filesystem lookup.
    """
    if dotenv_path is None:
        return None
    root = Path(dotenv_path).expanduser().resolve().parent
    for name in DEFAULT_PRIVATE_KEY_FILENAMES:
        candidate = root / name
        if candidate.is_file():
            return candidate
    return None


def load_private_key(pem_bytes: bytes, source: str = "<memory>") -> object:
    """Parses an unencrypted PEM private key and refuses anything that is not RSA."""
    try:
        from cryptography.hazmat.primitives import serialization
    except ImportError as error:  # pragma: no cover - dependency is declared
        raise KalshiCredentialsError(
            "The 'cryptography' package is required to sign Kalshi requests. "
            "Install it with: python3 -m pip install cryptography"
        ) from error

    try:
        key = serialization.load_pem_private_key(pem_bytes, password=None)
    except (ValueError, TypeError) as error:
        raise KalshiCredentialsError(
            f"Could not read a private key from {source}: {error}. "
            "Kalshi issues an unencrypted RSA PEM; a passphrase-protected or "
            "PKCS#12 file has to be converted first."
        ) from error

    from cryptography.hazmat.primitives.asymmetric import rsa

    if not isinstance(key, rsa.RSAPrivateKey):
        raise KalshiCredentialsError(
            f"The key in {source} is {type(key).__name__}, but Kalshi signs with RSA-PSS."
        )
    return key


def credentials_available(*, env: dict[str, str] | None = None,
                          dotenv_path: Path | None = None) -> bool:
    """Whether `load_credentials` would succeed. Never raises."""
    try:
        load_credentials(env=env, dotenv_path=dotenv_path)
    except KalshiCredentialsError:
        return False
    return True


def _read_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values
