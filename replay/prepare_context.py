"""Local preparation CLI; UNIVERSE_BASE_URL maps to UniverseHTTP only."""

import argparse
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

from replay.preparation import MAX_BYTES, UniverseHTTP, prepare
from replay.streams.protocol import decode, ProtocolError


_SETTING = re.compile(r"^\s*(?:export\s+)?UNIVERSE_BASE_URL\s*=\s*(.*)$")
_VALUE = re.compile(r"^(?:\"([^\"]*)\"|'([^']*)'|([^\s]+?))(?:\s+#.*)?\s*$")


def universe_from_environment(*, env_file=Path(".env"), environ=None):
    """Exported setting wins; otherwise parse only this dotenv key, without execution.

    No expansion, credential loading, environment mutation or URL logging occurs.
    """
    environ = os.environ if environ is None else environ
    url = environ.get("UNIVERSE_BASE_URL")
    if url is None and env_file is not None and Path(env_file).exists():
        found = []
        path = Path(env_file)
        if path.stat().st_size > 256 * 1024:
            raise ValueError("dotenv file exceeds configuration bound")
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                match = _SETTING.match(line.rstrip("\r\n"))
                if match:
                    value = _VALUE.fullmatch(match[1])
                    if value is None:
                        raise ValueError("invalid UNIVERSE_BASE_URL dotenv assignment")
                    found.append(next(v for v in value.groups() if v is not None))
        if len(found) > 1:
            raise ValueError("duplicate UNIVERSE_BASE_URL dotenv assignment")
        url = found[0] if found else None
    if not url:
        raise ValueError("UNIVERSE_BASE_URL is required for preparation; set it in the dotenv file or exported environment")
    try:
        if type(url) is not str or any(c.isspace() or ord(c) < 32 for c in url):
            raise ValueError()
        parts = urlsplit(url)
        if not parts.hostname or parts.port == 0:
            raise ValueError()
        source = UniverseHTTP(url, timeout=10)
    except (ValueError, TypeError, ProtocolError) as error:
        raise ValueError("UNIVERSE_BASE_URL must be an HTTP(S) URL without credentials, query or fragment") from error
    return source


def main(argv=None):
    parser = argparse.ArgumentParser(description="Prepare one pinned bundle, including Universe outcome masks")
    parser.add_argument("config", type=Path)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    args = parser.parse_args(argv)
    try:
        source = universe_from_environment(env_file=args.env_file)
        with args.config.open("rb") as stream:
            config = decode(stream.read(MAX_BYTES + 1), MAX_BYTES)
        prepare(config, args.directory, universe=source)
    except (ValueError, OSError, ProtocolError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
