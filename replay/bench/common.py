"""Small bounded JSON and exclusive, durable evidence helpers."""

import hashlib
import math
import os
from pathlib import Path
import re
import traceback

from replay.preparation import encoded
from replay.streams.protocol import decode, obj, require
from replay.strategy_sdk import plain
from replay.supervisor import fsync_directory

MAX_BYTES = 16 * 1024 * 1024
NAME = re.compile(r'[A-Za-z0-9_-][A-Za-z0-9_.-]{0,79}')
IMPORT = re.compile(r'[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*')
RESERVED = {'.', '..', 'publisher', 'ready', 'publisher.json', 'result.json', 'interrupted.json'}


def read_json(path):
    with Path(path).open('rb') as stream:
        return decode(stream.read(MAX_BYTES + 1), MAX_BYTES)


def write_json(path, value):
    """Never replace a bench artifact, including after a failed attempt."""
    path = Path(path)
    data = encoded(plain(value)) + b'\n'
    require(len(data) <= MAX_BYTES, 'bench artifact byte limit')
    path.parent.mkdir(parents=True, exist_ok=True)
    # Final files are not commit markers. Exclusive creation keeps every failure
    # visible; fsync establishes durability before returning.
    with path.open('xb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    fsync_directory(path.parent)


def create_output(path):
    root = Path(path)
    require(not root.exists() and not root.is_symlink(), 'output directory already exists')
    root.parent.mkdir(parents=True, exist_ok=True)
    root.mkdir()  # exclusive, including a racing creator
    fsync_directory(root.parent)
    return root


def identity(value):
    return hashlib.sha256(encoded(plain(value))).hexdigest()


def sha(value):
    require(type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None, 'invalid SHA-256')
    return value


def name(value):
    require(type(value) is str and NAME.fullmatch(value) is not None and value not in RESERVED, 'invalid or reserved name')
    return value


def import_name(value):
    require(type(value) is str and IMPORT.fullmatch(value) is not None, 'invalid import name')
    return value


def positive(value):
    require(type(value) in (int, float) and math.isfinite(value) and value > 0, 'positive finite number required')
    return value


def path(value, *, directory=None, exists=True):
    require(type(value) is str and bool(value) and '\x00' not in value and Path(value).is_absolute(), 'absolute path required')
    result = Path(value)
    require(not any(p.startswith('.env') or p.endswith(('.key', '.pem')) or p in {'secrets', 'credentials'} for p in result.parts), 'secret paths are forbidden')
    if exists:
        require(result.exists(), 'input path does not exist')
        if directory is not None:
            require(result.is_dir() if directory else result.is_file(), 'input path has wrong type')
    return result


def error_document(error):
    return {'type': type(error).__name__, 'message': str(error)[:2048],
            'trace_tail': ''.join(traceback.format_exception(error))[-8192:]}


def closed_check(value):
    obj(value, 'passed details')
    require(type(value['passed']) is bool and type(value['details']) is dict, 'invalid check result')
    # Serialization is part of the check contract, including NaN rejection.
    data = encoded(value)
    decode(data, MAX_BYTES)
    return value
