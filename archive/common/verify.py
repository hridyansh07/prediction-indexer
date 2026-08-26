"""Re-prove an archive against the store, rather than against its receipt.

A receipt is a local claim that verification once succeeded. Everything here
asks the object store the question again — at retry time, at manifest build
time, and immediately before a deletion — because the interesting failures all
happen *after* the receipt was written: an object expired by a lifecycle rule, a
key overwritten by another deployment, a bucket restored from an older copy.

Nothing decodes without an expected identity, and nothing decodes into memory.
§3.5 of the Zstd specification fixes the order: stage and verify the stored
identity first, decode second, with the seal's byte length as a hard ceiling, so
a partially verified record is never exposed.
"""

from __future__ import annotations

import os
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from archive.common.durable import fsync_directory
from archive.storage.base import (
    JSON_CONTENT_TYPE,
    NDJSON_CONTENT_TYPE,
    ZSTD_CONTENT_ENCODING,
    ObjectExpectation,
    ObjectMetadata,
    ObjectStore,
    VerificationFailure,
)
from archive.storage.verification import verify_objects
from archive.common.receipts import ArchiveReceipt
from encoder import LogicalIdentity, decode_stream

__all__ = ["ArchiveVerification", "VerificationError", "decode_archived_segment", "verify_archive"]


class VerificationError(RuntimeError):
    """The archive does not currently match the receipt that describes it."""


@dataclass(frozen=True)
class ArchiveVerification:
    data: ObjectMetadata
    seal: ObjectMetadata


def verify_archive(store: ObjectStore, receipt: ArchiveReceipt) -> ArchiveVerification:
    """Heads both objects and checks every identity the receipt commits to.

    Does **not** need the local source: after a reaping, this is all that is
    left, and a manifest rebuild has to work from the receipt and the store
    alone (§6.4).
    """
    if receipt.location != store.store_id:
        raise VerificationError(
            f"receipt {receipt.path.name} names location {receipt.location!r}; this store is "
            f"{store.store_id!r}. A receipt verifies only against the location it names, even "
            "when the same keys and bytes exist elsewhere."
        )
    if (
        receipt.is_production
        and receipt.provider is not None
        and receipt.provider != store.provider
    ):
        raise VerificationError(
            f"receipt {receipt.path.name} names provider {receipt.provider!r}; this store is "
            f"{store.provider!r}"
        )
    if receipt.is_production:
        try:
            data, seal = verify_objects(
                store,
                (
                    ObjectExpectation(
                        receipt.data_key,
                        receipt.data_stored,
                        receipt.provider_checksum,
                        receipt.provider_checksum_algorithm,
                        NDJSON_CONTENT_TYPE,
                        ZSTD_CONTENT_ENCODING,
                    ),
                    ObjectExpectation(
                        receipt.seal_key,
                        receipt.seal_stored,
                        receipt.seal_provider_checksum,
                        receipt.seal_provider_checksum_algorithm,
                        JSON_CONTENT_TYPE,
                        None,
                    ),
                ),
            )
        except VerificationFailure as error:
            raise VerificationError(str(error)) from error
        return ArchiveVerification(data=data, seal=seal)

    data = _head(store, receipt.data_key)
    if not data.matches(receipt.data_stored):
        raise VerificationError(
            f"archive object {receipt.data_key} is {data.byte_length} bytes with sha256 "
            f"{data.sha256}; the receipt records {receipt.data_stored.byte_length} bytes "
            f"with sha256 {receipt.data_stored.sha256}"
        )
    seal = _head(store, receipt.seal_key)
    if not seal.matches(receipt.seal_stored):
        raise VerificationError(
            f"archived seal {receipt.seal_key} does not match the receipt's seal identity"
        )
    if data.content_encoding not in (None, "zstd"):
        raise VerificationError(
            f"archive object {receipt.data_key} declares content encoding "
            f"{data.content_encoding!r}"
        )
    return ArchiveVerification(data=data, seal=seal)


def decode_archived_segment(
    store: ObjectStore,
    receipt: ArchiveReceipt,
    destination: Path | BinaryIO,
    *,
    max_decoded_bytes: int | None = None,
) -> LogicalIdentity:
    """Decodes an archived segment back to exact NDJSON, or fails closed.

    The ceiling defaults to the byte length the seal committed. A limit breach
    aborts before byte `byte_length + 1` reaches the staging file, so an object
    that decodes to more than the segment it claims to be cannot be written out
    and inspected afterwards.

    **The destination filename appears only after every check has passed.**
    Decoding writes to a unique `.open` file and renames it, with the same
    discipline a seal or a receipt uses — otherwise a decode that failed its
    logical identity would leave a complete-looking file under the name a caller
    asked for, which is the exact shape of "partially verified records exposed
    as trusted evidence" that §3.5 forbids. A caller supplying its own writer
    owns that guarantee itself; a caller supplying a path gets it here.
    """
    limit = receipt.source.byte_length if max_decoded_bytes is None else max_decoded_bytes
    verify_archive(store, receipt)

    def decode(sink: BinaryIO) -> LogicalIdentity:
        with store.open(receipt.data_key, max_bytes=receipt.data_stored.byte_length) as reader:
            result = decode_stream(
                reader,
                sink,
                expected_logical=receipt.source,
                expected_stored=receipt.data_stored,
                max_decoded_bytes=limit,
            )
        return result.logical

    if not isinstance(destination, (str, Path)):
        return decode(destination)

    path = Path(destination)
    staged = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}.open")
    try:
        with staged.open("wb") as sink:
            logical = decode(sink)
            sink.flush()
            os.fsync(sink.fileno())
        os.replace(staged, path)
        fsync_directory(path.parent)
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    return logical


def _head(store: ObjectStore, key: str) -> ObjectMetadata:
    metadata = store.head(key)
    if metadata is None:
        raise VerificationError(f"archive object {key} is absent from {store.store_id}")
    return metadata
