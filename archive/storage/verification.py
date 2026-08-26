"""Receipt-owned object verification shared by every archive consumer."""

from __future__ import annotations

from typing import Iterable

from archive.storage.base import (
    ObjectExpectation,
    ObjectMetadata,
    ObjectStore,
    VerificationFailure,
)

__all__ = ["verify_metadata", "verify_objects"]


def verify_metadata(
    metadata: ObjectMetadata | None,
    expected: ObjectExpectation,
) -> ObjectMetadata:
    """Compare provider-authenticated metadata with one receipt expectation."""
    if metadata is None:
        raise VerificationFailure(f"archived object is absent: {expected.key}")
    if metadata.key != expected.key or not metadata.matches(expected.stored):
        raise VerificationFailure(
            f"archived object identity disagrees with its receipt: {expected.key}"
        )
    if not metadata.provider_checksum or not metadata.provider_checksum_algorithm:
        raise VerificationFailure(
            f"archived object lacks provider checksum evidence: {expected.key}"
        )
    if (expected.provider_checksum is None) != (
        expected.provider_checksum_algorithm is None
    ):
        raise VerificationFailure(
            f"receipt has incomplete provider checksum evidence: {expected.key}"
        )
    if expected.provider_checksum is not None and (
        metadata.provider_checksum != expected.provider_checksum
        or metadata.provider_checksum_algorithm
        != expected.provider_checksum_algorithm
    ):
        raise VerificationFailure(
            f"archived object provider checksum disagrees with its receipt: {expected.key}"
        )
    if metadata.content_type != expected.content_type:
        raise VerificationFailure(
            f"archived object content type disagrees with its receipt: {expected.key}"
        )
    if metadata.content_encoding != expected.content_encoding:
        raise VerificationFailure(
            f"archived object content encoding disagrees with its receipt: {expected.key}"
        )
    return metadata


def verify_objects(
    store: ObjectStore, expectations: Iterable[ObjectExpectation]
) -> tuple[ObjectMetadata, ...]:
    """Verify receipt objects in caller-defined deterministic order."""
    return tuple(store.verify(expected) for expected in expectations)
