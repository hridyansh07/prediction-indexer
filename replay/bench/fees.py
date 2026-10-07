"""Declarative schedules in Fee SDK tagged terms, with pinned local sources."""

import hashlib
import json
from pathlib import Path

from replay.fees.artifacts import MAX_BYTES, build_catalog, load_catalog, parse_canonical, source_from_bytes
from replay.fees.domain import canonical, tree
from replay.fees.schedules import Catalog, Schedule, Polymarket, Rounding, Kalshi, LimitlessClob
from replay.preparation import encoded
from replay.streams.protocol import obj, require
from .common import create_output, path, read_json, sha, write_json


def validate_fee_spec(spec):
    obj(spec, 'version schedules')
    require(type(spec['version']) is int and spec['version'] == 1, 'fee spec version')
    require(type(spec['schedules']) is list and len(spec['schedules']) <= 10000, 'fee schedules array')
    schedules, blobs = [], {}
    for raw in spec['schedules']:
        require(type(raw) is dict and raw.get('type') == 'Schedule', 'Fee SDK Schedule encoding required')
        # Sources use local path, expected hash, URL and retrieval time. The SDK
        # computes and validates the byte length rather than trusting the caller.
        require(type(raw.get('sources')) is list and 0 < len(raw['sources']) <= 32, 'schedule sources required')
        sources = []
        for source in raw['sources']:
            obj(source, 'path sha256 url retrieved_at')
            source_path = path(source['path'], directory=False)
            expected = sha(source['sha256'])
            with source_path.open('rb') as stream:
                data = stream.read(MAX_BYTES + 1)
            require(len(data) <= MAX_BYTES and hashlib.sha256(data).hexdigest() == expected, 'source hash mismatch')
            evidence = source_from_bytes(source['url'], source['retrieved_at'], data)
            sources.append(tree(evidence))
            blobs[expected] = data
        schedule = parse_canonical((json.dumps({**raw, 'sources': sources}, sort_keys=True, separators=(',', ':'), ensure_ascii=True) + '\n').encode())
        require(type(schedule) is Schedule, 'schedule required')
        required_scale = None
        if isinstance(schedule.model, Polymarket) and schedule.model.rounding is Rounding.PM_CEIL5_SCENARIO:
            required_scale = 5
        elif isinstance(schedule.model, (Kalshi, LimitlessClob)):
            required_scale = 6
        require(required_scale is None or schedule.fee_scale == required_scale, 'model rounding/fee_scale mismatch')
        schedules.append(schedule)
    return Catalog.build(schedules), blobs


def build_fees(spec_path, output):
    spec = read_json(spec_path)
    catalog, blobs = validate_fee_spec(spec)
    root = create_output(output)
    directory = build_catalog(root, catalog, blobs)
    verified = load_catalog(directory)
    report = {'version': 1, 'spec_sha256': hashlib.sha256(encoded(spec)).hexdigest(),
              'catalog_identity': verified.identity, 'catalog_directory': directory.name}
    write_json(root / 'bench_fees.json', report)
    return report
