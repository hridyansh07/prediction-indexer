import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import {
  latestRunKeys,
  parseManifestKey,
  reportFile,
  summarizeReport,
  validateManifest,
  validateReportBytes,
} from '../src/server/core.js';
const id = (b: Uint8Array) => ({
  sha256: createHash('sha256').update(b).digest('hex'),
  byte_length: b.length,
});
const candidate = (
  bundle_id: string,
  eligible: boolean,
  rejection_reasons: string[],
) => ({
  bundle_id,
  eligible,
  event_status: eligible ? 'ELIGIBLE' : 'REJECTED',
  rejection_reasons,
  participants: ['Alpha', 'Beta'],
  venues: ['kalshi', 'polymarket'],
  activation_at: '2026-08-10T13:00:00Z',
  capture_start_at: '2026-08-10T12:00:00Z',
  admission: {},
  market_exclusions: {},
  eligible_market_ids: [],
});
const artifacts = Object.fromEntries(
  [
    'rule_templates.ndjson',
    'rule_drift.ndjson',
    ...['kalshi', 'polymarket', 'limitless'].flatMap((venue) => [
      `catalog_${venue}_events.ndjson`,
      `catalog_${venue}_markets.ndjson`,
    ]),
  ].map((name) => [name, {}]),
);
const report = {
  report_version: 1,
  mode: 'shadow',
  run_id: '20260810T120000.000001Z',
  generated_at: '2026-08-10T12:00:00Z',
  input_complete: true,
  discovery_failures: {},
  artifact_format: 'ndjson',
  artifacts,
  candidates: [
    candidate('a', true, []),
    candidate('b', false, ['low_volume']),
    candidate('c', true, []),
  ],
  catalogs: ['kalshi', 'polymarket', 'limitless'].map((venue, index) => ({
    venue,
    complete: index !== 1,
    events: 1,
    markets: 2,
  })),
  selection: {
    bundle_ids: ['a'],
    bundle_count: 1,
    targets: { kalshi: [1], polymarket: [2, 3], limitless: [] },
    allocation_rejections: { c: 'target_budget_exceeded' },
    publication_performed: false,
  },
};
test('parses only canonical committed manifest keys and sorts by run id', () => {
  assert.equal(
    parseManifestKey(
      'x/date=2026-08-10/run=20260810T120000.000001Z/run_manifest.json',
    )?.runId,
    report.run_id,
  );
  assert.equal(
    parseManifestKey(
      'x/date=2026-08-09/run=20260810T120000.000001Z/run_manifest.json',
    ),
    null,
  );
  const got = latestRunKeys([
    'p/date=2026-08-10/run=20260810T120000.000001Z/run_manifest.json',
    'p/date=2026-08-10/run=20260810T130000.000001Z/run_manifest.json',
  ]);
  assert.match(got[0].key, /130000/);
});
test('validates manifest v2', () => {
  assert.equal(
    validateManifest(
      {
        targeter_run_manifest_version: 2,
        run_id: report.run_id,
        generated_at: report.generated_at,
        input_complete: true,
        files: [],
      },
      report.run_id,
    ).run_id,
    report.run_id,
  );
  assert.throws(() => validateManifest({}, report.run_id));
});
test('requires the report commit metadata and approved compression profile', () => {
  const compression = {
    algorithm: 'zstd',
    level: 3,
    frame_checksum: true,
    dictionary: null,
    frame_count: 1,
    encoder: 'test',
  };
  const compressed = {
    file: 'selection_report.json.zst',
    content_type: 'application/json',
    content_encoding: 'zstd',
    decoded: {},
    stored: {},
    compression,
  };
  assert.throws(() => reportFile({ files: [compressed] }), /metadata marker/);
  const marker = {
    file: 'selection_report.meta.json',
    sha256: '0'.repeat(64),
    byte_length: 10,
    content_type: 'application/json',
    content_encoding: null,
  };
  assert.equal(
    reportFile({ files: [compressed, marker] }).file,
    'selection_report.json.zst',
  );
});
test('verifies plain report identity before parsing', async () => {
  const bytes = Buffer.from(JSON.stringify(report));
  const file = {
    file: 'selection_report.json',
    ...id(bytes),
    content_type: 'application/json',
    content_encoding: null,
  };
  assert.equal(
    (await validateReportBytes(bytes, file, report.run_id)).run_id,
    report.run_id,
  );
  await assert.rejects(
    () =>
      validateReportBytes(
        bytes,
        { ...file, sha256: '0'.repeat(64) },
        report.run_id,
      ),
    /identity/,
  );
});
test('strict Zstd decode rejects missing/bad checksums, truncation, trailing frames, and LF drift', async () => {
  const zstdReport = {
    ...report,
    artifact_format: 'zstd',
    artifacts: Object.fromEntries(
      Object.keys(artifacts).map((name) => [`${name}.zst`, {}]),
    ),
  };
  const decoded = Buffer.from(`${JSON.stringify(zstdReport)}\n`);
  const encode = (checksum: boolean) => {
    const result = spawnSync(
      'zstd',
      [
        '--compress',
        '-3',
        checksum ? '--check' : '--no-check',
        '--stdout',
        '--quiet',
      ],
      { input: decoded },
    );
    assert.equal(result.status, 0, result.stderr.toString());
    return result.stdout;
  };
  const stored = encode(true);
  const file = {
    file: 'selection_report.json.zst',
    stored: id(stored),
    decoded: { ...id(decoded), line_count: 1 },
  };
  assert.equal(
    (await validateReportBytes(stored, file, report.run_id)).run_id,
    report.run_id,
  );
  for (const invalid of [
    Buffer.concat([stored, stored]),
    stored.subarray(0, stored.length - 1),
    Buffer.from(stored),
  ]) {
    if (invalid.length === stored.length) invalid[invalid.length - 1] ^= 1;
    await assert.rejects(
      () =>
        validateReportBytes(
          invalid,
          { ...file, stored: id(invalid) },
          report.run_id,
        ),
      /strict Zstd/,
    );
  }
  const unchecked = encode(false);
  await assert.rejects(
    () =>
      validateReportBytes(
        unchecked,
        { ...file, stored: id(unchecked) },
        report.run_id,
      ),
    /strict Zstd/,
  );
  await assert.rejects(
    () =>
      validateReportBytes(
        stored,
        { ...file, decoded: { ...file.decoded, line_count: 0 } },
        report.run_id,
      ),
    /decoded identity/,
  );
});
test('summarizes admission and allocation rejection evidence', () =>
  assert.deepEqual(summarizeReport(report), {
    candidates: 3,
    selected: 1,
    rejected: 2,
    targets: 3,
    catalogsComplete: 2,
    catalogsTotal: 3,
    rejectionReasons: { low_volume: 1, target_budget_exceeded: 1 },
  }));
