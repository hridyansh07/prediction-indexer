import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  latestRunKeys,
  reportFile,
  summarizeReport,
  validateManifest,
  validateReportPath,
} from '../src/server/core.js';

const hash = (bytes: Uint8Array) =>
  createHash('sha256').update(bytes).digest('hex');
const report = (runId = '20260810T120000.000001Z') => ({
  report_version: 1,
  mode: 'shadow',
  run_id: runId,
  generated_at: '2026-08-10T12:00:00Z',
  input_complete: true,
  discovery_failures: {},
  catalogs: [],
  candidates: [],
  selection: {
    bundle_ids: [],
    bundle_count: 0,
    targets: { kalshi: [], polymarket: [], limitless: [] },
    allocation_rejections: {},
    publication_performed: false,
  },
});
const plainFile = (bytes: Buffer) =>
  reportFile({
    files: [
      {
        file: 'selection_report.json',
        sha256: hash(bytes),
        byte_length: bytes.length,
        content_type: 'application/json',
        content_encoding: null,
      },
    ],
  });

test('filters canonical keys and applies latest-five run-id ordering', () => {
  const keys = Array.from(
    { length: 6 },
    (_, i) =>
      `p/date=2026-08-10/run=20260810T1${i}0000.000001Z/run_manifest.json`,
  );
  keys.push(
    'p/catalog_kalshi_events.ndjson',
    'p/date=bad/run=x/run_manifest.json',
  );
  assert.deepEqual(
    latestRunKeys(keys).map((x) => x.parsed.runId.slice(9, 11)),
    ['15', '14', '13', '12', '11'],
  );
});

test('keeps manifest schemas closed and report compression profiles exact', () => {
  const runId = report().run_id;
  const manifest = {
    targeter_run_manifest_version: 2,
    run_id: runId,
    generated_at: report().generated_at,
    input_complete: true,
    files: [],
  };
  assert.equal(validateManifest(manifest, runId).run_id, runId);
  assert.throws(
    () => validateManifest({ ...manifest, unexpected: true }, runId),
    /fields/,
  );
  assert.throws(
    () =>
      reportFile({
        files: [
          {
            file: 'selection_report.json.zst',
            content_type: 'application/json',
            content_encoding: 'zstd',
            stored: { sha256: '0'.repeat(64), byte_length: 1 },
            decoded: {
              sha256: '0'.repeat(64),
              byte_length: 1,
              line_count: 0,
            },
            compression: {
              algorithm: 'zstd',
              level: 4,
              frame_checksum: true,
              dictionary: null,
              frame_count: 1,
              encoder: 'test',
            },
          },
          {
            file: 'selection_report.meta.json',
            sha256: '0'.repeat(64),
            byte_length: 1,
            content_type: 'application/json',
            content_encoding: null,
          },
        ],
      }),
    /compression profile/,
  );
});

test('summarizes admission and allocation rejection evidence', () => {
  const value: any = report();
  value.candidates = [
    { bundle_id: 'selected' },
    { bundle_id: 'admission', rejection_reasons: ['low_volume'] },
    { bundle_id: 'allocation', rejection_reasons: [] },
  ];
  value.selection.bundle_ids = ['selected'];
  value.selection.bundle_count = 1;
  value.selection.allocation_rejections = {
    allocation: 'target_budget_exceeded',
  };
  assert.deepEqual(summarizeReport(value).rejectionReasons, {
    low_volume: 1,
    target_budget_exceeded: 1,
  });
});

test('parses plain staged paths and compressed paths only after decoder success', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'ui-core-'));
  try {
    const decoded = Buffer.from(JSON.stringify(report()));
    const plainPath = join(dir, 'plain');
    await writeFile(plainPath, decoded);
    assert.equal(
      (
        await validateReportPath(
          plainPath,
          plainFile(decoded),
          report().run_id,
          null,
        )
      ).run_id,
      report().run_id,
    );

    const stored = Buffer.from('not decoded by UI');
    const storedPath = join(dir, 'stored');
    const decodedPath = join(dir, 'decoded');
    await writeFile(storedPath, stored);
    await writeFile(decodedPath, decoded);
    const compressed = reportFile({
      files: [
        {
          file: 'selection_report.json.zst',
          content_type: 'application/json',
          content_encoding: 'zstd',
          stored: { sha256: hash(stored), byte_length: stored.length },
          decoded: {
            sha256: hash(decoded),
            byte_length: decoded.length,
            line_count: 0,
          },
          compression: {
            algorithm: 'zstd',
            level: 3,
            frame_checksum: true,
            dictionary: null,
            frame_count: 1,
            encoder: 'test',
          },
        },
        {
          file: 'selection_report.meta.json',
          sha256: '0'.repeat(64),
          byte_length: 1,
          content_type: 'application/json',
          content_encoding: null,
        },
      ],
    });
    let decoderSucceeded = false;
    const decoder = {
      async withDecodedFile<T>(
        _path: string,
        expectation: any,
        use: (path: string) => Promise<T>,
      ) {
        assert.equal(expectation.stored.sha256, hash(stored));
        decoderSucceeded = true;
        return use(decodedPath);
      },
    };
    assert.equal(
      (
        await validateReportPath(
          storedPath,
          compressed,
          report().run_id,
          decoder,
        )
      ).run_id,
      report().run_id,
    );
    assert.equal(decoderSucceeded, true);
    const rejecting = {
      async withDecodedFile() {
        throw new Error('decode rejected');
      },
    };
    await assert.rejects(
      () =>
        validateReportPath(storedPath, compressed, report().run_id, rejecting),
      /strict Zstd.*decode rejected/,
    );
    assert.equal(await readFile(storedPath, 'utf8'), 'not decoded by UI');
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
