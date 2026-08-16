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
import {
  isLegitimateTerminalRetirement,
  selectedBundleViews,
} from '../src/client/view-model.js';
import {
  BASE_RUN_ID,
  degradedReportV2,
  retainedReportV2,
  RUN_ID,
  selectedReportV1,
  terminalRetirementReportV2,
} from './continuity-fixtures.js';

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

test('accepts complete old and target-record artifact inventories', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'ui-target-records-'));
  let sequence = 0;
  const validate = async (
    artifactFormat: 'ndjson' | 'zstd',
    artifacts: Record<string, unknown>,
  ) => {
    const value: any = report();
    value.catalogs = [
      { venue: 'kalshi', complete: true, events: 0, markets: 0 },
    ];
    value.artifact_format = artifactFormat;
    value.artifacts = artifacts;
    const bytes = Buffer.from(JSON.stringify(value));
    const path = join(directory, `report-${sequence++}`);
    await writeFile(path, bytes);
    return validateReportPath(path, plainFile(bytes), value.run_id, null);
  };

  try {
    for (const [artifactFormat, suffix] of [
      ['ndjson', '.ndjson'],
      ['zstd', '.ndjson.zst'],
    ] as const) {
      const baseArtifacts = {
        [`rule_templates${suffix}`]: {},
        [`rule_drift${suffix}`]: {},
        [`catalog_kalshi_events${suffix}`]: {},
        [`catalog_kalshi_markets${suffix}`]: {},
      };
      const targetRecords = Object.fromEntries(
        ['kalshi', 'polymarket', 'limitless'].map((venue) => [
          `target_records_${venue}${suffix}`,
          {},
        ]),
      );
      assert.equal(
        (await validate(artifactFormat, baseArtifacts)).run_id,
        report().run_id,
      );
      assert.equal(
        (
          await validate(artifactFormat, {
            ...baseArtifacts,
            ...targetRecords,
          })
        ).run_id,
        report().run_id,
      );
      await assert.rejects(
        () =>
          validate(artifactFormat, {
            ...baseArtifacts,
            [`target_records_kalshi${suffix}`]: {},
          }),
        /artifact inventory is incomplete or unexpected/,
      );
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('validates and derives report v2 retained continuity without a current candidate', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'ui-continuity-v2-'));
  let sequence = 0;
  const validate = async (value: any) => {
    const bytes = Buffer.from(JSON.stringify(value));
    const path = join(directory, `report-${sequence++}`);
    await writeFile(path, bytes);
    return validateReportPath(path, plainFile(bytes), value.run_id, null);
  };
  try {
    const retained = await validate(retainedReportV2());
    assert.equal(retained.report_version, 2);
    assert.equal(retained.candidates.length, 0);
    assert.equal(summarizeReport(retained).rejected, 0);
    const [bundle] = selectedBundleViews(retained);
    assert.equal(bundle.bundleId, 'bundle-retained-without-current-candidate');
    assert.equal(bundle.candidate, null);
    assert.equal(bundle.retained, true);
    assert.equal(bundle.targets.length, 2);
    assert.equal(bundle.continuityBaseRunId, BASE_RUN_ID);
    assert.equal(bundle.continuity?.targets[1].terminal_probe.state, 'unknown');

    const degraded = await validate(degradedReportV2());
    assert.equal(degraded.continuity_degraded_base_run_id, BASE_RUN_ID);

    const badDisposition: any = structuredClone(retainedReportV2());
    badDisposition.continuity.dispositions[
      'bundle-retained-without-current-candidate'
    ] = 'silently_dropped';
    await assert.rejects(
      () => validate(badDisposition),
      /disposition is invalid/,
    );

    const badProbe: any = structuredClone(retainedReportV2());
    badProbe.continuity.bundles[0].targets[1].terminal_probe.state = 'missing';
    await assert.rejects(() => validate(badProbe), /terminal_probe state/);

    const badActivation: any = structuredClone(retainedReportV2());
    badActivation.continuity.bundles[0].targets[0].activation_at =
      '2026-08-16T11:31:00Z';
    await assert.rejects(
      () => validate(badActivation),
      /activation disagrees with its bundle/,
    );

    const badScore: any = structuredClone(retainedReportV2());
    badScore.selection.targets.kalshi[0].continuity_score = 41;
    await assert.rejects(
      () => validate(badScore),
      /continuity_score disagrees with its provenance/,
    );

    const partialContinuity: any = structuredClone(retainedReportV2());
    delete partialContinuity.continuity.retained_bundle_ids;
    await assert.rejects(
      () => validate(partialContinuity),
      /continuity fields are invalid/,
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('preserves report v1 selected-target compatibility', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'ui-report-v1-'));
  try {
    const value = selectedReportV1();
    const bytes = Buffer.from(JSON.stringify(value));
    const path = join(directory, 'selection-report');
    await writeFile(path, bytes);
    const validated = await validateReportPath(
      path,
      plainFile(bytes),
      RUN_ID,
      null,
    );
    const [bundle] = selectedBundleViews(validated);
    assert.equal(validated.report_version, 1);
    assert.equal(bundle.candidate?.participants.join(' vs '), 'Alpha vs Beta');
    assert.equal(bundle.continuity, null);
    assert.equal(bundle.targets[0].continuity_score, undefined);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('recognizes all-terminal and clamp retirement as a legitimate empty v2 decision', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'ui-terminal-empty-'));
  try {
    const value = terminalRetirementReportV2();
    const bytes = Buffer.from(JSON.stringify(value));
    const path = join(directory, 'selection-report');
    await writeFile(path, bytes);
    const validated = await validateReportPath(
      path,
      plainFile(bytes),
      RUN_ID,
      null,
    );
    assert.equal(isLegitimateTerminalRetirement(validated), true);
    assert.deepEqual(selectedBundleViews(validated), []);
    assert.equal(
      validated.continuity.bundles[1].targets[0].terminal_probe.state,
      'unknown',
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
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
