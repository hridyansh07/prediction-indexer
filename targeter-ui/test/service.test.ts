import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import type { ReadOnlyObjectStore } from '@prediction-indexer/read-only-object-store';
import { SnapshotService } from '../src/server/service.js';

const hash = (b: Uint8Array) => createHash('sha256').update(b).digest('hex');
const baseReport = (run_id: string) => ({
  report_version: 1,
  mode: 'shadow',
  run_id,
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
class FakeStore implements ReadOnlyObjectStore {
  opened: Array<{ key: string; max: number }> = [];
  fail = false;
  constructor(public objects: Map<string, Buffer>) {}
  async *listKeys(prefix: string) {
    for (const key of this.objects.keys())
      if (key.startsWith(prefix)) yield key;
  }
  async open(key: string, options: { maxBytes: number }) {
    this.opened.push({ key, max: options.maxBytes });
    if (this.fail) throw new Error('store failed');
    const bytes = this.objects.get(key);
    if (!bytes) throw new Error(`unexpected fetch ${key}`);
    return {
      body: (async function* () {
        yield bytes;
      })(),
      close() {},
    };
  }
}

test('stages only latest five manifests/reports and preserves stale last success', async () => {
  const objects = new Map<string, Buffer>();
  for (let i = 0; i < 6; i++) {
    const run = `20260810T1${i}0000.000001Z`;
    const key = `prefix/date=2026-08-10/run=${run}`;
    const bytes = Buffer.from(JSON.stringify(baseReport(run)));
    objects.set(
      `${key}/run_manifest.json`,
      Buffer.from(
        JSON.stringify({
          targeter_run_manifest_version: 2,
          run_id: run,
          generated_at: '2026-08-10T12:00:00Z',
          input_complete: true,
          files: [
            {
              file: 'selection_report.json',
              sha256: hash(bytes),
              byte_length: bytes.length,
              content_type: 'application/json',
              content_encoding: null,
            },
          ],
        }),
      ),
    );
    objects.set(`${key}/selection_report.json`, bytes);
    objects.set(`${key}/catalog_kalshi_events.ndjson`, Buffer.from('never'));
  }
  objects.set('prefix/not-a-commit.json', Buffer.from('never'));
  const store = new FakeStore(objects);
  const service = new SnapshotService(store, null, {}, 60, 600, 'prefix/');
  const first = await service.refresh();
  assert.equal(first.runs.length, 5);
  assert.equal(first.runs[0].runId, '20260810T150000.000001Z');
  assert.equal(store.opened.length, 10);
  assert.ok(
    store.opened
      .filter((x) => x.key.endsWith('run_manifest.json'))
      .every((x) => x.max === 1024 * 1024),
  );
  assert.ok(
    store.opened
      .filter((x) => x.key.endsWith('selection_report.json'))
      .every((x) => x.max === 16 * 1024 * 1024),
  );
  assert.ok(store.opened.every((x) => !x.key.includes('catalog')));
  objects.set(
    'prefix/date=2026-08-10/run=20260810T150000.000001Z/selection_report.json',
    Buffer.from('corrupt'),
  );
  const stale = await service.refresh();
  assert.equal(stale.stale, true);
  assert.equal(stale.runs[0].runId, first.runs[0].runId);
  assert.match(stale.lastRefreshError!, /staged object identity mismatch/);
});

test('stages compressed report and delegates decoding without catalog downloads', async () => {
  const run = '20260810T120000.000001Z';
  const root = `prefix/date=2026-08-10/run=${run}`;
  const decoded = Buffer.from(JSON.stringify(baseReport(run)));
  const stored = Buffer.from('compressed evidence');
  const marker = {
    file: 'selection_report.meta.json',
    sha256: '0'.repeat(64),
    byte_length: 1,
    content_type: 'application/json',
    content_encoding: null,
  };
  const compressed = {
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
  };
  const manifest = {
    targeter_run_manifest_version: 2,
    run_id: run,
    generated_at: '2026-08-10T12:00:00Z',
    input_complete: true,
    files: [compressed, marker],
  };
  const store = new FakeStore(
    new Map([
      [`${root}/run_manifest.json`, Buffer.from(JSON.stringify(manifest))],
      [`${root}/selection_report.json.zst`, stored],
      [`${root}/catalog_kalshi_events.ndjson`, Buffer.from('never')],
    ]),
  );
  const dir = await mkdtemp(join(tmpdir(), 'ui-service-'));
  try {
    const decodedPath = join(dir, 'decoded');
    await writeFile(decodedPath, decoded);
    let calls = 0;
    const decoder = {
      async withDecodedFile<T>(
        _storedPath: string,
        expectation: any,
        use: (path: string) => T | Promise<T>,
      ) {
        calls++;
        assert.equal(expectation.logical.sha256, hash(decoded));
        return use(decodedPath);
      },
    };
    const snapshot = await new SnapshotService(
      store,
      decoder,
      {},
      60,
      600,
      'prefix',
    ).refresh();
    assert.equal(snapshot.runs[0].runId, run);
    assert.equal(calls, 1);
    assert.ok(store.opened.every((x) => !x.key.includes('catalog')));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
