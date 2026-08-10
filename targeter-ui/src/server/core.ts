import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import type { RunSummary } from '../shared.js';

export const RUN_RE =
  /(?:^|\/)date=(\d{4}-\d{2}-\d{2})\/run=(\d{8}T\d{6}\.\d{6}Z)\/run_manifest\.json$/;
export const MAX_STORED_BYTES = 16 * 1024 * 1024;
export const MAX_DECODED_BYTES = 64 * 1024 * 1024;
const sha = (b: Uint8Array) => createHash('sha256').update(b).digest('hex');
const obj = (x: unknown, label: string): Record<string, any> => {
  if (!x || typeof x !== 'object' || Array.isArray(x))
    throw new Error(`${label} must be an object`);
  return x as Record<string, any>;
};
const int = (x: unknown, label: string) => {
  if (!Number.isSafeInteger(x) || (x as number) < 0)
    throw new Error(`${label} is invalid`);
  return x as number;
};
const timestamp = (x: unknown, label: string) => {
  if (
    typeof x !== 'string' ||
    !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(x) ||
    Number.isNaN(Date.parse(x))
  )
    throw new Error(`${label} is invalid`);
  return x;
};
const exactKeys = (
  value: Record<string, any>,
  expected: string[],
  label: string,
) => {
  if (Object.keys(value).sort().join('\0') !== [...expected].sort().join('\0'))
    throw new Error(`${label} fields are invalid`);
};
const supportedVenues = ['kalshi', 'polymarket', 'limitless'];
export function parseManifestKey(
  key: string,
): { runId: string; date: string } | null {
  const m = RUN_RE.exec(key);
  if (!m) return null;
  const d = new Date(
    `${m[1]}T${m[2].slice(9, 11)}:${m[2].slice(11, 13)}:${m[2].slice(13, 15)}.${m[2].slice(16, 22)}Z`,
  );
  if (
    Number.isNaN(d.valueOf()) ||
    m[2].slice(0, 8) !== m[1].replaceAll('-', '')
  )
    return null;
  return { date: m[1], runId: m[2] };
}
export function latestRunKeys(keys: string[], limit = 5) {
  return keys
    .map((k) => ({ key: k, parsed: parseManifestKey(k) }))
    .filter(
      (x): x is { key: string; parsed: { runId: string; date: string } } =>
        !!x.parsed,
    )
    .sort((a, b) => b.parsed.runId.localeCompare(a.parsed.runId))
    .slice(0, limit);
}
export function validateManifest(raw: unknown, expectedRunId: string) {
  const m = obj(raw, 'manifest');
  exactKeys(
    m,
    [
      'targeter_run_manifest_version',
      'run_id',
      'generated_at',
      'input_complete',
      'files',
    ],
    'manifest',
  );
  if (
    m.targeter_run_manifest_version !== 2 ||
    m.run_id !== expectedRunId ||
    typeof m.input_complete !== 'boolean' ||
    !Array.isArray(m.files)
  )
    throw new Error('invalid manifest v2');
  timestamp(m.generated_at, 'manifest generated_at');
  const names = m.files.map(
    (entry: unknown) => obj(entry, 'manifest file').file,
  );
  if (
    names.some((name: unknown) => typeof name !== 'string' || !name) ||
    new Set(names).size !== names.length
  )
    throw new Error('manifest filenames must be non-empty and unique');
  return m;
}
export function reportFile(manifest: Record<string, any>) {
  const files = manifest.files.map((x: unknown) => obj(x, 'manifest file'));
  const z = files.find((x: any) => x.file === 'selection_report.json.zst');
  const p = files.find((x: any) => x.file === 'selection_report.json');
  const f = z ?? p;
  if (!f || (z && p))
    throw new Error('manifest must name exactly one selection report');
  if (z) {
    const metadata = files.find(
      (x: any) => x.file === 'selection_report.meta.json',
    );
    if (!metadata)
      throw new Error('compressed selection report metadata marker is missing');
    exactKeys(
      metadata,
      ['file', 'byte_length', 'sha256', 'content_type', 'content_encoding'],
      'selection report metadata marker',
    );
    identity(
      { sha256: metadata.sha256, byte_length: metadata.byte_length },
      'selection report metadata marker identity',
    );
    if (
      metadata.content_type !== 'application/json' ||
      metadata.content_encoding !== null
    )
      throw new Error(
        'selection report metadata marker content metadata is invalid',
      );
    exactKeys(
      z,
      [
        'file',
        'content_type',
        'content_encoding',
        'decoded',
        'stored',
        'compression',
      ],
      'compressed selection report',
    );
    const compression = obj(z.compression, 'selection report compression');
    exactKeys(
      compression,
      [
        'algorithm',
        'level',
        'frame_checksum',
        'dictionary',
        'frame_count',
        'encoder',
      ],
      'selection report compression',
    );
    if (
      z.content_type !== 'application/json' ||
      z.content_encoding !== 'zstd' ||
      compression.algorithm !== 'zstd' ||
      compression.level !== 3 ||
      compression.frame_checksum !== true ||
      compression.dictionary !== null ||
      compression.frame_count !== 1 ||
      typeof compression.encoder !== 'string' ||
      !compression.encoder
    )
      throw new Error('selection report compression profile is invalid');
  } else {
    exactKeys(
      p,
      ['file', 'byte_length', 'sha256', 'content_type', 'content_encoding'],
      'plain selection report',
    );
    if (p.content_type !== 'application/json' || p.content_encoding !== null)
      throw new Error('plain selection report content metadata is invalid');
  }
  return f;
}
function identity(x: unknown, label: string, logical = false) {
  const i = obj(x, label);
  exactKeys(
    i,
    logical
      ? ['sha256', 'byte_length', 'line_count']
      : ['sha256', 'byte_length'],
    label,
  );
  if (
    !/^[a-f0-9]{64}$/.test(i.sha256) ||
    int(i.byte_length, `${label}.byte_length`) > MAX_DECODED_BYTES ||
    (logical && int(i.line_count, `${label}.line_count`) > i.byte_length)
  )
    throw new Error(`${label} is invalid`);
  return i as { sha256: string; byte_length: number; line_count?: number };
}
function littleEndian(frame: Uint8Array, offset: number, length: number) {
  let value = 0n;
  for (let index = 0; index < length; index++)
    value |= BigInt(frame[offset + index]) << BigInt(index * 8);
  return value;
}
function requireStrictZstdFrame(frame: Uint8Array, maxDecoded: number) {
  if (
    frame.byteLength < 12 ||
    frame[0] !== 0x28 ||
    frame[1] !== 0xb5 ||
    frame[2] !== 0x2f ||
    frame[3] !== 0xfd
  )
    throw new Error('selection report is not one ordinary Zstd frame');
  const descriptor = frame[4];
  const sizeFlag = descriptor >>> 6;
  const singleSegment = (descriptor & 0x20) !== 0;
  if (
    (descriptor & 0x18) !== 0 ||
    (descriptor & 0x03) !== 0 ||
    (descriptor & 0x04) === 0
  )
    throw new Error(
      'selection report Zstd frame flags violate the approved profile',
    );
  let offset = 5;
  if (!singleSegment) {
    const windowDescriptor = frame[offset++];
    const windowBase = 1n << BigInt(10 + (windowDescriptor >>> 3));
    const windowSize =
      windowBase + (windowBase >> 3n) * BigInt(windowDescriptor & 7);
    if (windowSize > BigInt(MAX_DECODED_BYTES))
      throw new Error('selection report Zstd window exceeds the decode limit');
  }
  const contentSizeBytes =
    sizeFlag === 0
      ? singleSegment
        ? 1
        : 0
      : sizeFlag === 1
        ? 2
        : sizeFlag === 2
          ? 4
          : 8;
  if (offset + contentSizeBytes > frame.byteLength)
    throw new Error('selection report Zstd frame header is truncated');
  if (contentSizeBytes) {
    let contentSize = littleEndian(frame, offset, contentSizeBytes);
    if (contentSizeBytes === 2) contentSize += 256n;
    if (contentSize > BigInt(maxDecoded))
      throw new Error(
        'selection report Zstd content size exceeds its logical identity',
      );
  }
  offset += contentSizeBytes;
  if (offset > frame.byteLength - 7)
    throw new Error('selection report Zstd frame header is truncated');
  let last = false;
  while (!last) {
    if (offset + 3 > frame.byteLength - 4)
      throw new Error('selection report Zstd block header is truncated');
    const header =
      frame[offset] | (frame[offset + 1] << 8) | (frame[offset + 2] << 16);
    offset += 3;
    last = (header & 1) !== 0;
    const type = (header >>> 1) & 3;
    if (type === 3)
      throw new Error('selection report Zstd block type is reserved');
    const blockSize = header >>> 3;
    offset += type === 1 ? 1 : blockSize;
    if (offset > frame.byteLength - 4)
      throw new Error('selection report Zstd block is truncated');
  }
  if (offset + 4 !== frame.byteLength)
    throw new Error(
      'selection report must contain exactly one Zstd frame with no trailing data',
    );
}
function decompressBounded(
  frame: Uint8Array,
  maxDecoded: number,
): Promise<Uint8Array> {
  return new Promise((resolve, reject) => {
    const child = spawn('zstd', ['--decompress', '--stdout', '--quiet'], {
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    const chunks: Buffer[] = [];
    let total = 0;
    let stderr = '';
    let settled = false;
    const fail = (error: Error) => {
      if (settled) return;
      settled = true;
      child.kill();
      reject(error);
    };
    child.on('error', (error) =>
      fail(new Error(`cannot run zstd decoder: ${error.message}`)),
    );
    child.stderr.on('data', (chunk: Buffer) => {
      if (stderr.length < 65536) stderr += chunk.toString('utf8');
    });
    child.stdout.on('data', (chunk: Buffer) => {
      total += chunk.length;
      if (total > maxDecoded) {
        fail(new Error('selection report exceeds decoded size limit'));
        return;
      }
      chunks.push(Buffer.from(chunk));
    });
    child.on('close', (code) => {
      if (settled) return;
      settled = true;
      if (code !== 0) {
        reject(
          new Error(
            `zstd decoder rejected frame${stderr.trim() ? `: ${stderr.trim()}` : ''}`,
          ),
        );
        return;
      }
      resolve(Buffer.concat(chunks, total));
    });
    child.stdin.on('error', (error) =>
      fail(new Error(`cannot write to zstd decoder: ${error.message}`)),
    );
    child.stdin.end(Buffer.from(frame));
  });
}
export async function validateReportBytes(
  stored: Uint8Array,
  file: Record<string, any>,
  runId: string,
) {
  if (stored.byteLength > MAX_STORED_BYTES)
    throw new Error('selection report exceeds stored size limit');
  const compressed = file.file.endsWith('.zst');
  const si = compressed
    ? identity(file.stored, 'stored')
    : identity(
        { sha256: file.sha256, byte_length: file.byte_length },
        'report identity',
      );
  if (si.byte_length !== stored.byteLength || sha(stored) !== si.sha256)
    throw new Error('selection report stored identity mismatch');
  const di = compressed ? identity(file.decoded, 'decoded', true) : si;
  let decoded: Uint8Array;
  try {
    if (compressed) requireStrictZstdFrame(stored, di.byte_length);
    decoded = compressed
      ? await decompressBounded(stored, di.byte_length)
      : stored;
  } catch (error) {
    throw new Error(
      `selection report strict Zstd decode failed: ${error instanceof Error ? error.message : 'unknown error'}`,
    );
  }
  const lineCount = decoded.reduce(
    (count, byte) => count + (byte === 0x0a ? 1 : 0),
    0,
  );
  if (
    decoded.byteLength !== di.byte_length ||
    sha(decoded) !== di.sha256 ||
    (compressed && lineCount !== di.line_count)
  )
    throw new Error('selection report decoded identity mismatch');
  let report: unknown;
  try {
    report = JSON.parse(
      new TextDecoder('utf-8', { fatal: true }).decode(decoded),
    );
  } catch {
    throw new Error('selection report is not valid UTF-8 JSON');
  }
  const r = obj(report, 'selection report');
  if (
    r.report_version !== 1 ||
    r.mode !== 'shadow' ||
    r.run_id !== runId ||
    typeof r.input_complete !== 'boolean' ||
    !Array.isArray(r.catalogs) ||
    !Array.isArray(r.candidates)
  )
    throw new Error('invalid selection report v1');
  timestamp(r.generated_at, 'selection report generated_at');
  obj(r.discovery_failures, 'selection report discovery_failures');
  if (
    (r.artifact_format === undefined) !== (r.artifacts === undefined) ||
    (r.artifact_format !== undefined &&
      !['zstd', 'ndjson'].includes(r.artifact_format))
  )
    throw new Error('selection report artifact inventory is invalid');
  const catalogVenues = new Set<string>();
  for (const rawCatalog of r.catalogs) {
    const catalog = obj(rawCatalog, 'catalog summary');
    if (
      !supportedVenues.includes(catalog.venue) ||
      catalogVenues.has(catalog.venue) ||
      typeof catalog.complete !== 'boolean'
    )
      throw new Error('catalog summaries are invalid');
    int(catalog.events, 'catalog events');
    int(catalog.markets, 'catalog markets');
    catalogVenues.add(catalog.venue);
  }
  if (r.artifacts !== undefined) {
    const artifacts = obj(r.artifacts, 'selection report artifacts');
    const suffix = r.artifact_format === 'zstd' ? '.ndjson.zst' : '.ndjson';
    const expected = [
      `rule_templates${suffix}`,
      `rule_drift${suffix}`,
      ...[...catalogVenues].flatMap((venue) => [
        `catalog_${venue}_events${suffix}`,
        `catalog_${venue}_markets${suffix}`,
      ]),
    ];
    if (Object.keys(artifacts).sort().join('\0') !== expected.sort().join('\0'))
      throw new Error(
        'selection report artifact inventory is incomplete or unexpected',
      );
  }
  const selection = obj(r.selection, 'selection');
  if (
    selection.publication_performed !== false ||
    !Array.isArray(selection.bundle_ids) ||
    selection.bundle_count !== selection.bundle_ids.length ||
    new Set(selection.bundle_ids).size !== selection.bundle_ids.length ||
    selection.bundle_ids.some((id: unknown) => typeof id !== 'string' || !id)
  )
    throw new Error('selection report selection is invalid');
  const targets = obj(selection.targets, 'selection targets');
  if (
    Object.keys(targets).sort().join('\0') !==
      [...supportedVenues].sort().join('\0') ||
    supportedVenues.some((venue) => !Array.isArray(targets[venue]))
  )
    throw new Error('selection report targets are invalid');
  obj(selection.allocation_rejections, 'selection allocation_rejections');
  const candidates = new Map<string, Record<string, any>>();
  for (const rawCandidate of r.candidates) {
    const candidate = obj(rawCandidate, 'candidate');
    const participants = candidate.participants;
    const venues = candidate.venues;
    if (
      typeof candidate.bundle_id !== 'string' ||
      !candidate.bundle_id ||
      candidates.has(candidate.bundle_id) ||
      typeof candidate.eligible !== 'boolean' ||
      !Array.isArray(candidate.rejection_reasons) ||
      candidate.rejection_reasons.some(
        (reason: unknown) => typeof reason !== 'string' || !reason,
      ) ||
      candidate.event_status !==
        (candidate.eligible ? 'ELIGIBLE' : 'REJECTED') ||
      !Array.isArray(participants) ||
      participants.length !== 2 ||
      participants.some(
        (value: unknown) => typeof value !== 'string' || !value,
      ) ||
      !Array.isArray(venues) ||
      new Set(venues).size !== venues.length ||
      venues.some((venue: unknown) => !supportedVenues.includes(String(venue)))
    )
      throw new Error('selection report candidate is invalid');
    timestamp(candidate.activation_at, 'candidate activation_at');
    timestamp(candidate.capture_start_at, 'candidate capture_start_at');
    obj(candidate.admission, 'candidate admission');
    obj(candidate.market_exclusions, 'candidate market_exclusions');
    if (!Array.isArray(candidate.eligible_market_ids))
      throw new Error('candidate eligible_market_ids is invalid');
    candidates.set(candidate.bundle_id, candidate);
  }
  if (selection.bundle_ids.some((id: string) => !candidates.get(id)?.eligible))
    throw new Error('selected bundle is absent or ineligible');
  return r;
}
export function summarizeReport(report: Record<string, any>): RunSummary {
  const cs = Array.isArray(report.candidates) ? report.candidates : [];
  const sel = new Set(
    Array.isArray(report.selection?.bundle_ids)
      ? report.selection.bundle_ids
      : [],
  );
  const allocations = obj(
    report.selection?.allocation_rejections ?? {},
    'allocation rejections',
  );
  const reasons: Record<string, number> = {};
  for (const c of cs)
    if (!sel.has(c?.bundle_id)) {
      const admission = Array.isArray(c?.rejection_reasons)
        ? c.rejection_reasons
        : [];
      const rejected = admission.length
        ? admission
        : [allocations[c?.bundle_id] ?? 'eligible_not_selected'];
      for (const reason of rejected)
        reasons[String(reason)] = (reasons[String(reason)] ?? 0) + 1;
    }
  const cats =
    report.catalogs && typeof report.catalogs === 'object'
      ? Object.values(report.catalogs)
      : [];
  const targets =
    report.selection?.targets && typeof report.selection.targets === 'object'
      ? Object.values(report.selection.targets).reduce(
          (n: number, v: any) => n + (Array.isArray(v) ? v.length : 0),
          0,
        )
      : 0;
  return {
    candidates: cs.length,
    selected: sel.size,
    rejected: cs.length - sel.size,
    targets: targets as number,
    catalogsComplete: cats.filter((x: any) => x?.complete === true).length,
    catalogsTotal: cats.length,
    rejectionReasons: reasons,
  };
}
