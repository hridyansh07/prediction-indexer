import { createHash } from 'node:crypto';
import { readFile, stat } from 'node:fs/promises';
import type { DecodeExpectation } from '@prediction-indexer/rust-v1-decoder';
import {
  continuityDispositions,
  type ContinuityBundle,
  type RunSummary,
  type SelectionReport,
} from '../shared.js';

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
const text = (x: unknown, label: string) => {
  if (typeof x !== 'string' || !x) throw new Error(`${label} is invalid`);
  return x;
};
const finite = (x: unknown, label: string) => {
  if (typeof x !== 'number' || !Number.isFinite(x))
    throw new Error(`${label} is invalid`);
  return x;
};
const uniqueStrings = (x: unknown, label: string, allowEmpty = false) => {
  if (
    !Array.isArray(x) ||
    (!allowEmpty && !x.length) ||
    x.some((item) => typeof item !== 'string' || !item) ||
    new Set(x).size !== x.length
  )
    throw new Error(`${label} is invalid`);
  return x as string[];
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
  const stored = z
    ? identity(z.stored, 'stored')
    : identity(
        { sha256: p.sha256, byte_length: p.byte_length },
        'report identity',
      );
  const logical = z ? identity(z.decoded, 'decoded', true) : stored;
  return {
    ...f,
    storedIdentity: {
      sha256: stored.sha256,
      byteLength: stored.byte_length,
    },
    logicalIdentity: {
      sha256: logical.sha256,
      byteLength: logical.byte_length,
      ...(logical.line_count === undefined
        ? {}
        : { lineCount: logical.line_count }),
    },
  } as ReportFile;
}
export type ReportFile = Record<string, any> & {
  file: 'selection_report.json' | 'selection_report.json.zst';
  storedIdentity: { sha256: string; byteLength: number };
  logicalIdentity: { sha256: string; byteLength: number; lineCount?: number };
};

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
export interface ReportDecoder {
  withDecodedFile<T>(
    storedPath: string,
    expectation: DecodeExpectation,
    use: (decodedPath: string) => T | Promise<T>,
  ): Promise<T>;
}

interface ValidatedContinuity {
  byBundle: Map<string, ContinuityBundle>;
  retainedBundleIds: Set<string>;
  dispositions: Map<string, string>;
}

function validateContinuity(report: Record<string, any>): ValidatedContinuity {
  const result: ValidatedContinuity = {
    byBundle: new Map(),
    retainedBundleIds: new Set(),
    dispositions: new Map(),
  };
  if (report.report_version === 1) return result;

  const continuity = obj(report.continuity, 'selection report continuity');
  exactKeys(
    continuity,
    ['bundles', 'retained_bundle_ids', 'dispositions'],
    'selection report continuity',
  );
  if (!Array.isArray(continuity.bundles))
    throw new Error('selection report continuity bundles are invalid');
  for (const rawBundle of continuity.bundles) {
    const bundle = obj(rawBundle, 'continuity bundle');
    exactKeys(
      bundle,
      ['base_run_id', 'bundle_id', 'activation_at', 'score', 'targets'],
      'continuity bundle',
    );
    const bundleId = text(bundle.bundle_id, 'continuity bundle_id');
    if (result.byBundle.has(bundleId))
      throw new Error('selection report repeats a continuity bundle');
    text(bundle.base_run_id, 'continuity base_run_id');
    timestamp(bundle.activation_at, 'continuity activation_at');
    finite(bundle.score, 'continuity score');
    if (!Array.isArray(bundle.targets) || !bundle.targets.length)
      throw new Error('continuity bundle targets are invalid');
    const targetIds = new Set<string>();
    for (const rawTarget of bundle.targets) {
      const target = obj(rawTarget, 'continuity target');
      exactKeys(
        target,
        [
          'target_id',
          'venue',
          'venue_market_id',
          'canonical_class',
          'subscription_ids',
          'activation_at',
          'capture_start_at',
          'source_ref',
          'terminal_probe',
        ],
        'continuity target',
      );
      const venue = text(target.venue, 'continuity target venue');
      const venueMarketId = text(
        target.venue_market_id,
        'continuity target venue_market_id',
      );
      const targetId = text(target.target_id, 'continuity target_id');
      if (
        !supportedVenues.includes(venue) ||
        targetId !== `${venue}:${venueMarketId}` ||
        targetIds.has(targetId)
      )
        throw new Error('continuity target identity is invalid');
      targetIds.add(targetId);
      text(target.canonical_class, 'continuity target canonical_class');
      uniqueStrings(
        target.subscription_ids,
        'continuity target subscription_ids',
      );
      if (
        timestamp(target.activation_at, 'continuity target activation_at') !==
        bundle.activation_at
      )
        throw new Error(
          'continuity target activation disagrees with its bundle',
        );
      timestamp(target.capture_start_at, 'continuity target capture_start_at');
      text(target.source_ref, 'continuity target source_ref');
      const probe = obj(target.terminal_probe, 'continuity terminal_probe');
      exactKeys(probe, ['state', 'reason'], 'continuity terminal_probe');
      if (!['open', 'terminal', 'unknown'].includes(probe.state))
        throw new Error('continuity terminal_probe state is invalid');
      text(probe.reason, 'continuity terminal_probe reason');
    }
    result.byBundle.set(bundleId, bundle as unknown as ContinuityBundle);
  }
  for (const bundleId of uniqueStrings(
    continuity.retained_bundle_ids,
    'selection report retained_bundle_ids',
    true,
  )) {
    if (!result.byBundle.has(bundleId))
      throw new Error('retained bundle is absent from continuity evidence');
    result.retainedBundleIds.add(bundleId);
  }
  const rawDispositions = obj(
    continuity.dispositions,
    'selection report continuity dispositions',
  );
  if (
    Object.keys(rawDispositions).sort().join('\0') !==
    [...result.byBundle.keys()].sort().join('\0')
  )
    throw new Error(
      'continuity dispositions do not account for every prior bundle',
    );
  const allowedDispositions = new Set<string>(continuityDispositions);
  for (const [bundleId, disposition] of Object.entries(rawDispositions)) {
    if (
      typeof disposition !== 'string' ||
      !allowedDispositions.has(disposition)
    )
      throw new Error('continuity disposition is invalid');
    result.dispositions.set(bundleId, disposition);
  }
  if (
    !Array.isArray(report.continuity_diagnostics) ||
    report.continuity_diagnostics.some(
      (diagnostic: unknown) => typeof diagnostic !== 'string' || !diagnostic,
    )
  )
    throw new Error('selection report continuity_diagnostics are invalid');
  if (
    report.continuity_degraded_base_run_id !== null &&
    (typeof report.continuity_degraded_base_run_id !== 'string' ||
      !report.continuity_degraded_base_run_id)
  )
    throw new Error(
      'selection report continuity_degraded_base_run_id is invalid',
    );
  if (
    report.continuity_degraded_base_run_id !== null &&
    (result.byBundle.size || !report.continuity_diagnostics.length)
  )
    throw new Error('selection report degraded continuity state is invalid');
  return result;
}

export async function validateReportPath(
  storedPath: string,
  file: ReportFile,
  runId: string,
  decoder: ReportDecoder | null,
) {
  const compressed = file.file.endsWith('.zst');
  const parse = async (decodedPath: string) => {
    const metadata = await stat(decodedPath);
    if (!metadata.isFile() || metadata.size !== file.logicalIdentity.byteLength)
      throw new Error('selection report decoded identity mismatch');
    if (metadata.size > MAX_DECODED_BYTES)
      throw new Error('selection report exceeds decoded size limit');
    const decoded = await readFile(decodedPath);
    if (!compressed && sha(decoded) !== file.logicalIdentity.sha256)
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
      ![1, 2].includes(r.report_version) ||
      r.mode !== 'shadow' ||
      r.run_id !== runId ||
      typeof r.input_complete !== 'boolean' ||
      !Array.isArray(r.catalogs) ||
      !Array.isArray(r.candidates)
    )
      throw new Error('invalid selection report');
    timestamp(r.generated_at, 'selection report generated_at');
    obj(r.discovery_failures, 'selection report discovery_failures');
    if (
      (r.artifact_format === undefined) !== (r.artifacts === undefined) ||
      (r.artifact_format !== undefined &&
        !['zstd', 'ndjson'].includes(r.artifact_format))
    )
      throw new Error('selection report artifact inventory is invalid');
    const continuity = validateContinuity(r);
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
      const names = Object.keys(artifacts);
      const targetRecords = supportedVenues.map(
        (venue) => `target_records_${venue}${suffix}`,
      );
      const expected = [
        `rule_templates${suffix}`,
        `rule_drift${suffix}`,
        ...[...catalogVenues].flatMap((venue) => [
          `catalog_${venue}_events${suffix}`,
          `catalog_${venue}_markets${suffix}`,
        ]),
        ...(targetRecords.some((name) => names.includes(name))
          ? targetRecords
          : []),
      ];
      if (names.sort().join('\0') !== expected.sort().join('\0'))
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
    const selectedTargetIds = new Set<string>();
    for (const venue of supportedVenues) {
      for (const rawTarget of targets[venue]) {
        const target = obj(rawTarget, 'selection target');
        exactKeys(
          target,
          [
            'target_id',
            'bundle_id',
            'canonical_class',
            'subscription_ids',
            'activation_at',
            'capture_start_at',
            'source_ref',
            ...(r.report_version === 2 ? ['continuity_score'] : []),
          ],
          'selection target',
        );
        const targetId = text(target.target_id, 'selection target_id');
        const bundleId = text(target.bundle_id, 'selection target bundle_id');
        if (
          !targetId.startsWith(`${venue}:`) ||
          selectedTargetIds.has(targetId) ||
          !selection.bundle_ids.includes(bundleId)
        )
          throw new Error('selection target identity is invalid');
        selectedTargetIds.add(targetId);
        text(target.canonical_class, 'selection target canonical_class');
        uniqueStrings(
          target.subscription_ids,
          'selection target subscription_ids',
        );
        timestamp(target.activation_at, 'selection target activation_at');
        timestamp(target.capture_start_at, 'selection target capture_start_at');
        text(target.source_ref, 'selection target source_ref');
        if (r.report_version === 2)
          finite(target.continuity_score, 'selection target continuity_score');
      }
    }
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
        venues.some(
          (venue: unknown) => !supportedVenues.includes(String(venue)),
        )
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
    if (
      selection.bundle_ids.some(
        (id: string) =>
          !candidates.get(id)?.eligible &&
          !continuity.retainedBundleIds.has(id),
      )
    )
      throw new Error('selected bundle is absent or ineligible');
    if (r.report_version === 2) {
      for (const venue of supportedVenues) {
        for (const target of targets[venue]) {
          const continuityBundle = continuity.byBundle.get(target.bundle_id);
          const expectedScore =
            continuityBundle?.score ?? candidates.get(target.bundle_id)?.score;
          if (
            !Number.isFinite(expectedScore) ||
            target.continuity_score !== expectedScore
          )
            throw new Error(
              'selection target continuity_score disagrees with its provenance',
            );
        }
      }
      for (const [bundleId, disposition] of continuity.dispositions) {
        const selected = selection.bundle_ids.includes(bundleId);
        const retained = continuity.retainedBundleIds.has(bundleId);
        if (
          (disposition === 'retained' && (!retained || !selected)) ||
          (disposition !== 'retained' && retained) ||
          (disposition === 'held_current_candidate' && !selected) ||
          ([
            'continuity_budget_trimmed',
            'all_markets_terminal',
            'terminal_clamp_elapsed',
          ].includes(disposition) &&
            selected)
        )
          throw new Error(
            'selection report continuity disposition is inconsistent',
          );
        if (
          disposition === 'all_markets_terminal' &&
          !continuity.byBundle
            .get(bundleId)!
            .targets.every(
              (target) => target.terminal_probe.state === 'terminal',
            )
        )
          throw new Error('all-terminal disposition has a non-terminal probe');
      }
      for (const bundleId of continuity.retainedBundleIds) {
        const expected = continuity.byBundle.get(bundleId)!;
        const selected = supportedVenues.flatMap((venue) =>
          targets[venue].filter(
            (target: Record<string, any>) => target.bundle_id === bundleId,
          ),
        );
        if (
          selected.length !== expected.targets.length ||
          expected.targets.some((continuityTarget) => {
            const target = selected.find(
              (item: Record<string, any>) =>
                item.target_id === continuityTarget.target_id,
            );
            return (
              !target ||
              target.canonical_class !== continuityTarget.canonical_class ||
              target.source_ref !== continuityTarget.source_ref ||
              target.activation_at !== continuityTarget.activation_at ||
              target.capture_start_at !== continuityTarget.capture_start_at ||
              JSON.stringify(target.subscription_ids) !==
                JSON.stringify(continuityTarget.subscription_ids)
            );
          })
        )
          throw new Error(
            'retained selection targets disagree with continuity evidence',
          );
      }
    }
    return r as SelectionReport;
  };
  if (!compressed) return parse(storedPath);
  if (!decoder) throw new Error('selection report decoder is required');
  const logical = file.logicalIdentity;
  if (logical.lineCount === undefined)
    throw new Error('decoded identity is invalid');
  try {
    return await decoder.withDecodedFile(
      storedPath,
      {
        stored: file.storedIdentity,
        logical: { ...logical, lineCount: logical.lineCount },
        maxDecodedBytes: MAX_DECODED_BYTES,
      },
      parse,
    );
  } catch (error) {
    throw new Error(
      `selection report strict Zstd decode failed: ${error instanceof Error ? error.message : 'unknown error'}`,
    );
  }
}
export function summarizeReport(report: SelectionReport): RunSummary {
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
    rejected: cs.filter((candidate) => !sel.has(candidate?.bundle_id)).length,
    targets: targets as number,
    catalogsComplete: cats.filter((x: any) => x?.complete === true).length,
    catalogsTotal: cats.length,
    rejectionReasons: reasons,
  };
}
