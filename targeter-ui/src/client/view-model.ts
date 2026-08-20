import type {
  ContinuityBundle,
  ContinuityBundleV3,
  ContinuityDisposition,
  ContinuitySelectionReport,
  SelectionReport,
  SelectionTarget,
} from '../shared';

export interface SelectedBundleView {
  bundleId: string;
  candidate: Record<string, any> | null;
  continuity: ContinuityBundle | null;
  disposition: ContinuityDisposition | null;
  retained: boolean;
  targets: Array<SelectionTarget & { venue: string }>;
  score: number;
  continuityBaseRunId: string | null;
  continuityOriginRunId: string | null;
  continuityOriginReportSha256: string | null;
  continuityOriginArchiveManifestKey: string | null;
  continuityOriginArchiveManifestSha256: string | null;
  occurrenceKind: 'complete' | 'retained_reference';
}

const finiteScore = (value: unknown) =>
  typeof value === 'number' && Number.isFinite(value)
    ? value
    : Number.NEGATIVE_INFINITY;

export function isContinuityReport(
  report: SelectionReport,
): report is ContinuitySelectionReport {
  return report.report_version !== 1;
}

export function isContinuityBundleV3(
  bundle: ContinuityBundle,
): bundle is ContinuityBundleV3 {
  return 'origin_run_id' in bundle;
}

export function selectedBundleViews(
  report: SelectionReport,
): SelectedBundleView[] {
  const candidates = new Map(
    report.candidates.map((candidate) => [
      String(candidate.bundle_id),
      candidate,
    ]),
  );
  const continuity = isContinuityReport(report)
    ? new Map(
        report.continuity.bundles.map((bundle) => [bundle.bundle_id, bundle]),
      )
    : new Map<string, ContinuityBundle>();
  const retained = isContinuityReport(report)
    ? new Set(report.continuity.retained_bundle_ids)
    : new Set<string>();
  const targets = new Map<string, Array<SelectionTarget & { venue: string }>>();
  for (const [venue, items] of Object.entries(report.selection.targets)) {
    for (const target of items) {
      const current = targets.get(target.bundle_id) ?? [];
      current.push({ ...target, venue });
      targets.set(target.bundle_id, current);
    }
  }

  return report.selection.bundle_ids
    .map((bundleId): SelectedBundleView => {
      const candidate = candidates.get(bundleId) ?? null;
      const evidence = continuity.get(bundleId) ?? null;
      const origin =
        evidence && isContinuityBundleV3(evidence) ? evidence : null;
      const selectedTargets = targets.get(bundleId) ?? [];
      const score = Math.max(
        finiteScore(candidate?.score),
        finiteScore(evidence?.score),
        ...selectedTargets.map((target) =>
          finiteScore(target.continuity_score),
        ),
      );
      return {
        bundleId,
        candidate,
        continuity: evidence,
        disposition: isContinuityReport(report)
          ? (report.continuity.dispositions[bundleId] ?? null)
          : null,
        retained: retained.has(bundleId),
        targets: selectedTargets,
        score,
        continuityBaseRunId:
          evidence?.base_run_id ??
          (report.report_version === 2 ? report.run_id : null),
        continuityOriginRunId:
          origin?.origin_run_id ??
          (report.report_version === 3 ? report.run_id : null),
        continuityOriginReportSha256: origin?.origin_report_sha256 ?? null,
        continuityOriginArchiveManifestKey:
          origin?.origin_archive_manifest_key ?? null,
        continuityOriginArchiveManifestSha256:
          origin?.origin_archive_manifest_sha256 ?? null,
        occurrenceKind: retained.has(bundleId)
          ? 'retained_reference'
          : 'complete',
      };
    })
    .sort(
      (left, right) =>
        right.score - left.score || left.bundleId.localeCompare(right.bundleId),
    );
}

export function legitimateEmptyGenerationReason(
  report: SelectionReport,
): 'retirement' | 'budget_trimmed' | null {
  if (!isContinuityReport(report)) return null;
  const dispositions = Object.values(report.continuity.dispositions);
  const empty =
    report.selection.bundle_ids.length === 0 &&
    Object.values(report.selection.targets).every(
      (targets) => !targets.length,
    ) &&
    report.continuity.bundles.length > 0 &&
    dispositions.length === report.continuity.bundles.length;
  if (!empty) return null;
  if (
    dispositions.every((disposition) =>
      ['all_markets_terminal', 'terminal_clamp_elapsed'].includes(disposition),
    )
  )
    return 'retirement';
  if (
    report.report_version === 3 &&
    dispositions.every(
      (disposition) => disposition === 'continuity_budget_trimmed',
    )
  )
    return 'budget_trimmed';
  return null;
}
