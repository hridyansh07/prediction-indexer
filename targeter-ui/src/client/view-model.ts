import type {
  ContinuityBundle,
  ContinuityDisposition,
  SelectionReport,
  SelectionReportV2,
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
  continuityBaseRunId: string;
}

const finiteScore = (value: unknown) =>
  typeof value === 'number' && Number.isFinite(value)
    ? value
    : Number.NEGATIVE_INFINITY;

export function isReportV2(
  report: SelectionReport,
): report is SelectionReportV2 {
  return report.report_version === 2;
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
  const continuity = isReportV2(report)
    ? new Map(
        report.continuity.bundles.map((bundle) => [bundle.bundle_id, bundle]),
      )
    : new Map<string, ContinuityBundle>();
  const retained = isReportV2(report)
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
    .map((bundleId) => {
      const candidate = candidates.get(bundleId) ?? null;
      const evidence = continuity.get(bundleId) ?? null;
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
        disposition: isReportV2(report)
          ? (report.continuity.dispositions[bundleId] ?? null)
          : null,
        retained: retained.has(bundleId),
        targets: selectedTargets,
        score,
        continuityBaseRunId: evidence?.base_run_id ?? report.run_id,
      };
    })
    .sort(
      (left, right) =>
        right.score - left.score || left.bundleId.localeCompare(right.bundleId),
    );
}

export function isLegitimateTerminalRetirement(
  report: SelectionReport,
): boolean {
  if (!isReportV2(report)) return false;
  const dispositions = Object.values(report.continuity.dispositions);
  return (
    report.selection.bundle_ids.length === 0 &&
    Object.values(report.selection.targets).every(
      (targets) => !targets.length,
    ) &&
    report.continuity.bundles.length > 0 &&
    dispositions.length === report.continuity.bundles.length &&
    dispositions.every((disposition) =>
      ['all_markets_terminal', 'terminal_clamp_elapsed'].includes(disposition),
    )
  );
}
