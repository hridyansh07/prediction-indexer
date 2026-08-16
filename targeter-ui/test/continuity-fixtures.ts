import type {
  ContinuityBundle,
  ContinuityTarget,
  SelectionReportV1,
  SelectionReportV2,
  SelectionTarget,
  TerminalProbeState,
} from '../src/shared.js';

export const RUN_ID = '20260816T120000.000001Z';
export const BASE_RUN_ID = '20260816T110000.000001Z';

export function selectedReportV1(): SelectionReportV1 {
  const bundleId = 'bundle-v1-current-candidate';
  return {
    report_version: 1,
    mode: 'shadow',
    run_id: RUN_ID,
    generated_at: '2026-08-16T12:00:00Z',
    input_complete: true,
    discovery_failures: {},
    catalogs: [],
    candidates: [
      {
        bundle_id: bundleId,
        eligible: true,
        rejection_reasons: [],
        event_status: 'ELIGIBLE',
        participants: ['Alpha', 'Beta'],
        venues: ['kalshi'],
        activation_at: '2026-08-16T13:00:00Z',
        capture_start_at: '2026-08-16T12:00:00Z',
        admission: {},
        market_exclusions: {},
        eligible_market_ids: ['kalshi:v1-market'],
        score: 10,
      },
    ],
    selection: {
      bundle_ids: [bundleId],
      bundle_count: 1,
      targets: {
        kalshi: [
          {
            target_id: 'kalshi:v1-market',
            bundle_id: bundleId,
            canonical_class: 'series.moneyline',
            subscription_ids: ['v1-asset'],
            activation_at: '2026-08-16T13:00:00Z',
            capture_start_at: '2026-08-16T12:00:00Z',
            source_ref: 'v1-source',
          },
        ],
        polymarket: [],
        limitless: [],
      },
      allocation_rejections: {},
      publication_performed: false,
    },
  };
}

const continuityTarget = (
  venue: 'kalshi' | 'polymarket',
  state: TerminalProbeState,
  reason: string,
): ContinuityTarget => ({
  target_id: `${venue}:${venue}-old-market`,
  venue,
  venue_market_id: `${venue}-old-market`,
  canonical_class: 'series.moneyline',
  subscription_ids: [`${venue}-asset`],
  activation_at: '2026-08-16T11:30:00Z',
  capture_start_at: '2026-08-16T10:30:00Z',
  source_ref: `${venue}-source`,
  terminal_probe: { state, reason },
});

const selectedTarget = (
  bundleId: string,
  target: ContinuityTarget,
  score: number,
): SelectionTarget => ({
  target_id: target.target_id,
  bundle_id: bundleId,
  canonical_class: target.canonical_class,
  subscription_ids: target.subscription_ids,
  activation_at: target.activation_at,
  capture_start_at: target.capture_start_at,
  source_ref: target.source_ref,
  continuity_score: score,
});

const base = (): SelectionReportV2 => ({
  report_version: 2,
  mode: 'shadow',
  run_id: RUN_ID,
  generated_at: '2026-08-16T12:00:00Z',
  input_complete: true,
  discovery_failures: {},
  catalogs: [],
  candidates: [],
  continuity: { bundles: [], retained_bundle_ids: [], dispositions: {} },
  continuity_diagnostics: [],
  continuity_degraded_base_run_id: null,
  selection: {
    bundle_ids: [],
    bundle_count: 0,
    budget_used: { kalshi: 0, polymarket: 0, limitless: 0 },
    targets: { kalshi: [], polymarket: [], limitless: [] },
    allocation_rejections: {},
    publication_performed: false,
  },
});

export function retainedReportV2(): SelectionReportV2 {
  const bundleId = 'bundle-retained-without-current-candidate';
  const score = 42.5;
  const targets = [
    continuityTarget('kalshi', 'terminal', 'status_finalized'),
    continuityTarget('polymarket', 'unknown', 'terminal_probe_http_404'),
  ];
  const bundle: ContinuityBundle = {
    base_run_id: BASE_RUN_ID,
    bundle_id: bundleId,
    activation_at: '2026-08-16T11:30:00Z',
    score,
    targets,
  };
  const report = base();
  report.continuity = {
    bundles: [bundle],
    retained_bundle_ids: [bundleId],
    dispositions: { [bundleId]: 'retained' },
  };
  report.selection.bundle_ids = [bundleId];
  report.selection.bundle_count = 1;
  report.selection.targets.kalshi = [
    selectedTarget(bundleId, targets[0], score),
  ];
  report.selection.targets.polymarket = [
    selectedTarget(bundleId, targets[1], score),
  ];
  report.selection.budget_used = { kalshi: 1, polymarket: 1, limitless: 0 };
  return report;
}

export function terminalRetirementReportV2(): SelectionReportV2 {
  const terminalId = 'bundle-all-terminal';
  const clampId = 'bundle-clamp-retired';
  const terminalTargets = [
    continuityTarget('kalshi', 'terminal', 'status_finalized'),
    continuityTarget('polymarket', 'terminal', 'accepting_orders_false'),
  ].map((target) => ({ ...target, activation_at: '2026-08-16T03:00:00Z' }));
  const clampTargets = [
    continuityTarget('kalshi', 'unknown', 'terminal_probe_timeout'),
  ].map((target) => ({ ...target, activation_at: '2026-08-16T03:00:00Z' }));
  const terminalBundle: ContinuityBundle = {
    base_run_id: BASE_RUN_ID,
    bundle_id: terminalId,
    activation_at: '2026-08-16T03:00:00Z',
    score: 20,
    targets: terminalTargets,
  };
  const clampBundle: ContinuityBundle = {
    base_run_id: BASE_RUN_ID,
    bundle_id: clampId,
    activation_at: '2026-08-16T03:00:00Z',
    score: 10,
    targets: clampTargets,
  };
  const report = base();
  report.continuity = {
    bundles: [terminalBundle, clampBundle],
    retained_bundle_ids: [],
    dispositions: {
      [terminalId]: 'all_markets_terminal',
      [clampId]: 'terminal_clamp_elapsed',
    },
  };
  return report;
}

export function degradedReportV2(): SelectionReportV2 {
  const report = base();
  report.continuity_diagnostics = [
    'continuity_degraded_after_timeout: committed generation metadata unavailable',
  ];
  report.continuity_degraded_base_run_id = BASE_RUN_ID;
  return report;
}
