export type Json =
  | null
  | boolean
  | number
  | string
  | Json[]
  | { [key: string]: Json };

export const continuityDispositions = [
  'retained',
  'held_current_candidate',
  'continuity_budget_trimmed',
  'all_markets_terminal',
  'terminal_clamp_elapsed',
] as const;
export type ContinuityDisposition = (typeof continuityDispositions)[number];
export type TerminalProbeState = 'open' | 'terminal' | 'unknown';
export interface TerminalProbe {
  state: TerminalProbeState;
  reason: string;
}
export interface ContinuityTarget {
  target_id: string;
  venue: 'kalshi' | 'polymarket' | 'limitless';
  venue_market_id: string;
  canonical_class: string;
  subscription_ids: string[];
  activation_at: string;
  capture_start_at: string;
  source_ref: string;
  terminal_probe: TerminalProbe;
}
interface ContinuityBundleBase {
  base_run_id: string;
  bundle_id: string;
  activation_at: string;
  score: number;
  targets: ContinuityTarget[];
}
export type ContinuityBundleV2 = ContinuityBundleBase;
export interface ContinuityBundleV3 extends ContinuityBundleBase {
  origin_run_id: string;
  origin_report_sha256: string;
  origin_archive_manifest_key: string;
  origin_archive_manifest_sha256: string;
}
export type ContinuityBundle = ContinuityBundleV2 | ContinuityBundleV3;
export interface ContinuityState<
  Bundle extends ContinuityBundle = ContinuityBundle,
> {
  bundles: Bundle[];
  retained_bundle_ids: string[];
  dispositions: Record<string, ContinuityDisposition>;
}
export interface SelectionTarget {
  target_id: string;
  bundle_id: string;
  canonical_class: string;
  subscription_ids: string[];
  activation_at: string;
  capture_start_at: string;
  source_ref: string;
  continuity_score?: number;
}
export interface SelectionState {
  bundle_ids: string[];
  bundle_count: number;
  targets: Record<string, SelectionTarget[]>;
  allocation_rejections: Record<string, string>;
  publication_performed: false;
  budget_used?: Record<string, number>;
}
interface SelectionReportBase extends Record<string, any> {
  mode: 'shadow';
  run_id: string;
  generated_at: string;
  input_complete: boolean;
  catalogs: Array<Record<string, any>>;
  candidates: Array<Record<string, any>>;
  selection: SelectionState;
}
export interface SelectionReportV1 extends SelectionReportBase {
  report_version: 1;
}
export interface SelectionReportV2 extends SelectionReportBase {
  report_version: 2;
  continuity: ContinuityState<ContinuityBundleV2>;
  continuity_diagnostics: string[];
  continuity_degraded_base_run_id: string | null;
}
export interface SelectionReportV3 extends SelectionReportBase {
  report_version: 3;
  continuity: ContinuityState<ContinuityBundleV3>;
  continuity_diagnostics: string[];
  continuity_degraded_base_run_id: string | null;
}
export type ContinuitySelectionReport = SelectionReportV2 | SelectionReportV3;
export type SelectionReport = SelectionReportV1 | ContinuitySelectionReport;

export interface TargetResolutionV2 {
  version: 2;
  source: 'targeter_v2';
  run_id: string;
  bundle_id: string;
  target_id: string;
  canonical_class: string;
  activation_at: string;
  capture_start_at: string;
  source_ref: string;
  selection_report_sha256: string;
  archive_manifest_key: string;
  archive_manifest_sha256: string;
  continuity_score: number;
  continuity_base_run_id: string;
}
export interface TargetResolutionV3 {
  version: 3;
  source: 'targeter_v2';
  run_id: string;
  bundle_id: string;
  target_id: string;
  canonical_class: string;
  activation_at: string;
  capture_start_at: string;
  source_ref: string;
  selection_report_sha256: string;
  archive_manifest_key: string;
  archive_manifest_sha256: string;
  continuity_score: number;
  continuity_base_run_id: string | null;
  continuity_origin_run_id: string;
  continuity_origin_report_sha256: string;
  continuity_origin_archive_manifest_key: string;
  continuity_origin_archive_manifest_sha256: string;
}
export interface RunView {
  runId: string;
  generatedAt: string;
  inputComplete: boolean;
  strategyVersion: string | number | null;
  report: SelectionReport;
  summary: RunSummary;
}
export interface RunSummary {
  candidates: number;
  selected: number;
  rejected: number;
  targets: number;
  catalogsComplete: number;
  catalogsTotal: number;
  rejectionReasons: Record<string, number>;
}
export interface Snapshot {
  generatedAt: string;
  stale: boolean;
  refreshing: boolean;
  lastSuccessfulRefresh: string | null;
  lastRefreshError: string | null;
  refreshSeconds: number;
  expectedRunSeconds: number;
  source: 's3' | 'fixture';
  runs: RunView[];
  config: {
    label: string;
    version: string | number | null;
    versionMatchesRunIds: string[];
    value: Json;
  };
}
