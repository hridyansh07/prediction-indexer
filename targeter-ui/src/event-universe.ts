export type UniverseSort = 'activation' | 'selected';
export type OccurrenceKind = 'complete' | 'retained';
export type SelectionDisposition = 'held_current_candidate' | 'retained' | null;
export type RetirementDisposition =
  | 'all_markets_terminal'
  | 'terminal_clamp_elapsed';

export interface UniverseSource {
  manifest_key: string;
  manifest_sha256: string;
  report_key: string;
  report_sha256: string;
}

export interface UniverseOrigin extends UniverseSource {
  run_id: string;
  generated_at: string;
}

export interface UniverseRetirement {
  retired_at: string;
  disposition: RetirementDisposition;
  terminal_observed_at: string | null;
  source: UniverseSource & { run_id: string };
}

export interface UniverseSelection {
  run_id: string;
  generated_at: string;
  bundle_id: string;
  occurrence_kind: OccurrenceKind;
  continuity_selected: boolean;
  continuity_disposition: SelectionDisposition;
  sport: string;
  game: string | null;
  topology: string | null;
  activation_at: string;
  capture_start_at: string;
  retirement: UniverseRetirement | null;
  source: UniverseSource;
  origin: UniverseOrigin;
}

export interface UniverseMarket {
  target_id: string;
  venue: string;
  selected: boolean;
}

export interface UniverseTarget {
  target_id: string;
  venue: string;
  canonical_class: string;
  source_ref: string;
  subscription_ids: string[];
}

export interface UniverseRelationship {
  left: string;
  right: string;
  relationship: string;
  scope: string;
  left_venue: string;
  right_venue: string;
  coverage: string;
}

export interface UniverseContext {
  bundle_id: string;
  sport: string;
  game: string | null;
  topology: string | null;
  participants: string[];
  participant_keys: string[];
  activation_at: string;
  capture_start_at: string;
  event_refs: string[];
  markets: UniverseMarket[];
  targets: UniverseTarget[];
  relationships: UniverseRelationship[];
}

export interface UniverseSelectionDetail extends UniverseSelection {
  context: UniverseContext;
}

export interface UniverseSelectionPage {
  selections: UniverseSelection[];
  sort: UniverseSort;
  next_cursor: string | null;
}

export interface UniverseRun {
  run_id: string;
  generated_at: string;
  generated_at_ns: number;
  input_complete: boolean;
  report_version: 3;
  strategy_version: number;
  manifest_key: string;
  manifest_sha256: string;
  manifest_byte_length: number;
  report_key: string;
  report_sha256: string;
  report_byte_length: number;
  report_decoded_sha256: string;
  report_decoded_byte_length: number;
  projection_version: 1;
  projection_sha256: string;
  projection_row_count: number;
  indexed_at_ns: number;
}

export interface UniverseAudit {
  run_id: string;
  ok: boolean;
  projection_version: number;
  stored_sha256: string;
  actual_sha256: string;
  stored_row_count: number;
  actual_row_count: number;
  selection_row_count: number;
  retirement_row_count: number;
  contexts_ok: boolean;
}

export interface UniverseRunDetail extends UniverseRun {
  audit: UniverseAudit;
}

export interface UniverseRunPage {
  runs: UniverseRun[];
  next_cursor: string | null;
}

export type CadenceFreshnessState = 'current' | 'late' | 'unavailable';

export interface UniverseCadenceFreshness {
  state: CadenceFreshnessState;
  expected_run_seconds: number;
  latest_run_age_seconds: number | null;
  latest_indexed_at: string | null;
}

export type CadenceContinuityDisposition =
  | 'held_current_candidate'
  | 'retained'
  | 'continuity_budget_trimmed'
  | 'all_markets_terminal'
  | 'terminal_clamp_elapsed';

export type CadenceTerminalProbeState = 'open' | 'terminal' | 'unknown';

export interface UniverseCadenceCatalog {
  venue: string;
  complete: boolean;
  events: number;
  markets: number;
  requests: number;
  diagnostics: string[];
  classification_diagnostic_count: number;
  classification_diagnostics_by_code: Record<string, number>;
}

export interface UniverseCadenceRelationship {
  bundle_id: string;
  left: string;
  right: string;
  relationship: string;
  scope: string;
  left_venue: string;
  right_venue: string;
  cross_venue: boolean;
  coverage: string;
}

export interface UniverseCadenceCandidate {
  bundle_id: string;
  sport: string;
  game: string | null;
  topology: string | null;
  participants: string[];
  participant_keys: string[];
  event_refs: string[];
  activation_at: string;
  capture_start_at: string;
  score: number;
  score_components: Record<string, number>;
  eligible: boolean;
  event_status: 'ELIGIBLE' | 'REJECTED';
  rejection_reasons: string[];
  admission: {
    combined_moneyline_volume_usd: number;
    minimum_moneyline_volume_usd: number;
    moneyline_volume_usd_by_venue: Record<string, number>;
    moneyline_volume_usd_coverage: Record<
      string,
      { known_markets: number; unknown_markets: number }
    >;
  };
  market_exclusions: Record<string, string[]>;
  eligible_market_ids: string[];
  selected: boolean;
  allocation_rejection: string | null;
  relationship_analysis: {
    relationships: UniverseCadenceRelationship[];
    diagnostics?: string[];
    outcome_spaces?: Array<Record<string, unknown>>;
  };
}

export interface UniverseCadenceMatchRejection {
  sport?: string;
  game?: string | null;
  topology?: string | null;
  participant_keys?: string[];
  event_refs?: string[];
  reason?: string;
  details?: Record<string, unknown>;
}

export interface UniverseCadenceSelectedTarget {
  target_id: string;
  bundle_id: string;
  canonical_class: string;
  subscription_ids: string[];
  activation_at: string;
  capture_start_at: string;
  source_ref: string;
  continuity_score: number;
}

export interface UniverseCadenceContinuityTarget {
  target_id: string;
  venue: string;
  canonical_class: string;
  subscription_ids: string[];
  activation_at: string;
  capture_start_at: string;
  source_ref: string;
  terminal_probe: {
    state: CadenceTerminalProbeState;
    reason: string;
  };
}

export interface UniverseCadenceContinuityBundle {
  base_run_id: string;
  bundle_id: string;
  activation_at: string;
  score: number;
  origin_run_id?: string;
  disposition: CadenceContinuityDisposition;
  targets: UniverseCadenceContinuityTarget[];
}

export interface UniverseCadenceRun extends UniverseRun {
  catalogs: UniverseCadenceCatalog[];
  discovery_failures: Record<string, string>;
  counts: {
    candidates: number;
    eligible: number;
    selected: number;
    rejected: number;
    retained: number;
    retired: number;
  };
  reason_summaries: {
    candidate_rejections: Record<string, number>;
    allocation_rejections: Record<string, number>;
    continuity_dispositions: Record<string, number>;
  };
  match_rejections: UniverseCadenceMatchRejection[];
  candidates: UniverseCadenceCandidate[];
  selected_targets: Record<string, UniverseCadenceSelectedTarget[]>;
  budget_used: Record<string, number>;
  continuity: {
    bundles: UniverseCadenceContinuityBundle[];
    retained_bundle_ids: string[];
    dispositions: Record<string, CadenceContinuityDisposition>;
  };
  diagnostics: {
    continuity: string[];
    continuity_degraded_base_run_id: string | null;
    target_records: Record<string, string[]>;
  };
  selections: UniverseSelectionDetail[];
}

export interface UniverseCadence {
  cadence_projection_version: 1;
  observed_at: string;
  freshness: UniverseCadenceFreshness;
  runs: UniverseCadenceRun[];
}

export interface UniverseHealth {
  status: 'ok';
  schema_version: number;
  latest_run: {
    run_id: string;
    generated_at: string;
    indexed_at_ns: number;
    input_complete: boolean;
    age_seconds: number;
    stale_after_seconds: number;
    stale: boolean;
  } | null;
  counts: {
    targeter_runs: number;
    selection_occurrences: number;
    bundle_retirements: number;
    bundle_contexts: number;
    context_targets: number;
  };
}

export interface UniverseFilters {
  activation_start?: string;
  activation_end?: string;
  selected_start?: string;
  selected_end?: string;
  venue?: string;
  sort?: UniverseSort;
  limit?: string;
}
