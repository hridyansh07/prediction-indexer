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
