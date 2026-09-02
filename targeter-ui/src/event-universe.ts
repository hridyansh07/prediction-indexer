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

export interface UniverseBundle {
  bundle_id: string;
  latest_run_id: string;
  sport: string;
  game: string | null;
  topology: string | null;
  participants: string[];
  activation_at: string;
  capture_start_at: string;
  first_selected_at: string;
  last_selected_at: string;
  occurrence_count: number;
  venues: string[];
  target_count: number;
  lifecycle: 'active' | 'retired';
}

export interface UniverseBundlePage {
  bundles: UniverseBundle[];
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

export interface UniverseFreshness {
  state: 'current' | 'late' | 'unavailable';
  expected_run_seconds: number;
  latest_run_age_seconds: number | null;
  latest_indexed_at: string | null;
}

export interface UniverseTargeterRunSummary {
  run_id: string;
  generated_at: string;
  input_complete: boolean;
  indexed_at: string;
}

export interface UniverseTargeterStatus {
  status_projection_version: 1;
  observed_at: string;
  freshness: UniverseFreshness;
  latest_run: UniverseTargeterRunSummary | null;
  current_complete_run: UniverseTargeterRunSummary | null;
  current_complete_summary: {
    selected_bundles: number;
    selected_targets: number;
    venues: string[];
  };
}

export interface UniverseEvent {
  event_id: string;
  sport: string;
  game: string | null;
  topology: string | null;
  activation_at: string;
  participants: string[];
  participant_keys: string[];
  first_seen_run_id: string;
  last_seen_run_id: string;
  venue_count?: number;
  market_count?: number;
  selected_run_count?: number;
}

export interface UniverseEventPage {
  events: Array<
    UniverseEvent & {
      venue_count: number;
      market_count: number;
      selected_run_count: number;
    }
  >;
  next_cursor: string | null;
}

export interface UniverseCanonicalMarket {
  market_id: string;
  market_template_version: number;
  outcome_space_version: number;
  event_id: string;
  canonical_class: string;
  market_type: string;
  scope: string;
  parameters: Record<string, unknown>;
  first_seen_run_id: string;
  last_seen_run_id: string;
  venue_market_count: number;
  venues: string[];
}

export interface UniverseRelationSummary {
  relation_id: number;
  relation_type: string;
  event_id?: string;
  scope: string;
  coverage: string;
  generation_version: number;
  canonical_hash: string;
}

export interface UniverseEventDetail {
  event: UniverseEvent;
  venue_events: Array<{
    venue: string;
    venue_event_id: string;
    title: string;
    league: string | null;
    status: string;
    source_ref: string;
    format: string | null;
    fragment_type: string | null;
    first_seen_run_id: string;
    last_seen_run_id: string;
  }>;
  markets: UniverseCanonicalMarket[];
  relations: UniverseRelationSummary[];
  observations: Array<{
    run_id: string;
    generated_at: string;
    bundle_id: string;
  }>;
}

export interface UniverseVenueMarket {
  venue: string;
  venue_market_id: string;
  venue_event_id: string;
  event_id: string;
  market_id: string;
  market_template_version: number;
  outcome_space_version: number;
  canonical_class: string;
  market_type: string;
  scope: string;
  title: string;
  parameters: Record<string, unknown>;
  subscription_ids: string[];
  outcome_labels: string[];
  status: string;
  accepting_orders: boolean;
  rules_hash: string | null;
  rule_template_id: string | null;
  source_ref: string;
  created_at: string | null;
  volume_24h: number | null;
  volume_total: number | null;
  volume_total_usd: number | null;
  liquidity: number | null;
  first_seen_run_id: string;
  last_seen_run_id: string;
}

export interface UniverseMarketDetail {
  market: UniverseCanonicalMarket;
  venue_markets: UniverseVenueMarket[];
  selections: Array<{
    run_id: string;
    generated_at: string;
    bundle_id: string;
    venue: string;
    venue_market_id: string;
    continuity_score: number;
    selection_reason: UniverseSelectedMarket['selection_reason'];
    origin_run_id: string;
  }>;
  relations: UniverseRelationSummary[];
}

export interface UniverseRelationDetail {
  relation: {
    relation_id: number;
    relation_type: string;
    generation_version: number;
    canonical_hash: string;
  };
  members: Array<{
    venue: string;
    venue_market_id: string;
    market_id: string;
    market_template_version: number;
    outcome_space_version: number;
    claim_key: string;
    role: string;
  }>;
  observations: Array<{
    run_id: string;
    generated_at: string;
    bundle_id: string;
    event_id: string;
    scope: string;
    coverage: string;
  }>;
}

export interface UniverseRelationshipTypeCatalog {
  relationship_type_catalog_version: 1;
  types: Array<{
    type: string;
    directed: boolean;
    member_roles: string[];
  }>;
}

export interface UniverseTargeterDecision {
  event_id: string;
  bundle_id: string;
  eligible: boolean;
  selected: boolean;
  score: number;
  score_components: Record<string, number>;
  rejection_reasons: string[];
  allocation_rejection: string | null;
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
}

export interface UniverseSelectedMarket {
  event_id: string;
  bundle_id: string;
  venue: string;
  venue_market_id: string;
  market_id: string;
  market_template_version: number;
  outcome_space_version: number;
  canonical_class: string;
  continuity_score: number;
  selection_reason: 'selected' | 'held_current_candidate' | 'retained';
  origin_run_id: string;
}

export interface UniverseTargeterRunDetail {
  run: UniverseTargeterRunSummary;
  source: UniverseSource;
  counts: {
    candidates: number;
    eligible: number;
    selected_events: number;
    selected_markets: number;
    relations: number;
  };
  decisions: UniverseTargeterDecision[];
  events: UniverseEvent[];
  selected_markets: UniverseSelectedMarket[];
  relations: UniverseRelationSummary[];
}

export interface UniverseHealth {
  status: 'ok';
  schema_version: 4;
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
    umbrella_events: number;
    canonical_markets: number;
    venue_markets: number;
    relations: number;
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
