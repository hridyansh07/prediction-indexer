import type { UniverseFilters, UniverseSelection } from '../event-universe';

export const universeLabel = (value: string | null | undefined) =>
  value
    ? value
        .replaceAll('_', ' ')
        .replace(/\b\w/g, (character) => character.toUpperCase())
    : '—';

export const UNIVERSE_RENDER_PAGE_SIZE = 100;

export function boundedRenderPage<T>(items: T[], page: number) {
  const pageCount = Math.max(
    1,
    Math.ceil(items.length / UNIVERSE_RENDER_PAGE_SIZE),
  );
  const currentPage = Math.min(Math.max(0, page), pageCount - 1);
  const start = currentPage * UNIVERSE_RENDER_PAGE_SIZE;
  return {
    items: items.slice(start, start + UNIVERSE_RENDER_PAGE_SIZE),
    currentPage,
    pageCount,
  };
}

export function occurrenceExplanation(selection: UniverseSelection) {
  if (selection.occurrence_kind === 'retained')
    return 'Retained reference — exact context and targets resolve to the immutable origin run.';
  if (selection.continuity_disposition === 'held_current_candidate')
    return 'Complete occurrence — this current candidate was protected by continuity.';
  return 'Complete occurrence — selected from this run’s current candidates.';
}

export function retirementExplanation(selection: UniverseSelection) {
  if (!selection.retirement) return 'No retirement observation indexed.';
  if (selection.retirement.disposition === 'all_markets_terminal')
    return `All selected markets were observed terminal by ${selection.retirement.terminal_observed_at}. This is an indexed upper-bound observation, not an exact match end.`;
  return `Safely evicted at ${selection.retirement.retired_at} after the terminal clamp elapsed. This is not evidence that the match ended.`;
}

export function universeFilterQuery(filters: UniverseFilters, cursor?: string) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(filters)) {
    if (!value) continue;
    const normalized =
      key.endsWith('_start') || key.endsWith('_end')
        ? localDateToUtc(value)
        : value;
    if (normalized) query.set(key, normalized);
  }
  if (cursor) query.set('cursor', cursor);
  return query;
}

function localDateToUtc(value: string) {
  const parsed = new Date(value);
  return Number.isNaN(parsed.valueOf()) ? null : parsed.toISOString();
}
