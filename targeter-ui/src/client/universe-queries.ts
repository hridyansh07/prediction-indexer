import {
  infiniteQueryOptions,
  QueryClient,
  queryOptions,
  useInfiniteQuery,
  useQuery,
} from '@tanstack/react-query';
import type {
  UniverseBundlePage,
  UniverseEventDetail,
  UniverseSelectionDetail,
  UniverseSelectionPage,
  UniverseTargeterRunDetail,
  UniverseTargeterStatus,
} from '../event-universe';
import { universeGet } from './universe-api';

export const STATUS_STALE_MS = 15_000;
export const IMMUTABLE_STALE_MS = 5 * 60_000;
export const QUERY_GC_MS = 5 * 60_000;
export const MAX_BUNDLE_PAGES = 8;
export const BUNDLE_PAGE_SIZE = 100;

export const universeKeys = {
  all: ['event-universe'] as const,
  status: (limit: number) =>
    [...universeKeys.all, 'targeter-status', limit] as const,
  targeterRun: (runId: string) =>
    [...universeKeys.all, 'targeter-run', runId] as const,
  bundles: (limit: number) =>
    [...universeKeys.all, 'bundles', { limit }] as const,
  selection: (runId: string, bundleId: string) =>
    [...universeKeys.all, 'selection', runId, bundleId] as const,
  bundleHistory: (bundleId: string, sort: 'selected', limit: number) =>
    [...universeKeys.all, 'bundle-history', bundleId, { sort, limit }] as const,
  event: (eventId: string) => [...universeKeys.all, 'event', eventId] as const,
};

export function createUniverseQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: {
        gcTime: QUERY_GC_MS,
        refetchOnWindowFocus: false,
        retry: 1,
      },
    },
  });
}

export const targeterStatusQuery = (limit = 5) =>
  queryOptions({
    queryKey: universeKeys.status(limit),
    queryFn: ({ signal }) =>
      universeGet<UniverseTargeterStatus>(
        `/v1/targeter/status?limit=${limit}`,
        signal,
      ),
    staleTime: STATUS_STALE_MS,
    refetchInterval: 60_000,
  });

export const targeterRunQuery = (runId: string) =>
  queryOptions({
    queryKey: universeKeys.targeterRun(runId),
    queryFn: ({ signal }) =>
      universeGet<UniverseTargeterRunDetail>(
        `/v1/targeter/runs/${encodeURIComponent(runId)}`,
        signal,
      ),
    staleTime: IMMUTABLE_STALE_MS,
  });

export const bundlesQuery = (limit = BUNDLE_PAGE_SIZE) =>
  infiniteQueryOptions({
    queryKey: universeKeys.bundles(limit),
    queryFn: ({ pageParam, signal }) => {
      const query = new URLSearchParams({ limit: String(limit) });
      if (pageParam) query.set('cursor', pageParam);
      return universeGet<UniverseBundlePage>(`/v1/bundles?${query}`, signal);
    },
    initialPageParam: null as string | null,
    getNextPageParam: (page) => page.next_cursor ?? undefined,
    getPreviousPageParam: () => undefined,
    maxPages: MAX_BUNDLE_PAGES,
    staleTime: IMMUTABLE_STALE_MS,
  });

export const selectionQuery = (runId: string, bundleId: string) =>
  queryOptions({
    queryKey: universeKeys.selection(runId, bundleId),
    queryFn: ({ signal }) =>
      universeGet<UniverseSelectionDetail>(
        `/v1/runs/${encodeURIComponent(runId)}/selections/${encodeURIComponent(bundleId)}`,
        signal,
      ),
    staleTime: IMMUTABLE_STALE_MS,
    gcTime: 0,
  });

export const bundleHistoryQuery = (
  bundleId: string,
  sort: 'selected' = 'selected',
  limit = BUNDLE_PAGE_SIZE,
) =>
  queryOptions({
    queryKey: universeKeys.bundleHistory(bundleId, sort, limit),
    queryFn: ({ signal }) =>
      universeGet<UniverseSelectionPage>(
        `/v1/bundles/${encodeURIComponent(bundleId)}/history?sort=${sort}&limit=${limit}`,
        signal,
      ),
    staleTime: IMMUTABLE_STALE_MS,
    gcTime: 0,
  });

export const eventDetailQuery = (eventId: string) =>
  queryOptions({
    queryKey: universeKeys.event(eventId),
    queryFn: ({ signal }) =>
      universeGet<UniverseEventDetail>(
        `/v1/events/${encodeURIComponent(eventId)}`,
        signal,
      ),
    staleTime: 0,
    gcTime: 0,
  });

export function useTargeterStatus() {
  return useQuery(targeterStatusQuery());
}

export function useTargeterRun(runId: string | null) {
  return useQuery({
    ...targeterRunQuery(runId ?? ''),
    enabled: Boolean(runId),
  });
}

export function useBundles() {
  return useInfiniteQuery(bundlesQuery());
}

export function useSelectionDetail(
  runId: string | null,
  bundleId: string | null,
) {
  return useQuery({
    ...selectionQuery(runId ?? '', bundleId ?? ''),
    enabled: Boolean(runId && bundleId),
  });
}

export function useBundleHistory(bundleId: string | null) {
  return useQuery({
    ...bundleHistoryQuery(bundleId ?? ''),
    enabled: Boolean(bundleId),
  });
}

export function useEventDetail(eventId: string | null) {
  return useQuery({
    ...eventDetailQuery(eventId ?? ''),
    enabled: Boolean(eventId),
  });
}
