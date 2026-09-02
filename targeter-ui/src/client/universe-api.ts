const ROOT = '/api/event-universe';
const SHORT_CACHE_MS = 15_000;
const IMMUTABLE_CACHE_MS = 5 * 60_000;
const MAX_CACHED_RESPONSES = 8;

const values = new Map<string, { expiresAt: number; value: unknown }>();
const pending = new Map<string, Promise<unknown>>();

interface UniverseGetOptions {
  signal?: AbortSignal;
  cache?: boolean;
}

const cacheDuration = (path: string) =>
  /^\/v1\/events\/[^/?]+$/.test(path)
    ? 0
    : path === '/healthz' || path.startsWith('/v1/targeter/status')
      ? SHORT_CACHE_MS
      : IMMUTABLE_CACHE_MS;

export async function universeGet<T>(
  path: string,
  options: UniverseGetOptions = {},
): Promise<T> {
  const cache = options.cache ?? true;
  const now = Date.now();
  const cached = values.get(path);
  if (cache && cached && cached.expiresAt > now) return cached.value as T;
  const existing = pending.get(path);
  if (cache && existing) return existing as Promise<T>;

  const request = fetch(`${ROOT}${path}`, {
    method: 'GET',
    headers: { accept: 'application/json' },
    signal: options.signal,
  })
    .then((response) => {
      if (!response.ok) throw new Error('Event Universe is unavailable.');
      return response.json() as Promise<T>;
    })
    .then((value) => {
      const duration = cacheDuration(path);
      if (cache && duration > 0) {
        values.delete(path);
        values.set(path, { expiresAt: Date.now() + duration, value });
        while (values.size > MAX_CACHED_RESPONSES)
          values.delete(values.keys().next().value!);
      }
      return value;
    })
    .finally(() => {
      if (cache) pending.delete(path);
    });
  if (cache) pending.set(path, request);
  return request;
}
