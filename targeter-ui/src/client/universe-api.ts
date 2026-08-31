const ROOT = '/api/event-universe';
const SHORT_CACHE_MS = 15_000;
const IMMUTABLE_CACHE_MS = 5 * 60_000;

const values = new Map<string, { expiresAt: number; value: unknown }>();
const pending = new Map<string, Promise<unknown>>();

const cacheDuration = (path: string) =>
  path === '/healthz' || path.startsWith('/v1/targeter/status')
    ? SHORT_CACHE_MS
    : IMMUTABLE_CACHE_MS;

export async function universeGet<T>(path: string): Promise<T> {
  const now = Date.now();
  const cached = values.get(path);
  if (cached && cached.expiresAt > now) return cached.value as T;
  const existing = pending.get(path);
  if (existing) return existing as Promise<T>;

  const request = fetch(`${ROOT}${path}`, {
    method: 'GET',
    headers: { accept: 'application/json' },
  })
    .then((response) => {
      if (!response.ok) throw new Error('Event Universe is unavailable.');
      return response.json() as Promise<T>;
    })
    .then((value) => {
      values.set(path, { expiresAt: Date.now() + cacheDuration(path), value });
      return value;
    })
    .finally(() => pending.delete(path));
  pending.set(path, request);
  return request;
}
