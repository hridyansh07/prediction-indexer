import { UniverseClient } from './universe-client';

const ROOT =
  (import.meta.env?.VITE_UNIVERSE_API_BASE_URL as string | undefined) ??
  (typeof window === 'undefined' ? 'http://localhost' : null);
const client = ROOT
  ? new UniverseClient({
      baseUrl: ROOT,
      fetch: (...arguments_) => fetch(...arguments_),
    })
  : null;

export async function universeGet<T>(
  path: string,
  signal?: AbortSignal,
): Promise<T> {
  if (!client) throw new Error('VITE_UNIVERSE_API_BASE_URL is required');
  return client.get(path, (value) => value as T, signal);
}
