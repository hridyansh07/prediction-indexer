const ROOT = '/api/event-universe';

export async function universeGet<T>(
  path: string,
  signal?: AbortSignal,
): Promise<T> {
  const response = await fetch(`${ROOT}${path}`, {
    method: 'GET',
    headers: { accept: 'application/json' },
    signal,
  });
  if (!response.ok) throw new Error('Event Universe is unavailable.');
  return response.json() as Promise<T>;
}
