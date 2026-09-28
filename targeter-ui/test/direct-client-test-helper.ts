import {
  dispatchEventUniverseRequest,
  EventUniverseClient,
  universePublicFailure,
} from '../src/server/event-universe.js';

type Environment = {
  UNIVERSE_API_BASE_URL?: string;
  UNIVERSE_API_AUTHORIZATION?: string;
  UNIVERSE_API_TIMEOUT_MS?: string;
  UNIVERSE_API_MAX_RESPONSE_BYTES?: string;
};

const cache = new Map<string, { expiresAt: number; document: unknown }>();
const fetchIds = new WeakMap<object, number>();
let nextFetchId = 0;

/** Legacy request-shaped harness retained only to exercise the shared closed
 * Event Universe codecs after browser traffic moved to the direct API client. */
export async function handleDirectUniverseTestRequest(
  request: Request,
  environment: Environment,
  fetchImpl: typeof fetch,
) {
  const baseUrl = environment.UNIVERSE_API_BASE_URL;
  if (!baseUrl)
    return Response.json(
      { error: 'Event Universe is not configured' },
      { status: 503 },
    );
  const url = new URL(request.url);
  const paths = url.searchParams.getAll('__universe_path');
  if (paths.length !== 1)
    return Response.json(
      { error: 'Invalid Event Universe request' },
      { status: 400 },
    );
  url.searchParams.delete('__universe_path');
  url.searchParams.delete('universePath');
  const pathname = paths[0].startsWith('/') ? paths[0] : `/${paths[0]}`;
  const maxAge =
    pathname === '/healthz' || pathname === '/v1/targeter/status'
      ? 15
      : /^\/v1\/events\/[^/]+$/.test(pathname)
        ? 0
        : pathname.startsWith('/v1/')
          ? 300
          : 0;
  const fetchId = fetchIds.get(fetchImpl) ?? ++nextFetchId;
  fetchIds.set(fetchImpl, fetchId);
  const key = `${fetchId}:${baseUrl}:${pathname}?${url.searchParams}`;
  const hit = cache.get(key);
  if (hit && hit.expiresAt > Date.now()) return Response.json(hit.document);
  try {
    const client = new EventUniverseClient({
      baseUrl,
      authorization: environment.UNIVERSE_API_AUTHORIZATION,
      timeoutMs: environment.UNIVERSE_API_TIMEOUT_MS
        ? Number(environment.UNIVERSE_API_TIMEOUT_MS)
        : 5000,
      maxResponseBytes: environment.UNIVERSE_API_MAX_RESPONSE_BYTES
        ? Number(environment.UNIVERSE_API_MAX_RESPONSE_BYTES)
        : 1_750_000,
      fetch: fetchImpl,
    });
    const document = await dispatchEventUniverseRequest(
      client,
      pathname,
      url.searchParams,
    );
    if (maxAge)
      cache.set(key, { expiresAt: Date.now() + maxAge * 1000, document });
    return Response.json(document, {
      headers: {
        'cache-control': maxAge ? `private, max-age=${maxAge}` : 'no-store',
      },
    });
  } catch (error) {
    const failure = universePublicFailure(error);
    return Response.json(failure.body, { status: failure.status });
  }
}
