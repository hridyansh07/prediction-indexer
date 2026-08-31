import {
  dispatchEventUniverseRequest,
  EventUniverseClient,
  universePublicFailure,
} from '../targeter-ui/src/server/event-universe.js';

interface UniverseProxyEnvironment {
  UNIVERSE_API_BASE_URL?: string;
  UNIVERSE_API_AUTHORIZATION?: string;
  UNIVERSE_API_TIMEOUT_MS?: string;
  UNIVERSE_API_MAX_RESPONSE_BYTES?: string;
}

const RESPONSE_BUDGET_BYTES = 1_750_000;
const SHORT_CACHE_SECONDS = 15;
const IMMUTABLE_CACHE_SECONDS = 300;
const cache = new Map<string, { expiresAt: number; document: unknown }>();
const fetchIds = new WeakMap<object, number>();
let nextFetchId = 0;

export async function GET(request: Request) {
  return handleEventUniverseProxy(request, process.env, fetch);
}

export async function handleEventUniverseProxy(
  request: Request,
  environment: UniverseProxyEnvironment,
  fetchImpl: typeof fetch,
) {
  const baseUrl = environment.UNIVERSE_API_BASE_URL;
  if (!baseUrl) return json(503, { error: 'Event Universe is not configured' });

  const requestUrl = new URL(request.url);
  const paths = requestUrl.searchParams.getAll('__universe_path');
  if (paths.length !== 1)
    return json(400, { error: 'Invalid Event Universe request' });
  requestUrl.searchParams.delete('__universe_path');
  // Vercel echoes the matched source group of the vercel.json rewrite into the
  // destination query as well as substituting it, so `/api/event-universe/healthz`
  // arrives as `__universe_path=/healthz&universePath=healthz`. What remains here
  // is forwarded to dispatchEventUniverseRequest, whose per-route allow-lists
  // reject any unknown key outright — `requireNoQuery` rejects every key — so the
  // echo has to be dropped or every request fails as an invalid one. The group is
  // named `universePath` in vercel.json purely so this deletion is unambiguous;
  // the two names must change together.
  requestUrl.searchParams.delete('universePath');
  const pathname = paths[0].startsWith('/') ? paths[0] : `/${paths[0]}`;
  const maxAge = cacheMaxAge(pathname);
  const fetchId = fetchIds.get(fetchImpl) ?? ++nextFetchId;
  fetchIds.set(fetchImpl, fetchId);
  const cacheKey = `${fetchId}:${baseUrl}:${pathname}?${requestUrl.searchParams}`;
  const cached = cache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now())
    return json(
      200,
      cached.document,
      Math.ceil((cached.expiresAt - Date.now()) / 1000),
    );

  try {
    const client = new EventUniverseClient({
      baseUrl,
      authorization: environment.UNIVERSE_API_AUTHORIZATION,
      timeoutMs: positive(environment.UNIVERSE_API_TIMEOUT_MS, 5000),
      maxResponseBytes: bounded(
        environment.UNIVERSE_API_MAX_RESPONSE_BYTES,
        RESPONSE_BUDGET_BYTES,
      ),
      fetch: fetchImpl,
    });
    const document = await dispatchEventUniverseRequest(
      client,
      pathname,
      requestUrl.searchParams,
    );
    if (maxAge > 0)
      cache.set(cacheKey, {
        expiresAt: Date.now() + maxAge * 1000,
        document,
      });
    return json(200, document, maxAge);
  } catch (error) {
    const failure = universePublicFailure(error);
    return json(failure.status, failure.body);
  }
}

function positive(value: string | undefined, fallback: number) {
  const parsed = value === undefined ? fallback : Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0)
    throw new Error('Event Universe proxy bounds are invalid');
  return parsed;
}

function bounded(value: string | undefined, fallback: number) {
  const parsed = positive(value, fallback);
  if (parsed > RESPONSE_BUDGET_BYTES)
    throw new Error(
      'Event Universe response limit exceeds the application budget',
    );
  return parsed;
}

function cacheMaxAge(pathname: string) {
  if (pathname === '/healthz' || pathname === '/v1/targeter/status')
    return SHORT_CACHE_SECONDS;
  if (
    pathname.startsWith('/v1/') &&
    (pathname.startsWith('/v1/targeter/runs/') ||
      pathname.startsWith('/v1/bundles') ||
      pathname.startsWith('/v1/runs/') ||
      pathname === '/v1/runs' ||
      pathname === '/v1/selections')
  )
    return IMMUTABLE_CACHE_SECONDS;
  return 0;
}

function json(status: number, body: unknown, maxAge = 0) {
  const serialized = JSON.stringify(body);
  if (new TextEncoder().encode(serialized).byteLength > RESPONSE_BUDGET_BYTES) {
    return Response.json(
      { error: 'Event Universe response exceeds size budget' },
      {
        status: 502,
        headers: {
          'cache-control': 'no-store',
          'x-content-type-options': 'nosniff',
        },
      },
    );
  }
  return Response.json(body, {
    status,
    headers: {
      'cache-control': maxAge > 0 ? `private, max-age=${maxAge}` : 'no-store',
      'x-content-type-options': 'nosniff',
    },
  });
}
