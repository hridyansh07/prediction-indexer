import {
  dispatchEventUniverseRequest,
  EventUniverseClient,
  universePublicFailure,
} from '../targeter-ui/src/server/event-universe.js';

interface UniverseProxyEnvironment {
  TARGETER_UI_EVENT_UNIVERSE_URL?: string;
  TARGETER_UI_EVENT_UNIVERSE_AUTHORIZATION?: string;
  TARGETER_UI_EVENT_UNIVERSE_TIMEOUT_MS?: string;
  TARGETER_UI_EVENT_UNIVERSE_MAX_RESPONSE_BYTES?: string;
}

export async function GET(request: Request) {
  return handleEventUniverseProxy(request, process.env, fetch);
}

export async function handleEventUniverseProxy(
  request: Request,
  environment: UniverseProxyEnvironment,
  fetchImpl: typeof fetch,
) {
  const baseUrl = environment.TARGETER_UI_EVENT_UNIVERSE_URL;
  if (!baseUrl) return json(503, { error: 'Event Universe is not configured' });

  const requestUrl = new URL(request.url);
  const paths = requestUrl.searchParams.getAll('__universe_path');
  if (paths.length !== 1)
    return json(400, { error: 'Invalid Event Universe request' });
  requestUrl.searchParams.delete('__universe_path');
  const pathname = paths[0].startsWith('/') ? paths[0] : `/${paths[0]}`;

  try {
    const client = new EventUniverseClient({
      baseUrl,
      authorization: environment.TARGETER_UI_EVENT_UNIVERSE_AUTHORIZATION,
      timeoutMs: positive(
        environment.TARGETER_UI_EVENT_UNIVERSE_TIMEOUT_MS,
        5000,
      ),
      maxResponseBytes: positive(
        environment.TARGETER_UI_EVENT_UNIVERSE_MAX_RESPONSE_BYTES,
        2 * 1024 * 1024,
      ),
      fetch: fetchImpl,
    });
    const document = await dispatchEventUniverseRequest(
      client,
      pathname,
      requestUrl.searchParams,
    );
    return json(200, document);
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

function json(status: number, body: unknown) {
  return Response.json(body, {
    status,
    headers: {
      'cache-control': 'no-store',
      'x-content-type-options': 'nosniff',
    },
  });
}
