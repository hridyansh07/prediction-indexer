import {
  Router,
  type Request,
  type Response as ExpressResponse,
} from 'express';
import type {
  RetirementDisposition,
  UniverseAudit,
  UniverseBundle,
  UniverseBundlePage,
  UniverseCadence,
  UniverseCadenceRun,
  UniverseEventDetail,
  UniverseEventPage,
  UniverseMarketDetail,
  UniverseRelationDetail,
  UniverseRelationSummary,
  UniverseRelationshipTypeCatalog,
  UniverseSelectedMarket,
  UniverseTargeterDecision,
  UniverseTargeterRunDetail,
  UniverseTargeterRunSummary,
  UniverseTargeterStatus,
  UniverseContext,
  UniverseHealth,
  UniverseOrigin,
  UniverseRun,
  UniverseRunDetail,
  UniverseRunPage,
  UniverseSelection,
  UniverseSelectionDetail,
  UniverseSelectionPage,
  UniverseSource,
} from '../event-universe.js';

const SELECTION_QUERY = new Set([
  'activation_start',
  'activation_end',
  'selected_start',
  'selected_end',
  'venue',
  'sort',
  'limit',
  'cursor',
]);
const RUN_QUERY = new Set([
  'generated_start',
  'generated_end',
  'input_complete',
  'limit',
  'cursor',
]);
const CADENCE_QUERY = new Set(['limit']);
const RESPONSE_BUDGET_BYTES = 1_750_000;
const DETAIL_ROW_LIMIT = 1000;
const BUNDLE_QUERY = new Set(['limit', 'cursor']);
const EVENT_QUERY = new Set(['limit', 'cursor']);
const MARKET_QUERY = new Set([
  'market_template_version',
  'outcome_space_version',
]);
const POSITIVE_INTEGER_FIELDS = new Set([
  'market_template_version',
  'outcome_space_version',
]);
const TIMESTAMP_FIELDS = new Set([
  'activation_start',
  'activation_end',
  'selected_start',
  'selected_end',
  'generated_start',
  'generated_end',
]);
const RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$/;
const SHA256 = /^[a-f0-9]{64}$/;
const SAFE_ERROR = { error: 'Event Universe unavailable' };
const INVALID_ERROR = { error: 'Invalid Event Universe request' };

type Fetch = typeof fetch;
type Validator<T> = (value: unknown) => T;

export interface EventUniverseClientOptions {
  baseUrl: string;
  authorization?: string;
  timeoutMs?: number;
  maxResponseBytes?: number;
  fetch?: Fetch;
}

export class EventUniverseClient {
  private readonly baseUrl: URL;
  private readonly timeoutMs: number;
  private readonly maxResponseBytes: number;
  private readonly fetchImpl: Fetch;

  constructor(private readonly options: EventUniverseClientOptions) {
    this.baseUrl = new URL(options.baseUrl);
    if (!['http:', 'https:'].includes(this.baseUrl.protocol))
      throw new Error('Event Universe URL must use HTTP or HTTPS');
    if (this.baseUrl.search || this.baseUrl.hash)
      throw new Error(
        'Event Universe URL must not contain a query or fragment',
      );
    this.baseUrl.pathname = `${this.baseUrl.pathname.replace(/\/+$/, '')}/`;
    this.timeoutMs = positive(options.timeoutMs ?? 5000, 'timeout');
    this.maxResponseBytes = positive(
      options.maxResponseBytes ?? RESPONSE_BUDGET_BYTES,
      'response limit',
    );
    if (this.maxResponseBytes > RESPONSE_BUDGET_BYTES)
      throw new Error('response limit exceeds the application budget');
    this.fetchImpl = options.fetch ?? fetch;
  }

  health() {
    return this.get('healthz', new URLSearchParams(), validateHealth);
  }

  runs(query: URLSearchParams) {
    return this.get(
      'v1/runs',
      validateUniverseQuery(query, RUN_QUERY),
      validateRunPage,
    );
  }

  run(runId: string) {
    return this.get(
      `v1/runs/${encodeId(runId)}`,
      new URLSearchParams(),
      validateRunDetail,
    );
  }

  selections(query: URLSearchParams) {
    return this.get(
      'v1/selections',
      validateUniverseQuery(query, SELECTION_QUERY),
      validateSelectionPage,
    );
  }

  bundles(query: URLSearchParams) {
    return this.get(
      'v1/bundles',
      validateUniverseQuery(query, BUNDLE_QUERY),
      validateBundlePage,
    );
  }

  runSelections(runId: string, query: URLSearchParams) {
    return this.get(
      `v1/runs/${encodeId(runId)}/selections`,
      validateUniverseQuery(query, SELECTION_QUERY),
      validateSelectionPage,
    );
  }

  selection(runId: string, bundleId: string) {
    return this.get(
      `v1/runs/${encodeId(runId)}/selections/${encodeId(bundleId)}`,
      new URLSearchParams(),
      validateSelectionDetail,
    );
  }

  bundleHistory(bundleId: string, query: URLSearchParams) {
    return this.get(
      `v1/bundles/${encodeId(bundleId)}/history`,
      validateUniverseQuery(query, SELECTION_QUERY),
      validateSelectionPage,
    );
  }

  status(query: URLSearchParams) {
    const validated = validateUniverseQuery(query, CADENCE_QUERY);
    const limit = validated.get('limit');
    if (limit !== null && Number(limit) > 5) throw new UniverseRequestError();
    return this.get('v1/targeter/status', validated, validateTargeterStatus);
  }

  targeterRun(runId: string) {
    return this.get(
      `v1/targeter/runs/${encodeId(runId)}`,
      new URLSearchParams(),
      validateTargeterRun,
    );
  }

  event(eventId: string) {
    return this.get(
      `v1/events/${encodeId(eventId)}`,
      new URLSearchParams(),
      validateEventDetail,
    );
  }

  events(query: URLSearchParams) {
    return this.get(
      'v1/events',
      validateUniverseQuery(query, EVENT_QUERY),
      validateEventPage,
    );
  }

  market(marketId: string, query: URLSearchParams) {
    return this.get(
      `v1/markets/${encodeId(marketId)}`,
      validateUniverseQuery(query, MARKET_QUERY),
      validateMarketDetail,
    );
  }

  relation(relationId: string) {
    if (!/^[1-9]\d*$/.test(relationId)) throw new UniverseRequestError();
    return this.get(
      `v1/relations/${relationId}`,
      new URLSearchParams(),
      validateRelationDetail,
    );
  }

  relationshipTypes() {
    return this.get(
      'v1/relationship-types',
      new URLSearchParams(),
      validateRelationshipTypes,
    );
  }

  private async get<T>(
    path: string,
    query: URLSearchParams,
    validate: Validator<T>,
  ) {
    const url = new URL(path, this.baseUrl);
    url.search = query.toString();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const response = await this.fetchImpl(url, {
        headers: {
          accept: 'application/json',
          ...(this.options.authorization
            ? { authorization: this.options.authorization }
            : {}),
        },
        redirect: 'error',
        signal: controller.signal,
      });
      if (!response.ok) throw new UniverseUpstreamError();
      if (
        !response.headers
          .get('content-type')
          ?.toLowerCase()
          .includes('application/json')
      )
        throw new UniverseUpstreamError();
      const stated = Number(response.headers.get('content-length'));
      if (Number.isFinite(stated) && stated > this.maxResponseBytes)
        throw new UniverseUpstreamError();
      const bytes = await readBounded(response, this.maxResponseBytes);
      let document: unknown;
      try {
        document = JSON.parse(
          new TextDecoder('utf-8', { fatal: true }).decode(bytes),
        );
      } catch {
        throw new UniverseUpstreamError();
      }
      return validate(document);
    } catch (error) {
      if (
        error instanceof UniverseRequestError ||
        error instanceof UniverseUpstreamError
      )
        throw error;
      if (
        controller.signal.aborted ||
        (error instanceof Error && error.name === 'AbortError')
      )
        throw new UniverseTimeoutError();
      throw new UniverseUpstreamError();
    } finally {
      clearTimeout(timer);
    }
  }
}

export function createEventUniverseRouter(client: EventUniverseClient | null) {
  const router = Router();
  router.use(async (request: Request, response: ExpressResponse) => {
    if (/^\/v1\/events\/[^/]+$/.test(request.path))
      response.setHeader('cache-control', 'no-store');
    if (!client) return response.status(503).json(SAFE_ERROR);
    if (request.method !== 'GET')
      return response.status(405).json({ error: 'Method not allowed' });
    try {
      return response.json(
        await dispatchEventUniverseRequest(
          client,
          request.path,
          requestQuery(request),
        ),
      );
    } catch (error) {
      const failure = universePublicFailure(error);
      return response.status(failure.status).json(failure.body);
    }
  });
  return router;
}

export async function dispatchEventUniverseRequest(
  client: EventUniverseClient,
  pathname: string,
  query: URLSearchParams,
) {
  if (pathname === '/healthz') {
    requireNoQuery(query);
    return client.health();
  }
  if (pathname === '/v1/runs') return client.runs(query);
  if (pathname === '/v1/selections') return client.selections(query);
  if (pathname === '/v1/bundles') return client.bundles(query);
  if (pathname === '/v1/events') return client.events(query);
  if (pathname === '/v1/relationship-types') {
    requireNoQuery(query);
    return client.relationshipTypes();
  }
  if (pathname === '/v1/targeter/status') return client.status(query);
  let match = /^\/v1\/targeter\/runs\/([^/]+)$/.exec(pathname);
  if (match) {
    requireNoQuery(query);
    return client.targeterRun(pathSegment(match[1]));
  }
  match = /^\/v1\/events\/([^/]+)$/.exec(pathname);
  if (match) {
    requireNoQuery(query);
    return client.event(pathSegment(match[1]));
  }
  match = /^\/v1\/markets\/([^/]+)$/.exec(pathname);
  if (match) return client.market(pathSegment(match[1]), query);
  match = /^\/v1\/relations\/([^/]+)$/.exec(pathname);
  if (match) {
    requireNoQuery(query);
    return client.relation(pathSegment(match[1]));
  }

  match = /^\/v1\/runs\/([^/]+)$/.exec(pathname);
  if (match) {
    requireNoQuery(query);
    return client.run(pathSegment(match[1]));
  }
  match = /^\/v1\/runs\/([^/]+)\/selections$/.exec(pathname);
  if (match) return client.runSelections(pathSegment(match[1]), query);
  match = /^\/v1\/runs\/([^/]+)\/selections\/([^/]+)$/.exec(pathname);
  if (match) {
    requireNoQuery(query);
    return client.selection(pathSegment(match[1]), pathSegment(match[2]));
  }
  match = /^\/v1\/bundles\/([^/]+)\/history$/.exec(pathname);
  if (match) return client.bundleHistory(pathSegment(match[1]), query);
  throw new UniverseRouteNotFoundError();
}

export function universePublicFailure(error: unknown) {
  if (error instanceof UniverseRouteNotFoundError)
    return {
      status: 404,
      body: { error: 'Event Universe route not found' },
    } as const;
  if (error instanceof UniverseRequestError)
    return { status: 400, body: INVALID_ERROR } as const;
  return {
    status: error instanceof UniverseTimeoutError ? 504 : 502,
    body: SAFE_ERROR,
  } as const;
}

export function requestQuery(request: Pick<Request, 'originalUrl'>) {
  const query = request.originalUrl.split('?', 2)[1] ?? '';
  return new URLSearchParams(query);
}

function requireNoQuery(query: URLSearchParams) {
  if ([...query].length) throw new UniverseRequestError();
}

function pathSegment(value: string) {
  try {
    const decoded = decodeURIComponent(value);
    if (!decoded || decoded.includes('/')) throw new UniverseRequestError();
    return decoded;
  } catch (error) {
    if (error instanceof UniverseRequestError) throw error;
    throw new UniverseRequestError();
  }
}

export function validateUniverseQuery(
  query: URLSearchParams,
  allowed: Set<string>,
) {
  const result = new URLSearchParams();
  const seen = new Set<string>();
  for (const [key, value] of query) {
    if (!allowed.has(key) || seen.has(key) || !value || value.length > 4096)
      throw new UniverseRequestError();
    seen.add(key);
    if (
      TIMESTAMP_FIELDS.has(key) &&
      (!RFC3339.test(value) || Number.isNaN(Date.parse(value)))
    )
      throw new UniverseRequestError();
    if (key === 'sort' && !['activation', 'selected'].includes(value))
      throw new UniverseRequestError();
    if (key === 'input_complete' && !['true', 'false'].includes(value))
      throw new UniverseRequestError();
    if (
      key === 'limit' &&
      (!/^\d+$/.test(value) || Number(value) < 1 || Number(value) > 100)
    )
      throw new UniverseRequestError();
    if (POSITIVE_INTEGER_FIELDS.has(key) && !/^[1-9]\d*$/.test(value))
      throw new UniverseRequestError();
    result.append(key, value);
  }
  return result;
}

function encodeId(value: string) {
  if (!value || value.includes('/') || value.length > 1024)
    throw new UniverseRequestError();
  return encodeURIComponent(value);
}

async function readBounded(response: Response, limit: number) {
  if (!response.body) throw new UniverseUpstreamError();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > limit) {
        await reader.cancel();
        throw new UniverseUpstreamError();
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const output = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}

class UniverseRequestError extends Error {}
class UniverseRouteNotFoundError extends Error {}
class UniverseUpstreamError extends Error {}
class UniverseTimeoutError extends UniverseUpstreamError {}

const object = (value: unknown, keys: string[], label: string) => {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new UniverseUpstreamError(`${label} is invalid`);
  const record = value as Record<string, unknown>;
  if (Object.keys(record).sort().join('\0') !== [...keys].sort().join('\0'))
    throw new UniverseUpstreamError(`${label} fields are invalid`);
  return record;
};
const text = (value: unknown) => {
  if (typeof value !== 'string' || !value) throw new UniverseUpstreamError();
  return value;
};
const possiblyEmptyText = (value: unknown) => {
  if (typeof value !== 'string') throw new UniverseUpstreamError();
  return value;
};
const nullableText = (value: unknown) => (value === null ? null : text(value));
const number = (value: unknown) => {
  if (typeof value !== 'number' || !Number.isFinite(value))
    throw new UniverseUpstreamError();
  return value;
};
const integer = (value: unknown) => {
  const result = number(value);
  if (!Number.isInteger(result) || result < 0)
    throw new UniverseUpstreamError();
  return result;
};
const positiveInteger = (value: unknown) => {
  const result = integer(value);
  if (result === 0) throw new UniverseUpstreamError();
  return result;
};
const nullableNumber = (value: unknown) =>
  value === null ? null : number(value);
const boolean = (value: unknown) => {
  if (typeof value !== 'boolean') throw new UniverseUpstreamError();
  return value;
};
const timestamp = (value: unknown) => {
  const result = text(value);
  if (!RFC3339.test(result) || Number.isNaN(Date.parse(result)))
    throw new UniverseUpstreamError();
  return result;
};
const sha = (value: unknown) => {
  const result = text(value);
  if (!SHA256.test(result)) throw new UniverseUpstreamError();
  return result;
};
const strings = (value: unknown) => {
  if (!Array.isArray(value)) throw new UniverseUpstreamError();
  return value.map(text);
};
const array = <T>(value: unknown, validate: Validator<T>) => {
  if (!Array.isArray(value)) throw new UniverseUpstreamError();
  return value.map(validate);
};
const pageArray = <T>(value: unknown, validate: Validator<T>) => {
  if (!Array.isArray(value) || value.length > 100)
    throw new UniverseUpstreamError();
  return value.map(validate);
};
const detailArray = <T>(value: unknown, validate: Validator<T>) => {
  if (!Array.isArray(value) || value.length > DETAIL_ROW_LIMIT)
    throw new UniverseUpstreamError();
  return value.map(validate);
};
const jsonValue = (value: unknown, depth = 0): unknown => {
  if (depth > 12) throw new UniverseUpstreamError();
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'boolean' ||
    (typeof value === 'number' && Number.isFinite(value))
  )
    return value;
  if (Array.isArray(value))
    return value.map((item) => jsonValue(item, depth + 1));
  if (value && typeof value === 'object')
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [
        key,
        jsonValue(item, depth + 1),
      ]),
    );
  throw new UniverseUpstreamError();
};
const jsonObject = (value: unknown) => {
  const result = jsonValue(value);
  if (!result || typeof result !== 'object' || Array.isArray(result))
    throw new UniverseUpstreamError();
  return result as Record<string, unknown>;
};
const textRecord = (value: unknown) => {
  const record = jsonObject(value);
  return Object.fromEntries(
    Object.entries(record).map(([key, item]) => [key, text(item)]),
  );
};
const numberRecord = (value: unknown) => {
  const record = jsonObject(value);
  return Object.fromEntries(
    Object.entries(record).map(([key, item]) => [key, number(item)]),
  );
};
const integerRecord = (value: unknown) => {
  const record = jsonObject(value);
  return Object.fromEntries(
    Object.entries(record).map(([key, item]) => [key, integer(item)]),
  );
};
const stringListRecord = (value: unknown) => {
  const record = jsonObject(value);
  return Object.fromEntries(
    Object.entries(record).map(([key, item]) => [key, strings(item)]),
  );
};
const partialObject = (value: unknown, keys: string[], label: string) => {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new UniverseUpstreamError(`${label} is invalid`);
  const record = value as Record<string, unknown>;
  if (Object.keys(record).some((key) => !keys.includes(key)))
    throw new UniverseUpstreamError(`${label} fields are invalid`);
  return record;
};
const optional = <T>(
  record: Record<string, unknown>,
  key: string,
  validate: Validator<T>,
) => (key in record ? validate(record[key]) : undefined);

function validateSource(
  value: unknown,
  withRun = false,
): UniverseSource & { run_id?: string } {
  const keys = [
    'manifest_key',
    'manifest_sha256',
    'report_key',
    'report_sha256',
  ];
  if (withRun) keys.unshift('run_id');
  const item = object(value, keys, 'source');
  return {
    ...(withRun ? { run_id: text(item.run_id) } : {}),
    manifest_key: text(item.manifest_key),
    manifest_sha256: sha(item.manifest_sha256),
    report_key: text(item.report_key),
    report_sha256: sha(item.report_sha256),
  };
}

function validateOrigin(value: unknown): UniverseOrigin {
  const item = object(
    value,
    [
      'run_id',
      'generated_at',
      'manifest_key',
      'manifest_sha256',
      'report_key',
      'report_sha256',
    ],
    'origin',
  );
  return {
    run_id: text(item.run_id),
    generated_at: timestamp(item.generated_at),
    manifest_key: text(item.manifest_key),
    manifest_sha256: sha(item.manifest_sha256),
    report_key: text(item.report_key),
    report_sha256: sha(item.report_sha256),
  };
}

function validateSelection(value: unknown): UniverseSelection {
  const item = object(
    value,
    [
      'run_id',
      'generated_at',
      'bundle_id',
      'occurrence_kind',
      'continuity_selected',
      'continuity_disposition',
      'sport',
      'game',
      'topology',
      'activation_at',
      'capture_start_at',
      'retirement',
      'source',
      'origin',
    ],
    'selection',
  );
  const occurrence = text(item.occurrence_kind);
  const disposition = item.continuity_disposition;
  if (!['complete', 'retained'].includes(occurrence))
    throw new UniverseUpstreamError();
  if (
    disposition !== null &&
    !['held_current_candidate', 'retained'].includes(String(disposition))
  )
    throw new UniverseUpstreamError();
  let retirement = null;
  if (item.retirement !== null) {
    const retired = object(
      item.retirement,
      ['retired_at', 'disposition', 'terminal_observed_at', 'source'],
      'retirement',
    );
    const retirementDisposition = text(retired.disposition);
    if (
      !['all_markets_terminal', 'terminal_clamp_elapsed'].includes(
        retirementDisposition,
      )
    )
      throw new UniverseUpstreamError();
    const retiredAt = timestamp(retired.retired_at);
    const observed =
      retired.terminal_observed_at === null
        ? null
        : timestamp(retired.terminal_observed_at);
    if (
      (retirementDisposition === 'all_markets_terminal' &&
        observed !== retiredAt) ||
      (retirementDisposition === 'terminal_clamp_elapsed' && observed !== null)
    )
      throw new UniverseUpstreamError();
    retirement = {
      retired_at: retiredAt,
      disposition: retirementDisposition as RetirementDisposition,
      terminal_observed_at: observed,
      source: validateSource(retired.source, true) as UniverseSource & {
        run_id: string;
      },
    };
  }
  const runId = text(item.run_id);
  const origin = validateOrigin(item.origin);
  const continuitySelected = boolean(item.continuity_selected);
  if (
    (occurrence === 'retained' &&
      (!continuitySelected ||
        disposition !== 'retained' ||
        origin.run_id === runId)) ||
    (occurrence === 'complete' &&
      (origin.run_id !== runId ||
        ![null, 'held_current_candidate'].includes(
          disposition as 'held_current_candidate' | null,
        ) ||
        continuitySelected !== (disposition === 'held_current_candidate')))
  )
    throw new UniverseUpstreamError();
  return {
    run_id: runId,
    generated_at: timestamp(item.generated_at),
    bundle_id: text(item.bundle_id),
    occurrence_kind: occurrence as UniverseSelection['occurrence_kind'],
    continuity_selected: continuitySelected,
    continuity_disposition:
      disposition as UniverseSelection['continuity_disposition'],
    sport: text(item.sport),
    game: nullableText(item.game),
    topology: nullableText(item.topology),
    activation_at: timestamp(item.activation_at),
    capture_start_at: timestamp(item.capture_start_at),
    retirement,
    source: validateSource(item.source),
    origin,
  };
}

function validateSelectionPage(value: unknown): UniverseSelectionPage {
  const item = object(
    value,
    ['selections', 'sort', 'next_cursor'],
    'selection page',
  );
  const sort = text(item.sort);
  if (!['activation', 'selected'].includes(sort))
    throw new UniverseUpstreamError();
  return {
    selections: pageArray(item.selections, validateSelection),
    sort: sort as UniverseSelectionPage['sort'],
    next_cursor: item.next_cursor === null ? null : text(item.next_cursor),
  };
}

function validateBundle(value: unknown): UniverseBundle {
  const item = object(
    value,
    [
      'bundle_id',
      'latest_run_id',
      'sport',
      'game',
      'topology',
      'participants',
      'activation_at',
      'capture_start_at',
      'first_selected_at',
      'last_selected_at',
      'occurrence_count',
      'venues',
      'target_count',
      'lifecycle',
    ],
    'bundle',
  );
  const lifecycle = text(item.lifecycle);
  if (!['active', 'retired'].includes(lifecycle))
    throw new UniverseUpstreamError();
  return {
    bundle_id: text(item.bundle_id),
    latest_run_id: text(item.latest_run_id),
    sport: text(item.sport),
    game: nullableText(item.game),
    topology: nullableText(item.topology),
    participants: strings(item.participants),
    activation_at: timestamp(item.activation_at),
    capture_start_at: timestamp(item.capture_start_at),
    first_selected_at: timestamp(item.first_selected_at),
    last_selected_at: timestamp(item.last_selected_at),
    occurrence_count: integer(item.occurrence_count),
    venues: strings(item.venues),
    target_count: integer(item.target_count),
    lifecycle: lifecycle as UniverseBundle['lifecycle'],
  };
}

function validateBundlePage(value: unknown): UniverseBundlePage {
  const item = object(value, ['bundles', 'next_cursor'], 'bundle page');
  return {
    bundles: pageArray(item.bundles, validateBundle),
    next_cursor: item.next_cursor === null ? null : text(item.next_cursor),
  };
}

function validateContext(value: unknown): UniverseContext {
  const item = object(
    value,
    [
      'bundle_id',
      'sport',
      'game',
      'topology',
      'participants',
      'participant_keys',
      'activation_at',
      'capture_start_at',
      'event_refs',
      'markets',
      'targets',
      'relationships',
    ],
    'context',
  );
  return {
    bundle_id: text(item.bundle_id),
    sport: text(item.sport),
    game: nullableText(item.game),
    topology: nullableText(item.topology),
    participants: strings(item.participants),
    participant_keys: strings(item.participant_keys),
    activation_at: timestamp(item.activation_at),
    capture_start_at: timestamp(item.capture_start_at),
    event_refs: strings(item.event_refs),
    markets: array(item.markets, (value) => {
      const market = object(
        value,
        ['target_id', 'venue', 'selected'],
        'market',
      );
      return {
        target_id: text(market.target_id),
        venue: text(market.venue),
        selected: boolean(market.selected),
      };
    }),
    targets: array(item.targets, (value) => {
      const target = object(
        value,
        [
          'target_id',
          'venue',
          'canonical_class',
          'source_ref',
          'subscription_ids',
        ],
        'target',
      );
      return {
        target_id: text(target.target_id),
        venue: text(target.venue),
        canonical_class: text(target.canonical_class),
        source_ref: text(target.source_ref),
        subscription_ids: strings(target.subscription_ids),
      };
    }),
    relationships: array(item.relationships, (value) => {
      const relationship = object(
        value,
        [
          'left',
          'right',
          'relationship',
          'scope',
          'left_venue',
          'right_venue',
          'coverage',
        ],
        'relationship',
      );
      return Object.fromEntries(
        Object.keys(relationship).map((key) => [key, text(relationship[key])]),
      ) as unknown as UniverseContext['relationships'][number];
    }),
  };
}

function validateSelectionDetail(value: unknown): UniverseSelectionDetail {
  const item = object(
    value,
    [
      'run_id',
      'generated_at',
      'bundle_id',
      'occurrence_kind',
      'continuity_selected',
      'continuity_disposition',
      'sport',
      'game',
      'topology',
      'activation_at',
      'capture_start_at',
      'retirement',
      'source',
      'origin',
      'context',
    ],
    'selection detail',
  );
  const summary = { ...item };
  delete summary.context;
  return {
    ...validateSelection(summary),
    context: validateContext(item.context),
  };
}

const RUN_KEYS = [
  'run_id',
  'generated_at',
  'generated_at_ns',
  'input_complete',
  'report_version',
  'strategy_version',
  'manifest_key',
  'manifest_sha256',
  'manifest_byte_length',
  'report_key',
  'report_sha256',
  'report_byte_length',
  'report_decoded_sha256',
  'report_decoded_byte_length',
  'projection_version',
  'projection_sha256',
  'projection_row_count',
  'indexed_at_ns',
];
function validateRun(value: unknown): UniverseRun {
  const item = object(value, RUN_KEYS, 'run');
  if (item.report_version !== 3 || item.projection_version !== 1)
    throw new UniverseUpstreamError();
  return {
    run_id: text(item.run_id),
    generated_at: timestamp(item.generated_at),
    generated_at_ns: integer(item.generated_at_ns),
    input_complete: boolean(item.input_complete),
    report_version: 3,
    strategy_version: integer(item.strategy_version),
    manifest_key: text(item.manifest_key),
    manifest_sha256: sha(item.manifest_sha256),
    manifest_byte_length: integer(item.manifest_byte_length),
    report_key: text(item.report_key),
    report_sha256: sha(item.report_sha256),
    report_byte_length: integer(item.report_byte_length),
    report_decoded_sha256: sha(item.report_decoded_sha256),
    report_decoded_byte_length: integer(item.report_decoded_byte_length),
    projection_version: 1,
    projection_sha256: sha(item.projection_sha256),
    projection_row_count: integer(item.projection_row_count),
    indexed_at_ns: integer(item.indexed_at_ns),
  };
}
function validateRunPage(value: unknown): UniverseRunPage {
  const item = object(value, ['runs', 'next_cursor'], 'run page');
  return {
    runs: pageArray(item.runs, validateRun),
    next_cursor: item.next_cursor === null ? null : text(item.next_cursor),
  };
}

const CONTINUITY_DISPOSITIONS = [
  'held_current_candidate',
  'retained',
  'continuity_budget_trimmed',
  'all_markets_terminal',
  'terminal_clamp_elapsed',
] as const;

function continuityDisposition(value: unknown) {
  const result = text(value);
  if (!CONTINUITY_DISPOSITIONS.includes(result as never))
    throw new UniverseUpstreamError();
  return result as UniverseCadenceRun['continuity']['dispositions'][string];
}

function validateCadenceRelationship(value: unknown) {
  const item = object(
    value,
    [
      'bundle_id',
      'left',
      'right',
      'relationship',
      'scope',
      'left_venue',
      'right_venue',
      'cross_venue',
      'coverage',
    ],
    'cadence relationship',
  );
  return {
    bundle_id: text(item.bundle_id),
    left: text(item.left),
    right: text(item.right),
    relationship: text(item.relationship),
    scope: text(item.scope),
    left_venue: text(item.left_venue),
    right_venue: text(item.right_venue),
    cross_venue: boolean(item.cross_venue),
    coverage: text(item.coverage),
  };
}

function validateCadenceCandidate(value: unknown) {
  const item = object(
    value,
    [
      'bundle_id',
      'sport',
      'game',
      'topology',
      'participants',
      'participant_keys',
      'event_refs',
      'activation_at',
      'capture_start_at',
      'score',
      'score_components',
      'eligible',
      'event_status',
      'rejection_reasons',
      'admission',
      'market_exclusions',
      'eligible_market_ids',
      'selected',
      'allocation_rejection',
      'relationship_analysis',
    ],
    'cadence candidate',
  );
  const eventStatus = text(item.event_status);
  if (!['ELIGIBLE', 'REJECTED'].includes(eventStatus))
    throw new UniverseUpstreamError();
  const admission = ((value: unknown) => {
    const record = object(
      value,
      [
        'combined_moneyline_volume_usd',
        'minimum_moneyline_volume_usd',
        'moneyline_volume_usd_by_venue',
        'moneyline_volume_usd_coverage',
      ],
      'cadence admission',
    );
    const venues = jsonObject(record.moneyline_volume_usd_coverage);
    const coverage = Object.fromEntries(
      Object.entries(venues).map(([venue, counts]) => {
        const values = object(
          counts,
          ['known_markets', 'unknown_markets'],
          'cadence volume coverage',
        );
        return [
          venue,
          {
            known_markets: integer(values.known_markets),
            unknown_markets: integer(values.unknown_markets),
          },
        ];
      }),
    );
    return {
      combined_moneyline_volume_usd: number(
        record.combined_moneyline_volume_usd,
      ),
      minimum_moneyline_volume_usd: number(record.minimum_moneyline_volume_usd),
      moneyline_volume_usd_by_venue: numberRecord(
        record.moneyline_volume_usd_by_venue,
      ),
      moneyline_volume_usd_coverage: coverage,
    };
  })(item.admission);
  const relationship = object(
    item.relationship_analysis,
    Object.keys(jsonObject(item.relationship_analysis)),
    'cadence relationship analysis',
  );
  if (
    Object.keys(relationship).some(
      (key) =>
        !['relationships', 'diagnostics', 'outcome_spaces'].includes(key),
    )
  )
    throw new UniverseUpstreamError();
  if (!('relationships' in relationship)) throw new UniverseUpstreamError();
  return {
    bundle_id: text(item.bundle_id),
    sport: text(item.sport),
    game: nullableText(item.game),
    topology: nullableText(item.topology),
    participants: strings(item.participants),
    participant_keys: strings(item.participant_keys),
    event_refs: strings(item.event_refs),
    activation_at: timestamp(item.activation_at),
    capture_start_at: timestamp(item.capture_start_at),
    score: number(item.score),
    score_components: numberRecord(item.score_components),
    eligible: boolean(item.eligible),
    event_status: eventStatus as 'ELIGIBLE' | 'REJECTED',
    rejection_reasons: strings(item.rejection_reasons),
    admission,
    market_exclusions: stringListRecord(item.market_exclusions),
    eligible_market_ids: strings(item.eligible_market_ids),
    selected: boolean(item.selected),
    allocation_rejection:
      item.allocation_rejection === null
        ? null
        : text(item.allocation_rejection),
    relationship_analysis: {
      relationships: array(
        relationship.relationships,
        validateCadenceRelationship,
      ),
      diagnostics: optional(relationship, 'diagnostics', strings),
      outcome_spaces: optional(relationship, 'outcome_spaces', (value) =>
        array(value, jsonObject),
      ),
    },
  };
}

function validateCadenceMatchRejection(value: unknown) {
  const item = partialObject(
    value,
    [
      'sport',
      'game',
      'topology',
      'participant_keys',
      'event_refs',
      'reason',
      'details',
    ],
    'cadence match rejection',
  );
  return {
    sport: optional(item, 'sport', text),
    game: optional(item, 'game', nullableText),
    topology: optional(item, 'topology', nullableText),
    participant_keys: optional(item, 'participant_keys', strings),
    event_refs: optional(item, 'event_refs', strings),
    reason: optional(item, 'reason', text),
    details: optional(item, 'details', jsonObject),
  };
}

function validateCadenceSelectedTarget(value: unknown) {
  const item = object(
    value,
    [
      'target_id',
      'bundle_id',
      'canonical_class',
      'subscription_ids',
      'activation_at',
      'capture_start_at',
      'source_ref',
      'continuity_score',
    ],
    'cadence selected target',
  );
  return {
    target_id: text(item.target_id),
    bundle_id: text(item.bundle_id),
    canonical_class: text(item.canonical_class),
    subscription_ids: strings(item.subscription_ids),
    activation_at: timestamp(item.activation_at),
    capture_start_at: timestamp(item.capture_start_at),
    source_ref: text(item.source_ref),
    continuity_score: number(item.continuity_score),
  };
}

function validateCadenceContinuityBundle(value: unknown) {
  const item = partialObject(
    value,
    [
      'base_run_id',
      'bundle_id',
      'activation_at',
      'score',
      'origin_run_id',
      'disposition',
      'targets',
    ],
    'cadence continuity bundle',
  );
  for (const key of [
    'base_run_id',
    'bundle_id',
    'activation_at',
    'score',
    'disposition',
    'targets',
  ])
    if (!(key in item)) throw new UniverseUpstreamError();
  const bundleId = text(item.bundle_id);
  const activationAt = timestamp(item.activation_at);
  const targets = array(item.targets, (value) => {
    const target = object(
      value,
      [
        'target_id',
        'venue',
        'canonical_class',
        'subscription_ids',
        'activation_at',
        'capture_start_at',
        'source_ref',
        'terminal_probe',
      ],
      'cadence continuity target',
    );
    const probe = object(
      target.terminal_probe,
      ['state', 'reason'],
      'cadence terminal probe',
    );
    const state = text(probe.state);
    if (!['open', 'terminal', 'unknown'].includes(state))
      throw new UniverseUpstreamError();
    const targetActivation = timestamp(target.activation_at);
    if (targetActivation !== activationAt) throw new UniverseUpstreamError();
    return {
      target_id: text(target.target_id),
      venue: text(target.venue),
      canonical_class: text(target.canonical_class),
      subscription_ids: strings(target.subscription_ids),
      activation_at: targetActivation,
      capture_start_at: timestamp(target.capture_start_at),
      source_ref: text(target.source_ref),
      terminal_probe: {
        state: state as 'open' | 'terminal' | 'unknown',
        reason: text(probe.reason),
      },
    };
  });
  if (
    targets.length === 0 ||
    new Set(targets.map((target) => target.target_id)).size !== targets.length
  )
    throw new UniverseUpstreamError();
  return {
    base_run_id: text(item.base_run_id),
    bundle_id: bundleId,
    activation_at: activationAt,
    score: number(item.score),
    origin_run_id: optional(item, 'origin_run_id', text),
    disposition: continuityDisposition(item.disposition),
    targets,
  };
}

export function validateCadence(value: unknown): UniverseCadence {
  const item = object(
    value,
    ['cadence_projection_version', 'observed_at', 'freshness', 'runs'],
    'cadence projection',
  );
  if (item.cadence_projection_version !== 1) throw new UniverseUpstreamError();
  const freshness = object(
    item.freshness,
    [
      'state',
      'expected_run_seconds',
      'latest_run_age_seconds',
      'latest_indexed_at',
    ],
    'cadence freshness',
  );
  const state = text(freshness.state);
  if (!['current', 'late', 'unavailable'].includes(state))
    throw new UniverseUpstreamError();
  const expectedRunSeconds = integer(freshness.expected_run_seconds);
  if (expectedRunSeconds === 0) throw new UniverseUpstreamError();
  const latestRunAgeSeconds =
    freshness.latest_run_age_seconds === null
      ? null
      : integer(freshness.latest_run_age_seconds);
  const latestIndexedAt =
    freshness.latest_indexed_at === null
      ? null
      : timestamp(freshness.latest_indexed_at);
  const runs = array(item.runs, (value): UniverseCadenceRun => {
    const operationalKeys = [
      'catalogs',
      'discovery_failures',
      'counts',
      'reason_summaries',
      'match_rejections',
      'candidates',
      'selected_targets',
      'budget_used',
      'continuity',
      'diagnostics',
    ];
    const run = object(
      value,
      [...RUN_KEYS, 'selections', ...operationalKeys],
      'cadence run',
    );
    const { selections, ...allRunFields } = run;
    const runFields = Object.fromEntries(
      Object.entries(allRunFields).filter(([key]) => RUN_KEYS.includes(key)),
    );
    const validatedRun = validateRun(runFields);
    const validatedSelections = array(selections, validateSelectionDetail);
    if (
      validatedSelections.some(
        (selection) => selection.run_id !== validatedRun.run_id,
      )
    )
      throw new UniverseUpstreamError();
    const counts = object(
      run.counts,
      ['candidates', 'eligible', 'selected', 'rejected', 'retained', 'retired'],
      'cadence counts',
    );
    const catalogs = array(run.catalogs, (value) => {
      const catalog = object(
        value,
        [
          'venue',
          'complete',
          'events',
          'markets',
          'requests',
          'diagnostics',
          'classification_diagnostic_count',
          'classification_diagnostics_by_code',
        ],
        'cadence catalog',
      );
      return {
        venue: text(catalog.venue),
        complete: boolean(catalog.complete),
        events: integer(catalog.events),
        markets: integer(catalog.markets),
        requests: integer(catalog.requests),
        diagnostics: strings(catalog.diagnostics),
        classification_diagnostic_count: integer(
          catalog.classification_diagnostic_count,
        ),
        classification_diagnostics_by_code: integerRecord(
          catalog.classification_diagnostics_by_code,
        ),
      };
    });
    const reasonSummaries = object(
      run.reason_summaries,
      [
        'candidate_rejections',
        'allocation_rejections',
        'continuity_dispositions',
      ],
      'cadence reason summaries',
    );
    const candidates = array(run.candidates, validateCadenceCandidate);
    const selectedTargets = Object.fromEntries(
      Object.entries(jsonObject(run.selected_targets)).map(([key, value]) => [
        key,
        array(value, validateCadenceSelectedTarget),
      ]),
    );
    const continuity = object(
      run.continuity,
      ['bundles', 'retained_bundle_ids', 'dispositions'],
      'cadence continuity',
    );
    const continuityBundles = array(
      continuity.bundles,
      validateCadenceContinuityBundle,
    );
    const retainedBundleIds = strings(continuity.retained_bundle_ids);
    const dispositions = Object.fromEntries(
      Object.entries(jsonObject(continuity.dispositions)).map(
        ([key, value]) => [key, continuityDisposition(value)],
      ),
    );
    const diagnostics = object(
      run.diagnostics,
      ['continuity', 'continuity_degraded_base_run_id', 'target_records'],
      'cadence diagnostics',
    );
    const validatedCounts = Object.fromEntries(
      Object.entries(counts).map(([key, value]) => [key, integer(value)]),
    ) as UniverseCadenceRun['counts'];
    if (
      new Set(catalogs.map((catalog) => catalog.venue)).size !==
        catalogs.length ||
      new Set(candidates.map((candidate) => candidate.bundle_id)).size !==
        candidates.length ||
      new Set(retainedBundleIds).size !== retainedBundleIds.length ||
      new Set(continuityBundles.map((bundle) => bundle.bundle_id)).size !==
        continuityBundles.length ||
      continuityBundles.some(
        (bundle) => dispositions[bundle.bundle_id] !== bundle.disposition,
      ) ||
      Object.entries(selectedTargets).some(([, targets]) =>
        targets.some(
          (target) =>
            !validatedSelections.some(
              (selection) => selection.bundle_id === target.bundle_id,
            ),
        ),
      ) ||
      validatedCounts.candidates !== candidates.length ||
      validatedCounts.eligible !==
        candidates.filter((candidate) => candidate.eligible).length ||
      validatedCounts.rejected !==
        candidates.filter((candidate) => !candidate.eligible).length ||
      validatedCounts.selected !== validatedSelections.length ||
      validatedCounts.retained !== retainedBundleIds.length ||
      validatedCounts.retained !==
        validatedSelections.filter(
          (selection) => selection.occurrence_kind === 'retained',
        ).length ||
      validatedCounts.retired !==
        Object.values(dispositions).filter((value) =>
          ['all_markets_terminal', 'terminal_clamp_elapsed'].includes(value),
        ).length
    )
      throw new UniverseUpstreamError();
    return {
      ...validatedRun,
      catalogs,
      discovery_failures: textRecord(run.discovery_failures),
      counts: validatedCounts,
      reason_summaries: {
        candidate_rejections: integerRecord(
          reasonSummaries.candidate_rejections,
        ),
        allocation_rejections: integerRecord(
          reasonSummaries.allocation_rejections,
        ),
        continuity_dispositions: integerRecord(
          reasonSummaries.continuity_dispositions,
        ),
      },
      match_rejections: array(
        run.match_rejections,
        validateCadenceMatchRejection,
      ),
      candidates,
      selected_targets: selectedTargets,
      budget_used: numberRecord(run.budget_used),
      continuity: {
        bundles: continuityBundles,
        retained_bundle_ids: retainedBundleIds,
        dispositions,
      },
      diagnostics: {
        continuity: strings(diagnostics.continuity),
        continuity_degraded_base_run_id:
          diagnostics.continuity_degraded_base_run_id === null
            ? null
            : text(diagnostics.continuity_degraded_base_run_id),
        target_records: stringListRecord(diagnostics.target_records),
      },
      selections: validatedSelections,
    };
  });
  if (
    runs.length > 5 ||
    runs.some(
      (run, index) =>
        index > 0 &&
        Date.parse(runs[index - 1].generated_at) < Date.parse(run.generated_at),
    ) ||
    new Set(runs.map((run) => run.run_id)).size !== runs.length
  )
    throw new UniverseUpstreamError();
  if (
    (state === 'unavailable' &&
      (runs.length !== 0 ||
        latestRunAgeSeconds !== null ||
        latestIndexedAt !== null)) ||
    (state !== 'unavailable' &&
      (runs.length === 0 ||
        latestRunAgeSeconds === null ||
        latestIndexedAt === null))
  )
    throw new UniverseUpstreamError();
  return {
    cadence_projection_version: 1,
    observed_at: timestamp(item.observed_at),
    freshness: {
      state: state as UniverseCadence['freshness']['state'],
      expected_run_seconds: expectedRunSeconds,
      latest_run_age_seconds: latestRunAgeSeconds,
      latest_indexed_at: latestIndexedAt,
    },
    runs,
  };
}

export function validateTargeterRun(value: unknown): UniverseTargeterRunDetail {
  const item = object(
    value,
    [
      'run',
      'source',
      'counts',
      'decisions',
      'events',
      'selected_markets',
      'relations',
    ],
    'targeter run detail',
  );
  const source = object(
    item.source,
    ['manifest_key', 'manifest_sha256', 'report_key', 'report_sha256'],
    'targeter run source',
  );
  const counts = object(
    item.counts,
    [
      'candidates',
      'eligible',
      'selected_events',
      'selected_markets',
      'relations',
    ],
    'targeter run counts',
  );
  const decisions = detailArray(item.decisions, validateTargeterDecision);
  const events = detailArray(item.events, validateEvent);
  const selectedMarkets = detailArray(
    item.selected_markets,
    validateSelectedMarket,
  );
  const relations = detailArray(item.relations, (relation) =>
    validateRelationSummary(relation, true),
  );
  const eventIds = new Set(events.map((event) => event.event_id));
  const validatedCounts = {
    candidates: integer(counts.candidates),
    eligible: integer(counts.eligible),
    selected_events: integer(counts.selected_events),
    selected_markets: integer(counts.selected_markets),
    relations: integer(counts.relations),
  };
  if (
    validatedCounts.candidates !== decisions.length ||
    validatedCounts.eligible !==
      decisions.filter((decision) => decision.eligible).length ||
    validatedCounts.selected_markets !== selectedMarkets.length ||
    validatedCounts.selected_events !==
      new Set(selectedMarkets.map((market) => market.event_id)).size ||
    validatedCounts.relations !== relations.length ||
    eventIds.size !== events.length ||
    [...decisions, ...selectedMarkets, ...relations].some(
      (record) =>
        record.event_id === undefined || !eventIds.has(record.event_id),
    )
  )
    throw new UniverseUpstreamError();
  return {
    run: validateRunSummary(item.run),
    source: {
      manifest_key: text(source.manifest_key),
      manifest_sha256: sha(source.manifest_sha256),
      report_key: text(source.report_key),
      report_sha256: sha(source.report_sha256),
    },
    counts: validatedCounts,
    decisions,
    events,
    selected_markets: selectedMarkets,
    relations,
  };
}

function validateTargeterDecision(value: unknown): UniverseTargeterDecision {
  const item = object(
    value,
    [
      'event_id',
      'bundle_id',
      'eligible',
      'selected',
      'score',
      'score_components',
      'rejection_reasons',
      'allocation_rejection',
      'admission',
      'market_exclusions',
      'eligible_market_ids',
    ],
    'targeter decision',
  );
  const admission = object(
    item.admission,
    [
      'combined_moneyline_volume_usd',
      'minimum_moneyline_volume_usd',
      'moneyline_volume_usd_by_venue',
      'moneyline_volume_usd_coverage',
    ],
    'targeter admission',
  );
  const coverage = Object.fromEntries(
    Object.entries(jsonObject(admission.moneyline_volume_usd_coverage)).map(
      ([venue, value]) => {
        const record = object(
          value,
          ['known_markets', 'unknown_markets'],
          'moneyline volume coverage',
        );
        return [
          venue,
          {
            known_markets: integer(record.known_markets),
            unknown_markets: integer(record.unknown_markets),
          },
        ];
      },
    ),
  );
  return {
    event_id: text(item.event_id),
    bundle_id: text(item.bundle_id),
    eligible: boolean(item.eligible),
    selected: boolean(item.selected),
    score: number(item.score),
    score_components: numberRecord(item.score_components),
    rejection_reasons: strings(item.rejection_reasons),
    allocation_rejection:
      item.allocation_rejection === null
        ? null
        : text(item.allocation_rejection),
    admission: {
      combined_moneyline_volume_usd: number(
        admission.combined_moneyline_volume_usd,
      ),
      minimum_moneyline_volume_usd: number(
        admission.minimum_moneyline_volume_usd,
      ),
      moneyline_volume_usd_by_venue: numberRecord(
        admission.moneyline_volume_usd_by_venue,
      ),
      moneyline_volume_usd_coverage: coverage,
    },
    market_exclusions: stringListRecord(item.market_exclusions),
    eligible_market_ids: strings(item.eligible_market_ids),
  };
}

function validateSelectedMarket(value: unknown): UniverseSelectedMarket {
  const item = object(
    value,
    [
      'event_id',
      'bundle_id',
      'venue',
      'venue_market_id',
      'market_id',
      'market_template_version',
      'outcome_space_version',
      'canonical_class',
      'continuity_score',
      'selection_reason',
      'origin_run_id',
    ],
    'selected market',
  );
  const selectionReason = text(item.selection_reason);
  if (
    !['selected', 'held_current_candidate', 'retained'].includes(
      selectionReason,
    )
  )
    throw new UniverseUpstreamError();
  return {
    event_id: text(item.event_id),
    bundle_id: text(item.bundle_id),
    venue: text(item.venue),
    venue_market_id: text(item.venue_market_id),
    market_id: text(item.market_id),
    market_template_version: positiveInteger(item.market_template_version),
    outcome_space_version: positiveInteger(item.outcome_space_version),
    canonical_class: text(item.canonical_class),
    continuity_score: number(item.continuity_score),
    selection_reason:
      selectionReason as UniverseSelectedMarket['selection_reason'],
    origin_run_id: text(item.origin_run_id),
  };
}

function validateRelationSummary(
  value: unknown,
  includeEvent: boolean,
): UniverseRelationSummary {
  const keys = [
    'relation_id',
    'relation_type',
    ...(includeEvent ? ['event_id'] : []),
    'scope',
    'coverage',
    'generation_version',
    'canonical_hash',
  ];
  const item = object(value, keys, 'relation summary');
  return {
    relation_id: positiveInteger(item.relation_id),
    relation_type: text(item.relation_type),
    ...(includeEvent ? { event_id: text(item.event_id) } : {}),
    scope: text(item.scope),
    coverage: text(item.coverage),
    generation_version: positiveInteger(item.generation_version),
    canonical_hash: sha(item.canonical_hash),
  };
}

function validateEventPage(value: unknown): UniverseEventPage {
  const item = object(value, ['events', 'next_cursor'], 'event page');
  const events = pageArray(item.events, (value) => {
    const record = object(
      value,
      [
        'event_id',
        'sport',
        'game',
        'topology',
        'activation_at',
        'participants',
        'participant_keys',
        'first_seen_run_id',
        'last_seen_run_id',
        'venue_count',
        'market_count',
        'selected_run_count',
      ],
      'event summary',
    );
    const event = validateEvent(
      Object.fromEntries(
        Object.entries(record).filter(
          ([key]) =>
            !['venue_count', 'market_count', 'selected_run_count'].includes(
              key,
            ),
        ),
      ),
    );
    return {
      ...event,
      venue_count: integer(record.venue_count),
      market_count: integer(record.market_count),
      selected_run_count: integer(record.selected_run_count),
    };
  });
  if (new Set(events.map((event) => event.event_id)).size !== events.length)
    throw new UniverseUpstreamError();
  return {
    events,
    next_cursor: item.next_cursor === null ? null : text(item.next_cursor),
  };
}

function validateCanonicalMarket(value: unknown) {
  const record = object(
    value,
    [
      'market_id',
      'market_template_version',
      'outcome_space_version',
      'event_id',
      'canonical_class',
      'market_type',
      'scope',
      'parameters',
      'first_seen_run_id',
      'last_seen_run_id',
      'venue_market_count',
      'venues',
    ],
    'canonical market',
  );
  return {
    market_id: text(record.market_id),
    market_template_version: positiveInteger(record.market_template_version),
    outcome_space_version: positiveInteger(record.outcome_space_version),
    event_id: text(record.event_id),
    canonical_class: text(record.canonical_class),
    market_type: text(record.market_type),
    scope: text(record.scope),
    parameters: jsonObject(record.parameters),
    first_seen_run_id: text(record.first_seen_run_id),
    last_seen_run_id: text(record.last_seen_run_id),
    venue_market_count: integer(record.venue_market_count),
    venues: strings(record.venues),
  };
}

function validateMarketDetail(value: unknown): UniverseMarketDetail {
  const item = object(
    value,
    ['market', 'venue_markets', 'selections', 'relations'],
    'market detail',
  );
  const market = validateCanonicalMarket(item.market);
  const venueMarkets = detailArray(item.venue_markets, (value) => {
    const record = object(
      value,
      [
        'venue',
        'venue_market_id',
        'venue_event_id',
        'event_id',
        'market_id',
        'market_template_version',
        'outcome_space_version',
        'canonical_class',
        'market_type',
        'scope',
        'title',
        'parameters',
        'subscription_ids',
        'outcome_labels',
        'status',
        'accepting_orders',
        'rules_hash',
        'rule_template_id',
        'source_ref',
        'created_at',
        'volume_24h',
        'volume_total',
        'volume_total_usd',
        'liquidity',
        'first_seen_run_id',
        'last_seen_run_id',
      ],
      'venue market',
    );
    return {
      venue: text(record.venue),
      venue_market_id: text(record.venue_market_id),
      venue_event_id: text(record.venue_event_id),
      event_id: text(record.event_id),
      market_id: text(record.market_id),
      market_template_version: positiveInteger(record.market_template_version),
      outcome_space_version: positiveInteger(record.outcome_space_version),
      canonical_class: text(record.canonical_class),
      market_type: text(record.market_type),
      scope: text(record.scope),
      title: text(record.title),
      parameters: jsonObject(record.parameters),
      subscription_ids: strings(record.subscription_ids),
      outcome_labels: strings(record.outcome_labels),
      status: text(record.status),
      accepting_orders: boolean(record.accepting_orders),
      rules_hash: nullableText(record.rules_hash),
      rule_template_id: nullableText(record.rule_template_id),
      source_ref: text(record.source_ref),
      created_at:
        record.created_at === null ? null : timestamp(record.created_at),
      volume_24h: nullableNumber(record.volume_24h),
      volume_total: nullableNumber(record.volume_total),
      volume_total_usd: nullableNumber(record.volume_total_usd),
      liquidity: nullableNumber(record.liquidity),
      first_seen_run_id: text(record.first_seen_run_id),
      last_seen_run_id: text(record.last_seen_run_id),
    };
  });
  const selections = detailArray(item.selections, (value) => {
    const record = object(
      value,
      [
        'run_id',
        'generated_at',
        'bundle_id',
        'venue',
        'venue_market_id',
        'continuity_score',
        'selection_reason',
        'origin_run_id',
      ],
      'market selection',
    );
    const selectionReason = text(record.selection_reason);
    if (
      !['selected', 'held_current_candidate', 'retained'].includes(
        selectionReason,
      )
    )
      throw new UniverseUpstreamError();
    return {
      run_id: text(record.run_id),
      generated_at: timestamp(record.generated_at),
      bundle_id: text(record.bundle_id),
      venue: text(record.venue),
      venue_market_id: text(record.venue_market_id),
      continuity_score: number(record.continuity_score),
      selection_reason:
        selectionReason as UniverseSelectedMarket['selection_reason'],
      origin_run_id: text(record.origin_run_id),
    };
  });
  return {
    market,
    venue_markets: venueMarkets,
    selections,
    relations: detailArray(item.relations, (relation) =>
      validateRelationSummary(relation, false),
    ),
  };
}

function validateRelationDetail(value: unknown): UniverseRelationDetail {
  const item = object(
    value,
    ['relation', 'members', 'observations'],
    'relation detail',
  );
  const relation = object(
    item.relation,
    ['relation_id', 'relation_type', 'generation_version', 'canonical_hash'],
    'relation',
  );
  return {
    relation: {
      relation_id: positiveInteger(relation.relation_id),
      relation_type: text(relation.relation_type),
      generation_version: positiveInteger(relation.generation_version),
      canonical_hash: sha(relation.canonical_hash),
    },
    members: detailArray(item.members, (value) => {
      const record = object(
        value,
        [
          'venue',
          'venue_market_id',
          'market_id',
          'market_template_version',
          'outcome_space_version',
          'claim_key',
          'role',
        ],
        'relation member',
      );
      return {
        venue: text(record.venue),
        venue_market_id: text(record.venue_market_id),
        market_id: text(record.market_id),
        market_template_version: positiveInteger(
          record.market_template_version,
        ),
        outcome_space_version: positiveInteger(record.outcome_space_version),
        claim_key: possiblyEmptyText(record.claim_key),
        role: text(record.role),
      };
    }),
    observations: detailArray(item.observations, (value) => {
      const record = object(
        value,
        [
          'run_id',
          'generated_at',
          'bundle_id',
          'event_id',
          'scope',
          'coverage',
        ],
        'relation observation',
      );
      return {
        run_id: text(record.run_id),
        generated_at: timestamp(record.generated_at),
        bundle_id: text(record.bundle_id),
        event_id: text(record.event_id),
        scope: text(record.scope),
        coverage: text(record.coverage),
      };
    }),
  };
}

function validateRelationshipTypes(
  value: unknown,
): UniverseRelationshipTypeCatalog {
  const item = object(
    value,
    ['relationship_type_catalog_version', 'types'],
    'relationship type catalog',
  );
  if (item.relationship_type_catalog_version !== 1)
    throw new UniverseUpstreamError();
  const expected = new Map<string, { directed: boolean; roles: string[] }>([
    ['IDENTITY', { directed: false, roles: ['member'] }],
    ['IMPLICATION', { directed: true, roles: ['left', 'right'] }],
    ['REVERSE_IMPLICATION', { directed: true, roles: ['left', 'right'] }],
    ['MUTUAL_EXCLUSION', { directed: false, roles: ['member'] }],
    ['OVERLAP', { directed: false, roles: ['member'] }],
  ]);
  const types = array(item.types, (value) => {
    const record = object(
      value,
      ['type', 'directed', 'member_roles'],
      'relationship type',
    );
    return {
      type: text(record.type),
      directed: boolean(record.directed),
      member_roles: strings(record.member_roles),
    };
  });
  if (
    types.length !== expected.size ||
    new Set(types.map((type) => type.type)).size !== types.length ||
    types.some((type) => {
      const contract = expected.get(type.type);
      return (
        !contract ||
        type.directed !== contract.directed ||
        type.member_roles.join('\0') !== contract.roles.join('\0')
      );
    })
  )
    throw new UniverseUpstreamError();
  return { relationship_type_catalog_version: 1, types };
}

export function validateEventDetail(value: unknown): UniverseEventDetail {
  const item = object(
    value,
    ['event', 'venue_events', 'markets', 'relations', 'observations'],
    'event detail',
  );
  const event = validateEvent(item.event);
  const venueEvents = detailArray(item.venue_events, (value) => {
    const record = object(
      value,
      [
        'venue',
        'venue_event_id',
        'title',
        'league',
        'status',
        'source_ref',
        'format',
        'fragment_type',
        'first_seen_run_id',
        'last_seen_run_id',
      ],
      'venue event',
    );
    return {
      venue: text(record.venue),
      venue_event_id: text(record.venue_event_id),
      title: text(record.title),
      league: record.league === null ? null : text(record.league),
      status: text(record.status),
      source_ref: text(record.source_ref),
      format: record.format === null ? null : text(record.format),
      fragment_type:
        record.fragment_type === null ? null : text(record.fragment_type),
      first_seen_run_id: text(record.first_seen_run_id),
      last_seen_run_id: text(record.last_seen_run_id),
    };
  });
  const markets = detailArray(item.markets, validateCanonicalMarket);
  return {
    event,
    venue_events: venueEvents,
    markets,
    relations: detailArray(item.relations, (relation) =>
      validateRelationSummary(relation, false),
    ),
    observations: detailArray(item.observations, (value) => {
      const record = object(
        value,
        ['run_id', 'generated_at', 'bundle_id'],
        'event observation',
      );
      return {
        run_id: text(record.run_id),
        generated_at: timestamp(record.generated_at),
        bundle_id: text(record.bundle_id),
      };
    }),
  };
}

function validateEvent(value: unknown) {
  const item = object(
    value,
    [
      'event_id',
      'sport',
      'game',
      'topology',
      'activation_at',
      'participants',
      'participant_keys',
      'first_seen_run_id',
      'last_seen_run_id',
    ],
    'event',
  );
  return {
    event_id: text(item.event_id),
    sport: text(item.sport),
    game: item.game === null ? null : text(item.game),
    topology: item.topology === null ? null : text(item.topology),
    activation_at: timestamp(item.activation_at),
    participants: strings(item.participants),
    participant_keys: strings(item.participant_keys),
    first_seen_run_id: text(item.first_seen_run_id),
    last_seen_run_id: text(item.last_seen_run_id),
  };
}

function validateRunSummary(value: unknown): UniverseTargeterRunSummary {
  const item = object(
    value,
    ['run_id', 'generated_at', 'input_complete', 'indexed_at'],
    'targeter run summary',
  );
  return {
    run_id: text(item.run_id),
    generated_at: timestamp(item.generated_at),
    input_complete: boolean(item.input_complete),
    indexed_at: timestamp(item.indexed_at),
  };
}

export function validateTargeterStatus(value: unknown): UniverseTargeterStatus {
  const item = object(
    value,
    [
      'status_projection_version',
      'observed_at',
      'freshness',
      'latest_run',
      'current_complete_run',
      'current_complete_summary',
    ],
    'targeter status',
  );
  if (item.status_projection_version !== 1) throw new UniverseUpstreamError();
  const freshness = object(
    item.freshness,
    [
      'state',
      'expected_run_seconds',
      'latest_run_age_seconds',
      'latest_indexed_at',
    ],
    'targeter status freshness',
  );
  const state = text(freshness.state);
  if (!['current', 'late', 'unavailable'].includes(state))
    throw new UniverseUpstreamError();
  const expectedRunSeconds = integer(freshness.expected_run_seconds);
  const latestRunAgeSeconds =
    freshness.latest_run_age_seconds === null
      ? null
      : integer(freshness.latest_run_age_seconds);
  const latestIndexedAt =
    freshness.latest_indexed_at === null
      ? null
      : timestamp(freshness.latest_indexed_at);
  const summary = object(
    item.current_complete_summary,
    ['selected_bundles', 'selected_targets', 'venues'],
    'current complete summary',
  );
  const latestRun =
    item.latest_run === null ? null : validateRunSummary(item.latest_run);
  const currentCompleteRun =
    item.current_complete_run === null
      ? null
      : validateRunSummary(item.current_complete_run);
  if (
    (state === 'unavailable' &&
      (latestRun !== null ||
        latestRunAgeSeconds !== null ||
        latestIndexedAt !== null)) ||
    (state !== 'unavailable' &&
      (latestRun === null ||
        latestRunAgeSeconds === null ||
        latestIndexedAt === null)) ||
    (currentCompleteRun === null &&
      (summary.selected_bundles !== 0 || summary.selected_targets !== 0)) ||
    (currentCompleteRun !== null && !currentCompleteRun.input_complete)
  )
    throw new UniverseUpstreamError();
  return {
    status_projection_version: 1,
    observed_at: timestamp(item.observed_at),
    freshness: {
      state: state as UniverseTargeterStatus['freshness']['state'],
      expected_run_seconds: expectedRunSeconds,
      latest_run_age_seconds: latestRunAgeSeconds,
      latest_indexed_at: latestIndexedAt,
    },
    latest_run: latestRun,
    current_complete_run: currentCompleteRun,
    current_complete_summary: {
      selected_bundles: integer(summary.selected_bundles),
      selected_targets: integer(summary.selected_targets),
      venues: strings(summary.venues),
    },
  };
}

function validateAudit(value: unknown): UniverseAudit {
  const keys = [
    'run_id',
    'ok',
    'projection_version',
    'stored_sha256',
    'actual_sha256',
    'stored_row_count',
    'actual_row_count',
    'selection_row_count',
    'retirement_row_count',
    'contexts_ok',
  ];
  const item = object(value, keys, 'audit');
  return {
    run_id: text(item.run_id),
    ok: boolean(item.ok),
    projection_version: integer(item.projection_version),
    stored_sha256: sha(item.stored_sha256),
    actual_sha256: sha(item.actual_sha256),
    stored_row_count: integer(item.stored_row_count),
    actual_row_count: integer(item.actual_row_count),
    selection_row_count: integer(item.selection_row_count),
    retirement_row_count: integer(item.retirement_row_count),
    contexts_ok: boolean(item.contexts_ok),
  };
}
function validateRunDetail(value: unknown): UniverseRunDetail {
  const item = object(value, [...RUN_KEYS, 'audit'], 'run detail');
  const { audit, ...run } = item;
  return { ...validateRun(run), audit: validateAudit(audit) };
}
function validateHealth(value: unknown): UniverseHealth {
  const item = object(
    value,
    ['status', 'schema_version', 'latest_run', 'counts'],
    'health',
  );
  if (item.status !== 'ok' || item.schema_version !== 3)
    throw new UniverseUpstreamError();
  const counts = object(
    item.counts,
    [
      'targeter_runs',
      'selection_occurrences',
      'bundle_retirements',
      'bundle_contexts',
      'context_targets',
      'umbrella_events',
      'canonical_markets',
      'venue_markets',
      'relations',
    ],
    'counts',
  );
  let latest = null;
  if (item.latest_run !== null) {
    const run = object(
      item.latest_run,
      [
        'run_id',
        'generated_at',
        'indexed_at_ns',
        'input_complete',
        'age_seconds',
        'stale_after_seconds',
        'stale',
      ],
      'latest run',
    );
    latest = {
      run_id: text(run.run_id),
      generated_at: timestamp(run.generated_at),
      indexed_at_ns: integer(run.indexed_at_ns),
      input_complete: boolean(run.input_complete),
      age_seconds: integer(run.age_seconds),
      stale_after_seconds: integer(run.stale_after_seconds),
      stale: boolean(run.stale),
    };
  }
  return {
    status: 'ok',
    schema_version: 3,
    latest_run: latest,
    counts: {
      targeter_runs: integer(counts.targeter_runs),
      selection_occurrences: integer(counts.selection_occurrences),
      bundle_retirements: integer(counts.bundle_retirements),
      bundle_contexts: integer(counts.bundle_contexts),
      context_targets: integer(counts.context_targets),
      umbrella_events: integer(counts.umbrella_events),
      canonical_markets: integer(counts.canonical_markets),
      venue_markets: integer(counts.venue_markets),
      relations: integer(counts.relations),
    },
  };
}

function positive(value: number, label: string) {
  if (!Number.isInteger(value) || value <= 0)
    throw new Error(`${label} must be a positive integer`);
  return value;
}
