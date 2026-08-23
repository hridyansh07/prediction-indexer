import {
  Router,
  type Request,
  type Response as ExpressResponse,
} from 'express';
import type {
  RetirementDisposition,
  UniverseAudit,
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
      options.maxResponseBytes ?? 2 * 1024 * 1024,
      'response limit',
    );
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
  const proxy =
    <T>(load: (client: EventUniverseClient, request: Request) => Promise<T>) =>
    async (request: Request, response: ExpressResponse) => {
      if (!client) return response.status(503).json(SAFE_ERROR);
      try {
        return response.json(await load(client, request));
      } catch (error) {
        const failure = universePublicFailure(error);
        return response.status(failure.status).json(failure.body);
      }
    };
  router.get(
    '/healthz',
    proxy((active, request) => {
      requireNoQuery(request);
      return active.health();
    }),
  );
  router.get(
    '/v1/runs',
    proxy((active, request) => active.runs(requestQuery(request))),
  );
  router.get(
    '/v1/selections',
    proxy((active, request) => active.selections(requestQuery(request))),
  );
  router.get(
    '/v1/runs/:runId',
    proxy((active, request) => {
      requireNoQuery(request);
      return active.run(pathValue(request, 'runId'));
    }),
  );
  router.get(
    '/v1/runs/:runId/selections',
    proxy((active, request) =>
      active.runSelections(pathValue(request, 'runId'), requestQuery(request)),
    ),
  );
  router.get(
    '/v1/runs/:runId/selections/:bundleId',
    proxy((active, request) => {
      requireNoQuery(request);
      return active.selection(
        pathValue(request, 'runId'),
        pathValue(request, 'bundleId'),
      );
    }),
  );
  router.get(
    '/v1/bundles/:bundleId/history',
    proxy((active, request) =>
      active.bundleHistory(
        pathValue(request, 'bundleId'),
        requestQuery(request),
      ),
    ),
  );
  router.use((_request, response) =>
    response.status(404).json({ error: 'Event Universe route not found' }),
  );
  return router;
}

export function universePublicFailure(error: unknown) {
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

function requireNoQuery(request: Request) {
  if ([...requestQuery(request)].length) throw new UniverseRequestError();
}

function pathValue(request: Request, field: string) {
  const value = request.params[field];
  if (typeof value !== 'string') throw new UniverseRequestError();
  return value;
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
      (!/^\d+$/.test(value) || Number(value) < 1 || Number(value) > 1000)
    )
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
    selections: array(item.selections, validateSelection),
    sort: sort as UniverseSelectionPage['sort'],
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
    runs: array(item.runs, validateRun),
    next_cursor: item.next_cursor === null ? null : text(item.next_cursor),
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
  if (item.status !== 'ok') throw new UniverseUpstreamError();
  const counts = object(
    item.counts,
    [
      'targeter_runs',
      'selection_occurrences',
      'bundle_retirements',
      'bundle_contexts',
      'context_targets',
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
    schema_version: integer(item.schema_version),
    latest_run: latest,
    counts: {
      targeter_runs: integer(counts.targeter_runs),
      selection_occurrences: integer(counts.selection_occurrences),
      bundle_retirements: integer(counts.bundle_retirements),
      bundle_contexts: integer(counts.bundle_contexts),
      context_targets: integer(counts.context_targets),
    },
  };
}

function positive(value: number, label: string) {
  if (!Number.isInteger(value) || value <= 0)
    throw new Error(`${label} must be a positive integer`);
  return value;
}
