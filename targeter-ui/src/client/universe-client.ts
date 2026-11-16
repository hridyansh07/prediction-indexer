import type {
  UniverseBundle,
  UniverseSelectionDetail,
} from '../event-universe';

const RESPONSE_LIMIT = 1_750_000;
const DEFAULT_TIMEOUT_MS = 10_000;
export const BUNDLE_PAGE_SIZE = 5;
const PUBLIC_REQUEST_INTERVAL_MS = 3_500;
const MAX_BUNDLE_PAGES = 256;

type Codec<T> = (value: unknown) => T;
type JsonObject = Record<string, unknown>;

export class UniverseApiError extends Error {
  constructor(
    message: string,
    readonly status: number | null,
    readonly retryAfterSeconds: number | null = null,
    readonly kind: 'http' | 'timeout' | 'network' | 'contract' = 'http',
  ) {
    super(message);
    this.name = 'UniverseApiError';
  }
}

function object(
  value: unknown,
  fields: readonly string[],
  where: string,
): JsonObject {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw contract(`${where} must be an object`);
  const record = value as JsonObject;
  const actual = Object.keys(record).sort();
  const expected = [...fields].sort();
  if (
    actual.length !== expected.length ||
    actual.some((key, index) => key !== expected[index])
  )
    throw contract(`${where} has unexpected fields`);
  return record;
}

function contract(message: string): UniverseApiError {
  return new UniverseApiError(message, null, null, 'contract');
}

function string(value: unknown, where: string): string {
  if (typeof value !== 'string') throw contract(`${where} must be a string`);
  return value;
}

function nullableString(value: unknown, where: string): string | null {
  return value === null ? null : string(value, where);
}

function integer(value: unknown, where: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0)
    throw contract(`${where} must be a nonnegative integer`);
  return value as number;
}

function boolean(value: unknown, where: string): boolean {
  if (typeof value !== 'boolean') throw contract(`${where} must be boolean`);
  return value;
}

function array<T>(value: unknown, codec: Codec<T>, where: string): T[] {
  if (!Array.isArray(value)) throw contract(`${where} must be an array`);
  return value.map((item) => codec(item));
}

function enumValue<T extends string>(
  value: unknown,
  allowed: Set<string>,
  where: string,
): T {
  const result = string(value, where);
  if (!allowed.has(result)) throw contract(`${where} is invalid`);
  return result as T;
}

export function validateErrorResponse(value: unknown): { error: string } {
  const result = object(value, ['error'], 'error response');
  return { error: string(result.error, 'error') };
}

export function validateBundlePage(value: unknown): {
  bundles: UniverseBundle[];
  next_cursor: string | null;
} {
  const page = object(value, ['bundles', 'next_cursor'], 'bundle page');
  const bundles = array(
    page.bundles,
    (value) => {
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
      return {
        bundle_id: string(item.bundle_id, 'bundle_id'),
        latest_run_id: string(item.latest_run_id, 'latest_run_id'),
        sport: string(item.sport, 'sport'),
        game: nullableString(item.game, 'game'),
        topology: nullableString(item.topology, 'topology'),
        participants: array(
          item.participants,
          (name) => string(name, 'participant'),
          'participants',
        ),
        activation_at: string(item.activation_at, 'activation_at'),
        capture_start_at: string(item.capture_start_at, 'capture_start_at'),
        first_selected_at: string(item.first_selected_at, 'first_selected_at'),
        last_selected_at: string(item.last_selected_at, 'last_selected_at'),
        occurrence_count: integer(item.occurrence_count, 'occurrence_count'),
        venues: array(item.venues, (venue) => string(venue, 'venue'), 'venues'),
        target_count: integer(item.target_count, 'target_count'),
        lifecycle: enumValue<'active' | 'retired'>(
          item.lifecycle,
          new Set(['active', 'retired']),
          'lifecycle',
        ),
      };
    },
    'bundles',
  );
  return {
    bundles,
    next_cursor: nullableString(page.next_cursor, 'next_cursor'),
  };
}

// The client needs only the closed context from selection detail. Validate every
// field and retain the typed server shape without interpreting compatibility.
export function validateSelectionDetail(
  value: unknown,
): UniverseSelectionDetail {
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
  const context = object(
    item.context,
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
    'selection context',
  );
  const source = object(
    item.source,
    ['manifest_key', 'manifest_sha256', 'report_key', 'report_sha256'],
    'selection source',
  );
  for (const field of Object.keys(source))
    string(source[field], `source.${field}`);
  const origin = object(
    item.origin,
    [
      'manifest_key',
      'manifest_sha256',
      'report_key',
      'report_sha256',
      'run_id',
      'generated_at',
    ],
    'selection origin',
  );
  for (const field of Object.keys(origin))
    string(origin[field], `origin.${field}`);
  if (item.retirement !== null) {
    const retirement = object(
      item.retirement,
      ['retired_at', 'disposition', 'terminal_observed_at', 'source'],
      'retirement',
    );
    string(retirement.retired_at, 'retired_at');
    enumValue(
      retirement.disposition,
      new Set(['all_markets_terminal', 'terminal_clamp_elapsed']),
      'retirement.disposition',
    );
    nullableString(retirement.terminal_observed_at, 'terminal_observed_at');
    const retirementSource = object(
      retirement.source,
      [
        'manifest_key',
        'manifest_sha256',
        'report_key',
        'report_sha256',
        'run_id',
      ],
      'retirement source',
    );
    for (const field of Object.keys(retirementSource))
      string(retirementSource[field], `retirement.source.${field}`);
  }
  string(item.run_id, 'run_id');
  string(item.generated_at, 'generated_at');
  string(item.bundle_id, 'bundle_id');
  enumValue(
    item.occurrence_kind,
    new Set(['complete', 'retained']),
    'occurrence_kind',
  );
  boolean(item.continuity_selected, 'continuity_selected');
  if (item.continuity_disposition !== null)
    enumValue(
      item.continuity_disposition,
      new Set(['held_current_candidate', 'retained']),
      'continuity_disposition',
    );
  string(item.sport, 'sport');
  nullableString(item.game, 'game');
  nullableString(item.topology, 'topology');
  string(item.activation_at, 'activation_at');
  string(item.capture_start_at, 'capture_start_at');
  array(
    context.markets,
    (value) => {
      const market = object(
        value,
        ['target_id', 'venue', 'selected'],
        'context market',
      );
      string(market.target_id, 'target_id');
      string(market.venue, 'venue');
      boolean(market.selected, 'selected');
      return market;
    },
    'context.markets',
  );
  array(
    context.targets,
    (value) => {
      const target = object(
        value,
        [
          'target_id',
          'venue',
          'canonical_class',
          'source_ref',
          'subscription_ids',
        ],
        'context target',
      );
      string(target.target_id, 'target_id');
      string(target.venue, 'venue');
      string(target.canonical_class, 'canonical_class');
      string(target.source_ref, 'source_ref');
      array(
        target.subscription_ids,
        (id) => string(id, 'subscription id'),
        'subscription_ids',
      );
      return target;
    },
    'context.targets',
  );
  array(
    context.relationships,
    (value) => {
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
      for (const field of Object.keys(relationship))
        string(relationship[field], `relationship.${field}`);
      return relationship;
    },
    'relationships',
  );
  string(context.bundle_id, 'context.bundle_id');
  string(context.sport, 'context.sport');
  nullableString(context.game, 'context.game');
  nullableString(context.topology, 'context.topology');
  for (const field of [
    'participants',
    'participant_keys',
    'event_refs',
  ] as const)
    array(context[field], (entry) => string(entry, field), field);
  string(context.activation_at, 'context.activation_at');
  string(context.capture_start_at, 'context.capture_start_at');
  return value as UniverseSelectionDetail;
}

export type UniverseClientOptions = {
  baseUrl: string;
  timeoutMs?: number;
  fetch?: typeof fetch;
  requestIntervalMs?: number;
};

class RequestCoordinator {
  private tail = Promise.resolve();
  private nextStart = 0;
  private blockedUntil = 0;

  constructor(private readonly intervalMs: number) {}

  get enabled() {
    return this.intervalMs > 0;
  }

  async acquire(signal?: AbortSignal) {
    let releaseTurn!: () => void;
    const turn = new Promise<void>((resolve) => {
      releaseTurn = resolve;
    });
    const prior = this.tail;
    this.tail = prior.catch(() => undefined).then(() => turn);
    try {
      await abortable(prior, signal);
      await abortableDelay(
        Math.max(0, Math.max(this.nextStart, this.blockedUntil) - Date.now()),
        signal,
      );
    } catch (error) {
      releaseTurn();
      throw error;
    }
    return () => {
      this.nextStart = Date.now() + this.intervalMs;
      releaseTurn();
    };
  }

  defer(milliseconds: number) {
    this.blockedUntil = Math.max(
      this.blockedUntil,
      Date.now() + Math.max(this.intervalMs, milliseconds),
    );
  }
}

const requestCoordinators = new Map<string, RequestCoordinator>();

function requestCoordinator(baseUrl: URL, intervalMs: number) {
  const key = `${baseUrl.origin}|${intervalMs}`;
  let coordinator = requestCoordinators.get(key);
  if (!coordinator) {
    coordinator = new RequestCoordinator(intervalMs);
    requestCoordinators.set(key, coordinator);
  }
  return coordinator;
}

export class UniverseClient {
  private readonly baseUrl: URL;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;
  private readonly coordinator: RequestCoordinator;

  constructor(private readonly options: UniverseClientOptions) {
    this.baseUrl = new URL(options.baseUrl);
    if (
      this.baseUrl.protocol !== 'https:' &&
      this.baseUrl.hostname !== 'localhost'
    )
      throw new Error('Universe API base must use HTTPS');
    if (
      this.baseUrl.search ||
      this.baseUrl.hash ||
      this.baseUrl.username ||
      this.baseUrl.password
    )
      throw new Error(
        'Universe API base must not contain credentials, query, or fragment',
      );
    this.baseUrl.pathname = `${this.baseUrl.pathname.replace(/\/+$/, '')}/`;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.fetchImpl = options.fetch ?? ((...arguments_) => fetch(...arguments_));
    this.coordinator = requestCoordinator(
      this.baseUrl,
      options.requestIntervalMs ??
        (options.fetch === undefined ? PUBLIC_REQUEST_INTERVAL_MS : 0),
    );
  }

  get<T>(path: string, codec: Codec<T>, signal?: AbortSignal) {
    const relative = path.replace(/^\//, '');
    return this.request(relative, codec, { signal });
  }
  async bundles(
    signal?: AbortSignal,
    options: {
      cursor?: string | null;
      paceMs?: number;
      onPage?: (bundles: UniverseBundle[]) => void;
    } = {},
  ) {
    const bundles: UniverseBundle[] = [];
    let cursor = options.cursor ?? null;
    let pageSize = BUNDLE_PAGE_SIZE;
    const paceMs = options.paceMs ?? PUBLIC_REQUEST_INTERVAL_MS;
    const cursors = new Set<string>();
    for (let pageNumber = 0; pageNumber < MAX_BUNDLE_PAGES; ) {
      if (pageNumber > 0 || pageSize < BUNDLE_PAGE_SIZE)
        await abortableDelay(paceMs, signal);
      const query = new URLSearchParams({ limit: String(pageSize) });
      if (cursor) query.set('cursor', cursor);
      let page: ReturnType<typeof validateBundlePage>;
      try {
        page = await this.request(`v1/bundles?${query}`, validateBundlePage, {
          signal,
        });
      } catch (error) {
        if (
          error instanceof UniverseApiError &&
          error.status === 413 &&
          pageSize > 1
        ) {
          pageSize = Math.max(1, Math.floor(pageSize / 2));
          continue;
        }
        if (
          error instanceof UniverseApiError &&
          error.status === 413 &&
          pageSize === 1
        ) {
          const message =
            'An older historical bundle exceeds the Universe bundle-context limit even at one bundle per page.';
          if (bundles.length)
            return {
              bundles,
              next_cursor: cursor,
              warning: `${message} Showing the bundles discovered before it.`,
              retry_after_seconds: null,
            };
          throw new UniverseApiError(message, 413);
        }
        if (
          error instanceof UniverseApiError &&
          error.status === 429 &&
          bundles.length
        ) {
          return {
            bundles,
            next_cursor: cursor,
            warning:
              'Universe rate limit paused bundle discovery. The bundles already loaded are preserved; resume after the requested delay.',
            retry_after_seconds: error.retryAfterSeconds,
          };
        }
        throw error;
      }
      bundles.push(...page.bundles);
      options.onPage?.(page.bundles);
      pageNumber += 1;
      cursor = page.next_cursor;
      if (!cursor)
        return {
          bundles,
          next_cursor: null,
          warning: null,
          retry_after_seconds: null,
        };
      if (cursors.has(cursor)) throw contract('bundle cursor repeated');
      cursors.add(cursor);
    }
    return {
      bundles,
      next_cursor: cursor,
      warning:
        'Historical bundle discovery reached its bounded page limit. The bundles already loaded are preserved; resume from this point.',
      retry_after_seconds: null,
    };
  }
  selection(runId: string, bundleId: string, signal?: AbortSignal) {
    return this.request(
      `v1/runs/${encodeURIComponent(runId)}/selections/${encodeURIComponent(bundleId)}`,
      validateSelectionDetail,
      { signal },
    );
  }
  private async request<T>(
    path: string,
    codec: Codec<T>,
    options: {
      signal?: AbortSignal;
    } = {},
  ): Promise<T> {
    const release = this.coordinator.enabled
      ? await this.coordinator.acquire(options.signal)
      : () => undefined;
    const controller = new AbortController();
    let timedOut = false;
    const timeout = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, this.timeoutMs);
    const abort = () => controller.abort();
    options.signal?.addEventListener('abort', abort, { once: true });
    try {
      const response = await this.fetchImpl(new URL(path, this.baseUrl), {
        method: 'GET',
        credentials: 'omit',
        redirect: 'error',
        signal: controller.signal,
        headers: { accept: 'application/json' },
      });
      const stated = Number(response.headers.get('content-length'));
      if (Number.isFinite(stated) && stated > RESPONSE_LIMIT)
        throw contract('response exceeds the client size limit');
      const text = await response.text();
      if (new TextEncoder().encode(text).byteLength > RESPONSE_LIMIT)
        throw contract('response exceeds the client size limit');
      let document: unknown;
      try {
        document = JSON.parse(text);
      } catch {
        throw contract('response is not JSON');
      }
      if (!response.ok) {
        const error = validateErrorResponse(document);
        const retryAfter = parseRetryAfter(response.headers.get('retry-after'));
        if (response.status === 429)
          this.coordinator.defer((retryAfter ?? 0) * 1_000);
        throw new UniverseApiError(error.error, response.status, retryAfter);
      }
      return codec(document);
    } catch (error) {
      if (error instanceof UniverseApiError) throw error;
      if (options.signal?.aborted)
        throw new DOMException('Aborted', 'AbortError');
      if (timedOut)
        throw new UniverseApiError(
          'The Universe request timed out.',
          null,
          null,
          'timeout',
        );
      if (error instanceof Error && error.name === 'AbortError') throw error;
      throw new UniverseApiError(
        'The Universe API is unavailable.',
        null,
        null,
        'network',
      );
    } finally {
      clearTimeout(timeout);
      options.signal?.removeEventListener('abort', abort);
      release();
    }
  }
}

function abortable<T>(promise: Promise<T>, signal?: AbortSignal) {
  if (!signal) return promise;
  if (signal.aborted)
    return Promise.reject(new DOMException('Aborted', 'AbortError'));
  return new Promise<T>((resolve, reject) => {
    const abort = () => reject(new DOMException('Aborted', 'AbortError'));
    signal.addEventListener('abort', abort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener('abort', abort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener('abort', abort);
        reject(error);
      },
    );
  });
}

function abortableDelay(milliseconds: number, signal?: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException('Aborted', 'AbortError'));
      return;
    }
    const abort = () => {
      clearTimeout(timer);
      reject(new DOMException('Aborted', 'AbortError'));
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', abort);
      resolve();
    }, milliseconds);
    signal?.addEventListener('abort', abort, { once: true });
  });
}

export function parseRetryAfter(
  value: string | null,
  now = Date.now(),
): number | null {
  if (value === null) return null;
  if (/^\d+$/.test(value)) return Number(value);
  const date = Date.parse(value);
  return Number.isNaN(date)
    ? null
    : Math.max(0, Math.ceil((date - now) / 1000));
}
