import type {
  UniverseBundle,
  UniverseSelectionDetail,
} from '../event-universe';
import {
  replayStatuses,
  type ReplayStage,
  type ReplayStatus,
} from './replay-model';

const RESPONSE_LIMIT = 1_750_000;
const DEFAULT_TIMEOUT_MS = 10_000;
export const BUNDLE_PAGE_SIZE = 5;
const MAX_BUNDLE_PAGES = 128;
const MAX_BUNDLE_RATE_RETRIES = 3;
const JOB_ID = /^\d{8}T\d{6}Z-[0-9a-f]{16}$/;
const ADDRESS = /^0x[0-9a-fA-F]{40}$/;
const HEX_32 = /^[0-9a-f]{32}$/;
const DECIMAL = /^(0|[1-9][0-9]*)$/;
const replayStatusSet = new Set<string>(replayStatuses);
const stages = new Set<string>([
  'resolve',
  'bundle',
  'prepare',
  'run',
  'read',
  'archive',
]);

type Codec<T> = (value: unknown) => T;
type JsonObject = Record<string, unknown>;

export type ReplayRole = 'member' | 'admin';
export type ReplayStrategy = {
  name: string;
  label: string;
  description: string;
  status: 'active' | 'retired';
  config_keys: string[];
};
export type StrategyCatalogue = {
  version: 1;
  strategies: ReplayStrategy[];
  limits: string[];
};
export type ReplayRequest = {
  replay_request_version: 1;
  bundle_id: string;
  probe_markets: string[] | null;
  interval: null;
  strategy: { name: string; config: Record<string, unknown> };
  limits: string;
};
export type ReplayJobRecord = {
  job_id: string;
  created_at_ns: string;
  submitted_by: string;
  request_sha256: string;
  status: ReplayStatus;
  stage: ReplayStage | null;
  stage_attempts: number;
  next_attempt_at_ns: string | null;
  started_at_ns: string | null;
  updated_at_ns: string;
  finished_at_ns: string | null;
  pending_outcome: ReplayStatus | null;
  reason_code: string | null;
  reason_detail: string | null;
  blocked_reason_code: string | null;
  blocked_at_ns: string | null;
  archive_receipt_key: string | null;
};
export type ReplayJobDetail = ReplayJobRecord & { request: ReplayRequest };
export type ReplayJobEventRecord = {
  event_id: string;
  job_id: string;
  event_type: string;
  created_at_ns: string;
  status: ReplayStatus;
  stage: ReplayStage | null;
  stage_attempts: number;
  pending_outcome: ReplayStatus | null;
  reason_code: string | null;
};
export type AllowlistMember = { address: string; note: string };
export type ReplaySession = {
  token: string;
  address: string;
  role: ReplayRole;
  expires_at: string;
};

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

function ns(value: unknown, where: string): string {
  const result = string(value, where);
  if (!DECIMAL.test(result))
    throw contract(`${where} must be decimal nanoseconds`);
  return result;
}

function nullableNs(value: unknown, where: string): string | null {
  return value === null ? null : ns(value, where);
}

export function validateErrorResponse(value: unknown): { error: string } {
  const result = object(value, ['error'], 'error response');
  return { error: string(result.error, 'error') };
}

export function validateStrategyCatalogue(value: unknown): StrategyCatalogue {
  const result = object(
    value,
    ['version', 'strategies', 'limits'],
    'strategy catalogue',
  );
  if (result.version !== 1)
    throw contract('strategy catalogue version must be 1');
  const strategies = array(
    result.strategies,
    (entry) => {
      const item = object(
        entry,
        ['name', 'label', 'description', 'status', 'config_keys'],
        'strategy',
      );
      const status = enumValue<'active' | 'retired'>(
        item.status,
        new Set(['active', 'retired']),
        'strategy.status',
      );
      return {
        name: string(item.name, 'strategy.name'),
        label: string(item.label, 'strategy.label'),
        description: string(item.description, 'strategy.description'),
        status,
        config_keys: array(
          item.config_keys,
          (key) => string(key, 'config key'),
          'config_keys',
        ),
      };
    },
    'strategies',
  );
  return {
    version: 1,
    strategies,
    limits: array(result.limits, (limit) => string(limit, 'limit'), 'limits'),
  };
}

const jobFields = [
  'job_id',
  'created_at_ns',
  'submitted_by',
  'request_sha256',
  'status',
  'stage',
  'stage_attempts',
  'next_attempt_at_ns',
  'started_at_ns',
  'updated_at_ns',
  'finished_at_ns',
  'pending_outcome',
  'reason_code',
  'reason_detail',
  'blocked_reason_code',
  'blocked_at_ns',
  'archive_receipt_key',
] as const;

export function validateJob(value: unknown): ReplayJobRecord {
  const item = object(value, jobFields, 'replay job');
  const jobId = string(item.job_id, 'job_id');
  if (!JOB_ID.test(jobId)) throw contract('job_id is invalid');
  const submittedBy = string(item.submitted_by, 'submitted_by');
  if (!ADDRESS.test(submittedBy)) throw contract('submitted_by is invalid');
  const requestSha = string(item.request_sha256, 'request_sha256');
  if (!/^[0-9a-f]{64}$/.test(requestSha))
    throw contract('request_sha256 is invalid');
  return {
    job_id: jobId,
    created_at_ns: ns(item.created_at_ns, 'created_at_ns'),
    submitted_by: submittedBy,
    request_sha256: requestSha,
    status: enumValue<ReplayStatus>(item.status, replayStatusSet, 'status'),
    stage:
      item.stage === null
        ? null
        : enumValue<ReplayStage>(item.stage, stages, 'stage'),
    stage_attempts: integer(item.stage_attempts, 'stage_attempts'),
    next_attempt_at_ns: nullableNs(
      item.next_attempt_at_ns,
      'next_attempt_at_ns',
    ),
    started_at_ns: nullableNs(item.started_at_ns, 'started_at_ns'),
    updated_at_ns: ns(item.updated_at_ns, 'updated_at_ns'),
    finished_at_ns: nullableNs(item.finished_at_ns, 'finished_at_ns'),
    pending_outcome:
      item.pending_outcome === null
        ? null
        : enumValue<ReplayStatus>(
            item.pending_outcome,
            replayStatusSet,
            'pending_outcome',
          ),
    reason_code: nullableString(item.reason_code, 'reason_code'),
    reason_detail: nullableString(item.reason_detail, 'reason_detail'),
    blocked_reason_code: nullableString(
      item.blocked_reason_code,
      'blocked_reason_code',
    ),
    blocked_at_ns: nullableNs(item.blocked_at_ns, 'blocked_at_ns'),
    archive_receipt_key: nullableString(
      item.archive_receipt_key,
      'archive_receipt_key',
    ),
  };
}

export function validateReplayRequest(value: unknown): ReplayRequest {
  const request = object(
    value,
    [
      'replay_request_version',
      'bundle_id',
      'probe_markets',
      'interval',
      'strategy',
      'limits',
    ],
    'replay request',
  );
  if (request.replay_request_version !== 1 || request.interval !== null)
    throw contract('replay request version or interval is invalid');
  const strategy = object(
    request.strategy,
    ['name', 'config'],
    'strategy request',
  );
  const config = object(
    strategy.config,
    Object.keys(strategy.config as object),
    'strategy config',
  );
  return {
    replay_request_version: 1,
    bundle_id: string(request.bundle_id, 'bundle_id'),
    probe_markets:
      request.probe_markets === null
        ? null
        : array(
            request.probe_markets,
            (market) => string(market, 'probe market'),
            'probe_markets',
          ),
    interval: null,
    strategy: { name: string(strategy.name, 'strategy.name'), config },
    limits: string(request.limits, 'limits'),
  };
}

export function validateJobDetail(value: unknown): ReplayJobDetail {
  const item = object(value, [...jobFields, 'request'], 'replay job detail');
  const record = validateJob(
    Object.fromEntries(jobFields.map((field) => [field, item[field]])),
  );
  return { ...record, request: validateReplayRequest(item.request) };
}

export function validateJobPage(value: unknown) {
  const page = object(value, ['jobs', 'next_cursor'], 'replay job page');
  return {
    jobs: array(page.jobs, validateJob, 'jobs'),
    next_cursor: nullableString(page.next_cursor, 'next_cursor'),
  };
}

export function validateJobEventPage(value: unknown) {
  const page = object(value, ['events', 'next_cursor'], 'job event page');
  const events = array(
    page.events,
    (value) => {
      const event = object(
        value,
        [
          'event_id',
          'job_id',
          'event_type',
          'created_at_ns',
          'status',
          'stage',
          'stage_attempts',
          'pending_outcome',
          'reason_code',
        ],
        'job event',
      );
      return {
        event_id: ns(event.event_id, 'event_id'),
        job_id: string(event.job_id, 'job_id'),
        event_type: string(event.event_type, 'event_type'),
        created_at_ns: ns(event.created_at_ns, 'created_at_ns'),
        status: enumValue<ReplayStatus>(
          event.status,
          replayStatusSet,
          'status',
        ),
        stage:
          event.stage === null
            ? null
            : enumValue<ReplayStage>(event.stage, stages, 'stage'),
        stage_attempts: integer(event.stage_attempts, 'stage_attempts'),
        pending_outcome:
          event.pending_outcome === null
            ? null
            : enumValue<ReplayStatus>(
                event.pending_outcome,
                replayStatusSet,
                'pending_outcome',
              ),
        reason_code: nullableString(event.reason_code, 'reason_code'),
      } satisfies ReplayJobEventRecord;
    },
    'events',
  );
  return {
    events,
    next_cursor: nullableString(page.next_cursor, 'next_cursor'),
  };
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

// Replay needs only the closed context from selection detail. Validate every
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

export function validateNonce(value: unknown) {
  const item = object(value, ['nonce', 'expires_at'], 'nonce');
  const nonce = string(item.nonce, 'nonce');
  if (!HEX_32.test(nonce)) throw contract('nonce is invalid');
  return { nonce, expires_at: string(item.expires_at, 'expires_at') };
}

export function validateSession(value: unknown): ReplaySession {
  const item = object(
    value,
    ['token', 'address', 'role', 'expires_at'],
    'session',
  );
  const address = string(item.address, 'address');
  if (!ADDRESS.test(address)) throw contract('session address is invalid');
  return {
    token: string(item.token, 'token'),
    address,
    role: enumValue<ReplayRole>(
      item.role,
      new Set(['member', 'admin']),
      'role',
    ),
    expires_at: string(item.expires_at, 'expires_at'),
  };
}

export function validateAllowlist(value: unknown) {
  const item = object(value, ['members'], 'allowlist');
  return { members: array(item.members, validateMember, 'members') };
}

function validateMember(value: unknown): AllowlistMember {
  const item = object(value, ['address', 'note'], 'allowlist member');
  const address = string(item.address, 'address');
  if (!ADDRESS.test(address)) throw contract('allowlist address is invalid');
  return { address, note: string(item.note, 'note') };
}

function validateSubmission(value: unknown) {
  const item = object(value, ['job_id', 'status', 'replayed'], 'submission');
  return {
    job_id: string(item.job_id, 'job_id'),
    status: enumValue<ReplayStatus>(item.status, replayStatusSet, 'status'),
    replayed: boolean(item.replayed, 'replayed'),
  };
}

function validateOk(value: unknown) {
  const item = object(value, ['ok'], 'logout response');
  if (item.ok !== true) throw contract('logout response is invalid');
  return { ok: true as const };
}

function validateRemoved(value: unknown) {
  const item = object(value, ['address'], 'remove response');
  return { address: string(item.address, 'address') };
}

export type UniverseClientOptions = {
  baseUrl: string;
  timeoutMs?: number;
  fetch?: typeof fetch;
  getToken?: () => string | null;
  onUnauthorized?: () => void;
};

export class UniverseClient {
  private readonly baseUrl: URL;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;

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
  }

  strategies(signal?: AbortSignal) {
    return this.request('v1/replay/strategies', validateStrategyCatalogue, {
      signal,
    });
  }
  get<T>(path: string, codec: Codec<T>, signal?: AbortSignal) {
    const relative = path.replace(/^\//, '');
    return this.request(relative, codec, { signal });
  }
  jobs(
    status?: ReplayStatus,
    cursor?: string | null,
    signal?: AbortSignal,
    limit = 25,
  ) {
    const query = new URLSearchParams({ limit: String(limit) });
    if (status) query.set('status', status);
    if (cursor) query.set('cursor', cursor);
    return this.request(`v1/replay/jobs?${query}`, validateJobPage, { signal });
  }
  async jobPages(
    status: ReplayStatus | undefined,
    pageCount: number,
    signal?: AbortSignal,
  ) {
    const jobs: ReplayJobRecord[] = [];
    const seen = new Set<string>();
    let cursor: string | null = null;
    for (let pageNumber = 0; pageNumber < pageCount; pageNumber += 1) {
      const page = await this.jobs(status, cursor, signal);
      for (const job of page.jobs) {
        if (seen.has(job.job_id)) continue;
        seen.add(job.job_id);
        jobs.push(job);
      }
      cursor = page.next_cursor;
      if (!cursor) break;
    }
    return { jobs, next_cursor: cursor };
  }
  job(jobId: string, signal?: AbortSignal) {
    return this.request(
      `v1/replay/jobs/${encodeURIComponent(jobId)}`,
      validateJobDetail,
      { signal },
    );
  }
  jobEvents(
    jobId: string,
    cursor?: string | null,
    signal?: AbortSignal,
    limit = 50,
  ) {
    const query = new URLSearchParams({ limit: String(limit) });
    if (cursor) query.set('cursor', cursor);
    return this.request(
      `v1/replay/jobs/${encodeURIComponent(jobId)}/events?${query}`,
      validateJobEventPage,
      { signal },
    );
  }
  async bundles(signal?: AbortSignal) {
    const bundles: UniverseBundle[] = [];
    let cursor: string | null = null;
    let pageSize = BUNDLE_PAGE_SIZE;
    let rateRetries = 0;
    const cursors = new Set<string>();
    for (let pageNumber = 0; pageNumber < MAX_BUNDLE_PAGES; ) {
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
            };
          throw new UniverseApiError(message, 413);
        }
        if (
          error instanceof UniverseApiError &&
          error.status === 429 &&
          error.retryAfterSeconds !== null &&
          rateRetries < MAX_BUNDLE_RATE_RETRIES
        ) {
          rateRetries += 1;
          await abortableDelay(error.retryAfterSeconds * 1_000, signal);
          continue;
        }
        throw error;
      }
      rateRetries = 0;
      bundles.push(...page.bundles);
      pageNumber += 1;
      cursor = page.next_cursor;
      if (!cursor) return { bundles, next_cursor: null, warning: null };
      if (cursors.has(cursor)) throw contract('bundle cursor repeated');
      cursors.add(cursor);
    }
    throw new UniverseApiError(
      'Historical bundle discovery exceeded the bounded page limit.',
      null,
      null,
      'contract',
    );
  }
  selection(runId: string, bundleId: string, signal?: AbortSignal) {
    return this.request(
      `v1/runs/${encodeURIComponent(runId)}/selections/${encodeURIComponent(bundleId)}`,
      validateSelectionDetail,
      { signal },
    );
  }
  nonce(signal?: AbortSignal) {
    return this.request('v1/auth/nonce', validateNonce, { signal });
  }
  signIn(message: string, signature: string, signal?: AbortSignal) {
    return this.request('v1/auth/siwe', validateSession, {
      method: 'POST',
      body: { message, signature },
      signal,
    });
  }
  logout(signal?: AbortSignal) {
    return this.request('v1/auth/logout', validateOk, {
      method: 'POST',
      body: {},
      signal,
      authenticated: true,
    });
  }
  allowlist(signal?: AbortSignal) {
    return this.request('v1/admin/allowlist', validateAllowlist, {
      signal,
      authenticated: true,
    });
  }
  addMember(address: string, note: string, signal?: AbortSignal) {
    return this.request('v1/admin/allowlist', validateMember, {
      method: 'POST',
      body: { address, note },
      signal,
      authenticated: true,
    });
  }
  removeMember(address: string, signal?: AbortSignal) {
    return this.request(
      `v1/admin/allowlist/${encodeURIComponent(address)}`,
      validateRemoved,
      { method: 'DELETE', body: {}, signal, authenticated: true },
    );
  }
  submit(request: ReplayRequest, idempotencyKey: string, signal?: AbortSignal) {
    return this.request('v1/replay/jobs', validateSubmission, {
      method: 'POST',
      body: request,
      signal,
      authenticated: true,
      idempotencyKey,
    });
  }
  cancel(jobId: string, signal?: AbortSignal) {
    return this.request(
      `v1/replay/jobs/${encodeURIComponent(jobId)}/cancel`,
      validateJob,
      { method: 'POST', body: {}, signal, authenticated: true },
    );
  }

  async pollJob(
    jobId: string,
    onUpdate: (job: ReplayJobDetail) => void,
    options: {
      signal?: AbortSignal;
      attempts?: number;
      intervalMs?: number;
    } = {},
  ) {
    const attempts = options.attempts ?? 120;
    const intervalMs = options.intervalMs ?? 5_000;
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const job = await this.job(jobId, options.signal);
      onUpdate(job);
      if (!['queued', 'running', 'archiving'].includes(job.status)) return job;
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(resolve, intervalMs);
        options.signal?.addEventListener(
          'abort',
          () => {
            clearTimeout(timer);
            reject(new DOMException('Aborted', 'AbortError'));
          },
          { once: true },
        );
      });
    }
    throw new UniverseApiError(
      'Polling stopped after the bounded attempt limit.',
      null,
      null,
      'timeout',
    );
  }

  private async request<T>(
    path: string,
    codec: Codec<T>,
    options: {
      method?: 'GET' | 'POST' | 'DELETE';
      body?: unknown;
      signal?: AbortSignal;
      authenticated?: boolean;
      idempotencyKey?: string;
    } = {},
  ): Promise<T> {
    const controller = new AbortController();
    let timedOut = false;
    const timeout = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, this.timeoutMs);
    const abort = () => controller.abort();
    options.signal?.addEventListener('abort', abort, { once: true });
    try {
      const token = options.authenticated ? this.options.getToken?.() : null;
      if (options.authenticated && !token)
        throw new UniverseApiError('Sign in is required.', 401);
      const response = await this.fetchImpl(new URL(path, this.baseUrl), {
        method: options.method ?? 'GET',
        credentials: 'omit',
        redirect: 'error',
        signal: controller.signal,
        headers: {
          accept: 'application/json',
          ...(options.body === undefined
            ? {}
            : { 'content-type': 'application/json' }),
          ...(token ? { authorization: `Bearer ${token}` } : {}),
          ...(options.idempotencyKey
            ? { 'idempotency-key': options.idempotencyKey }
            : {}),
        },
        body:
          options.body === undefined ? undefined : JSON.stringify(options.body),
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
        if (response.status === 401 && options.authenticated)
          this.options.onUnauthorized?.();
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
    }
  }
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

export function startSerializedPolling(
  task: (signal: AbortSignal) => Promise<void>,
  intervalMs: number,
) {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | null = null;
  const run = async () => {
    if (controller.signal.aborted) return;
    try {
      await task(controller.signal);
    } catch {
      // Polling tasks own their visible error state. Keep the scheduler alive.
    } finally {
      if (!controller.signal.aborted)
        timer = setTimeout(() => void run(), intervalMs);
    }
  };
  void run();
  return () => {
    controller.abort();
    if (timer !== null) clearTimeout(timer);
  };
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

export function createUniverseClient(
  getToken?: () => string | null,
  onUnauthorized?: () => void,
) {
  const baseUrl = import.meta.env.VITE_UNIVERSE_API_BASE_URL as
    | string
    | undefined;
  if (!baseUrl) throw new Error('VITE_UNIVERSE_API_BASE_URL is required');
  return new UniverseClient({ baseUrl, getToken, onUnauthorized });
}
