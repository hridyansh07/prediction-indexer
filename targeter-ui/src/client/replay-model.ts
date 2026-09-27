export const replayStatuses = [
  'queued',
  'running',
  'archiving',
  'succeeded',
  'failed',
  'exhausted',
  'not_ready',
  'stale_bundle_cache',
  'archive_blocked',
  'cancelled',
] as const;

export type ReplayStatus = (typeof replayStatuses)[number];
export type ReplayStage =
  | 'resolve'
  | 'bundle'
  | 'prepare'
  | 'run'
  | 'read'
  | 'archive';

export type ReplayJob = {
  id: string;
  bundle: string;
  event: string;
  status: ReplayStatus;
  stage: ReplayStage | null;
  pendingOutcome: ReplayStatus | null;
  strategy: string;
  strategyLabel: string;
  markets: 'all' | string[];
  submittedBy: string;
  createdAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  attempts: number;
  reasonCode: string | null;
  reasonDetail: string | null;
  archiveReceipt: string | null;
};

export type ReplayJobEvent = {
  id: number;
  title: string;
  detail: string;
  at: string;
  tone: 'neutral' | 'positive' | 'warning' | 'negative';
};

export type ReplayStatusMeta = {
  label: string;
  description: string;
  tone: 'neutral' | 'positive' | 'warning' | 'negative';
  symbol: string;
};

export const replayStatusMeta: Record<ReplayStatus, ReplayStatusMeta> = {
  queued: {
    label: 'Queued',
    description: 'Waiting for an available runner',
    tone: 'neutral',
    symbol: '◷',
  },
  running: {
    label: 'Running',
    description: 'Replay is processing historical evidence',
    tone: 'neutral',
    symbol: '↻',
  },
  archiving: {
    label: 'Archiving',
    description: 'Work ended and durable evidence is being archived',
    tone: 'neutral',
    symbol: '⇧',
  },
  succeeded: {
    label: 'Succeeded',
    description: 'Replay and archival completed',
    tone: 'positive',
    symbol: '✓',
  },
  failed: {
    label: 'Failed',
    description: 'Replay stopped with a non-retryable failure',
    tone: 'negative',
    symbol: '×',
  },
  exhausted: {
    label: 'Exhausted',
    description: 'The retry budget was exhausted',
    tone: 'negative',
    symbol: '!',
  },
  not_ready: {
    label: 'Not ready',
    description: 'Required historical evidence is not ready',
    tone: 'warning',
    symbol: '◇',
  },
  stale_bundle_cache: {
    label: 'Stale cache',
    description: 'The immutable bundle cache needs operator attention',
    tone: 'warning',
    symbol: '△',
  },
  archive_blocked: {
    label: 'Archive blocked',
    description: 'Work ended, but its archive receipt is not durable',
    tone: 'negative',
    symbol: '▣',
  },
  cancelled: {
    label: 'Cancelled',
    description: 'Cancelled before the replay started',
    tone: 'neutral',
    symbol: '—',
  },
};

const fixtures: Record<ReplayStatus, Omit<ReplayJob, 'status' | 'id'>> = {
  queued: {
    bundle: 'iem-cologne-2026-final',
    event: 'Team Vitality vs MOUZ',
    stage: null,
    pendingOutcome: null,
    strategy: 'bundle_coverage',
    strategyLabel: 'Bundle coverage',
    markets: 'all',
    submittedBy: '0x7A42…19F2',
    createdAt: '2026-09-27T09:42:00Z',
    startedAt: null,
    finishedAt: null,
    attempts: 0,
    reasonCode: null,
    reasonDetail: null,
    archiveReceipt: null,
  },
  running: {
    bundle: 'worlds-2026-quarterfinal-2',
    event: 'Gen.G vs Bilibili Gaming',
    stage: 'run',
    pendingOutcome: null,
    strategy: 'bundle_coverage',
    strategyLabel: 'Bundle coverage',
    markets: ['polymarket:match-winner', 'kalshi:series-winner'],
    submittedBy: '0x7A42…19F2',
    createdAt: '2026-09-27T09:18:00Z',
    startedAt: '2026-09-27T09:19:00Z',
    finishedAt: null,
    attempts: 1,
    reasonCode: null,
    reasonDetail: null,
    archiveReceipt: null,
  },
  archiving: {
    bundle: 'epl-season-22-semifinal',
    event: 'Aurora Gaming vs The MongolZ',
    stage: 'archive',
    pendingOutcome: 'succeeded',
    strategy: 'bundle_coverage',
    strategyLabel: 'Bundle coverage',
    markets: 'all',
    submittedBy: '0x31D8…8A04',
    createdAt: '2026-09-27T08:51:00Z',
    startedAt: '2026-09-27T08:52:00Z',
    finishedAt: null,
    attempts: 1,
    reasonCode: null,
    reasonDetail: null,
    archiveReceipt: null,
  },
  succeeded: {
    bundle: 'ti-2026-lower-bracket-final',
    event: 'Team Spirit vs PARIVISION',
    stage: 'archive',
    pendingOutcome: null,
    strategy: 'bundle_coverage',
    strategyLabel: 'Bundle coverage',
    markets: 'all',
    submittedBy: '0x31D8…8A04',
    createdAt: '2026-09-26T18:04:00Z',
    startedAt: '2026-09-26T18:05:00Z',
    finishedAt: '2026-09-26T18:24:00Z',
    attempts: 1,
    reasonCode: null,
    reasonDetail: null,
    archiveReceipt:
      'replay/jobs/20260926T180400Z-421b73abf950cc0a/job_receipt.json',
  },
  failed: {
    bundle: 'valorant-champions-2026-sf1',
    event: 'FNATIC vs Paper Rex',
    stage: 'archive',
    pendingOutcome: null,
    strategy: 'bundle_coverage',
    strategyLabel: 'Bundle coverage',
    markets: 'all',
    submittedBy: '0x7A42…19F2',
    createdAt: '2026-09-26T14:10:00Z',
    startedAt: '2026-09-26T14:11:00Z',
    finishedAt: '2026-09-26T14:13:00Z',
    attempts: 1,
    reasonCode: 'integrity_failure',
    reasonDetail:
      'A source receipt did not match its archived object identity.',
    archiveReceipt:
      'replay/jobs/20260926T141000Z-218f22a964defc1b/job_receipt.json',
  },
  exhausted: {
    bundle: 'lck-2026-regional-final',
    event: 'T1 vs Hanwha Life Esports',
    stage: 'archive',
    pendingOutcome: null,
    strategy: 'bundle_coverage',
    strategyLabel: 'Bundle coverage',
    markets: 'all',
    submittedBy: '0x31D8…8A04',
    createdAt: '2026-09-26T11:36:00Z',
    startedAt: '2026-09-26T11:37:00Z',
    finishedAt: '2026-09-26T12:07:00Z',
    attempts: 3,
    reasonCode: 'supervisor_exhausted',
    reasonDetail: 'No attempt reached the required progress margin.',
    archiveReceipt:
      'replay/jobs/20260926T113600Z-83dbb62286a99ae8/job_receipt.json',
  },
  not_ready: {
    bundle: 'blast-fall-2026-group-a',
    event: 'NAVI vs FaZe Clan',
    stage: 'archive',
    pendingOutcome: null,
    strategy: 'bundle_coverage',
    strategyLabel: 'Bundle coverage',
    markets: 'all',
    submittedBy: '0x7A42…19F2',
    createdAt: '2026-09-25T20:08:00Z',
    startedAt: '2026-09-25T20:09:00Z',
    finishedAt: '2026-09-25T20:10:00Z',
    attempts: 1,
    reasonCode: 'canonical_not_archived',
    reasonDetail: 'One canonical window has not been archived yet.',
    archiveReceipt:
      'replay/jobs/20260925T200800Z-b05dca7d86308973/job_receipt.json',
  },
  stale_bundle_cache: {
    bundle: 'vct-emea-2026-grand-final',
    event: 'Team Liquid vs GIANTX',
    stage: 'archive',
    pendingOutcome: null,
    strategy: 'bundle_coverage',
    strategyLabel: 'Bundle coverage',
    markets: 'all',
    submittedBy: '0x31D8…8A04',
    createdAt: '2026-09-25T16:31:00Z',
    startedAt: '2026-09-25T16:32:00Z',
    finishedAt: '2026-09-25T16:33:00Z',
    attempts: 1,
    reasonCode: 'stale_bundle_cache',
    reasonDetail:
      'The cached producer identity differs from the current runner.',
    archiveReceipt:
      'replay/jobs/20260925T163100Z-3c4a4b924fbc1eda/job_receipt.json',
  },
  archive_blocked: {
    bundle: 'honor-of-kings-2026-final',
    event: 'AG Super Play vs Wolves',
    stage: 'archive',
    pendingOutcome: 'succeeded',
    strategy: 'bundle_coverage',
    strategyLabel: 'Bundle coverage',
    markets: 'all',
    submittedBy: '0x7A42…19F2',
    createdAt: '2026-09-25T10:44:00Z',
    startedAt: '2026-09-25T10:45:00Z',
    finishedAt: null,
    attempts: 3,
    reasonCode: null,
    reasonDetail: 'Archive storage remained unavailable after three attempts.',
    archiveReceipt: null,
  },
  cancelled: {
    bundle: 'esl-pro-league-2026-r4',
    event: 'G2 Esports vs Team Falcons',
    stage: 'archive',
    pendingOutcome: null,
    strategy: 'bundle_coverage',
    strategyLabel: 'Bundle coverage',
    markets: 'all',
    submittedBy: '0x7A42…19F2',
    createdAt: '2026-09-24T21:02:00Z',
    startedAt: null,
    finishedAt: '2026-09-24T21:04:00Z',
    attempts: 1,
    reasonCode: 'cancelled',
    reasonDetail: 'Cancelled by the submitter before processing began.',
    archiveReceipt:
      'replay/jobs/20260924T210200Z-750136bf7ddbc381/job_receipt.json',
  },
};

const fixtureJobIds = [
  '20260927T094200Z-96ec29da54106f1c',
  '20260927T091800Z-f3d2486a0d4407ad',
  '20260927T085100Z-c81a286902d8ba54',
  '20260926T180400Z-421b73abf950cc0a',
  '20260926T141000Z-218f22a964defc1b',
  '20260926T113600Z-83dbb62286a99ae8',
  '20260925T200800Z-b05dca7d86308973',
  '20260925T163100Z-3c4a4b924fbc1eda',
  '20260925T104400Z-68318652333f708d',
  '20260924T210200Z-750136bf7ddbc381',
];

export const replayJobs: ReplayJob[] = replayStatuses.map((status, index) => ({
  id: fixtureJobIds[index],
  status,
  ...fixtures[status],
}));

export const replayStages: ReplayStage[] = [
  'resolve',
  'bundle',
  'prepare',
  'run',
  'read',
  'archive',
];

export function jobProgress(job: ReplayJob): number {
  if (job.status === 'queued') return 0;
  if (job.status === 'running' && job.stage)
    return Math.round(
      ((replayStages.indexOf(job.stage) + 0.45) / replayStages.length) * 100,
    );
  if (job.status === 'archiving') return 96;
  if (job.status === 'archive_blocked') return 92;
  return 100;
}

export function findReplayJob(
  id: string,
  requested?: string | null,
): ReplayJob {
  const byStatus = replayStatuses.find((status) => status === requested);
  return (
    replayJobs.find((job) => job.status === byStatus) ??
    replayJobs.find((job) => job.id === id) ??
    replayJobs[1]
  );
}

export function replayJobEvents(job: ReplayJob): ReplayJobEvent[] {
  const events: ReplayJobEvent[] = [
    {
      id: 1,
      title: 'Request accepted',
      detail: 'The replay request was validated and queued.',
      at: job.createdAt,
      tone: 'neutral',
    },
  ];
  if (job.status === 'queued') return events;
  if (job.startedAt)
    events.push({
      id: 2,
      title: 'Runner claimed job',
      detail: `Started resolve stage · attempt ${Math.max(1, job.attempts)}`,
      at: job.startedAt,
      tone: 'neutral',
    });
  if (job.status === 'running') {
    events.push({
      id: 3,
      title: 'Replay in progress',
      detail: `Currently in the ${job.stage ?? 'resolve'} stage.`,
      at: '2026-09-27T09:25:00Z',
      tone: 'neutral',
    });
    return events;
  }
  if (job.status === 'archiving') {
    events.push({
      id: 3,
      title: 'Archiving evidence',
      detail: `Replay work ended with ${replayStatusMeta[job.pendingOutcome ?? 'succeeded'].label.toLowerCase()} pending.`,
      at: '2026-09-27T09:07:00Z',
      tone: 'neutral',
    });
    return events;
  }
  if (job.status === 'archive_blocked') {
    events.push({
      id: 3,
      title: 'Archive blocked',
      detail: job.reasonDetail ?? 'The archive could not be completed.',
      at: '2026-09-25T11:16:00Z',
      tone: 'negative',
    });
    return events;
  }
  events.push({
    id: 3,
    title: replayStatusMeta[job.status].label,
    detail: job.reasonDetail ?? 'The replay and its archive receipt completed.',
    at: job.finishedAt ?? job.createdAt,
    tone: replayStatusMeta[job.status].tone,
  });
  return events;
}

export type ReplayForm = {
  bundleId: string;
  marketMode: 'all' | 'selected';
  selectedMarkets: string[];
  strategy: string;
};

export function validateReplayForm(form: ReplayForm): Record<string, string> {
  const errors: Record<string, string> = {};
  if (!form.bundleId) errors.bundleId = 'Choose a historical bundle.';
  if (form.marketMode === 'selected' && form.selectedMarkets.length === 0)
    errors.markets = 'Choose at least one market or replay all markets.';
  if (!form.strategy) errors.strategy = 'Choose an active strategy.';
  return errors;
}
