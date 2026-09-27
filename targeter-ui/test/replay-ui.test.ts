import test from 'node:test';
import assert from 'node:assert/strict';
import {
  findReplayJob,
  jobProgress,
  replayJobEvents,
  replayJobs,
  replayStatuses,
  replayStatusMeta,
  validateReplayForm,
} from '../src/client/replay-model.js';

test('fixtures cover every landed visible status with non-color labels', () => {
  assert.deepEqual(
    replayJobs.map((job) => job.status),
    replayStatuses,
  );
  for (const status of replayStatuses) {
    assert.ok(replayStatusMeta[status].label);
    assert.ok(replayStatusMeta[status].symbol);
  }
});

test('progress distinguishes queued, mid-run, blocked archive, and terminal jobs', () => {
  assert.equal(jobProgress(findReplayJob('missing', 'queued')), 0);
  const running = jobProgress(findReplayJob('missing', 'running'));
  assert.ok(running > 0 && running < 100);
  assert.equal(jobProgress(findReplayJob('missing', 'archive_blocked')), 92);
  assert.equal(jobProgress(findReplayJob('missing', 'succeeded')), 100);
});

test('job timeline keeps queued cancellation distinct from running work', () => {
  const queued = replayJobEvents(findReplayJob('missing', 'queued'));
  const cancelled = replayJobEvents(findReplayJob('missing', 'cancelled'));
  assert.equal(queued.length, 1);
  assert.equal(cancelled.at(-1)?.title, 'Cancelled');
  assert.equal(
    cancelled.some((event) => event.title === 'Runner claimed job'),
    false,
  );
});

test('new replay validation requires bundle, selected markets, and active strategy', () => {
  assert.deepEqual(
    validateReplayForm({
      bundleId: '',
      marketMode: 'selected',
      selectedMarkets: [],
      strategy: '',
    }),
    {
      bundleId: 'Choose a historical bundle.',
      markets: 'Choose at least one market or replay all markets.',
      strategy: 'Choose an active strategy.',
    },
  );
  assert.deepEqual(
    validateReplayForm({
      bundleId: 'bundle-a',
      marketMode: 'all',
      selectedMarkets: [],
      strategy: 'bundle_coverage',
    }),
    {},
  );
});
