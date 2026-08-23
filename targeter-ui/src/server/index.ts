import express from 'express';
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { S3ReadOnlyObjectStore } from '@prediction-indexer/read-only-object-store';
import { RustV1Decoder } from '@prediction-indexer/rust-v1-decoder';
import { SnapshotService } from './service.js';
import {
  createEventUniverseRouter,
  EventUniverseClient,
} from './event-universe.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const readConfig = () =>
  JSON.parse(
    fs.readFileSync(
      path.resolve(here, '../../../configs/targeter_v2.json'),
      'utf8',
    ),
  );
const fixture = process.env.TARGETER_UI_FIXTURE_PATH;
const required = [
  'TARGETER_UI_S3_BUCKET',
  'TARGETER_UI_AWS_REGION',
  'TARGETER_UI_S3_EXPECTED_OWNER',
  'TARGETER_UI_DECODER_PATH',
] as const;
if (!fixture) {
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length)
    throw new Error(
      `Missing required environment variables: ${missing.join(', ')}`,
    );
}
if (!fixture && !/^\d{12}$/.test(process.env.TARGETER_UI_S3_EXPECTED_OWNER!))
  throw new Error(
    'TARGETER_UI_S3_EXPECTED_OWNER must be a 12-digit AWS account ID',
  );
const refreshSeconds = positive(
  process.env.TARGETER_UI_REFRESH_SECONDS,
  60,
  'TARGETER_UI_REFRESH_SECONDS',
);
const expectedRunSeconds = positive(
  process.env.TARGETER_UI_EXPECTED_RUN_SECONDS,
  600,
  'TARGETER_UI_EXPECTED_RUN_SECONDS',
);
const maxRuns = positive(
  process.env.TARGETER_UI_MAX_RUNS,
  5,
  'TARGETER_UI_MAX_RUNS',
);
if (maxRuns !== 5) throw new Error('TARGETER_UI_MAX_RUNS is fixed at 5');
const port = positive(process.env.PORT, 3000, 'PORT');
const store = fixture
  ? null
  : new S3ReadOnlyObjectStore({
      bucket: process.env.TARGETER_UI_S3_BUCKET!,
      region: process.env.TARGETER_UI_AWS_REGION!,
      expectedBucketOwner: process.env.TARGETER_UI_S3_EXPECTED_OWNER!,
    });
const decoder = fixture
  ? null
  : new RustV1Decoder({ binaryPath: process.env.TARGETER_UI_DECODER_PATH! });
const service = new SnapshotService(
  store,
  decoder,
  readConfig(),
  refreshSeconds,
  expectedRunSeconds,
  process.env.TARGETER_UI_S3_PREFIX ?? 'targeter-v2/runs',
  fixture,
);
const universeUrl = process.env.TARGETER_UI_EVENT_UNIVERSE_URL;
const universe = universeUrl
  ? new EventUniverseClient({
      baseUrl: universeUrl,
      authorization: process.env.TARGETER_UI_EVENT_UNIVERSE_AUTHORIZATION,
      timeoutMs: positive(
        process.env.TARGETER_UI_EVENT_UNIVERSE_TIMEOUT_MS,
        5000,
        'TARGETER_UI_EVENT_UNIVERSE_TIMEOUT_MS',
      ),
      maxResponseBytes: positive(
        process.env.TARGETER_UI_EVENT_UNIVERSE_MAX_RESPONSE_BYTES,
        2 * 1024 * 1024,
        'TARGETER_UI_EVENT_UNIVERSE_MAX_RESPONSE_BYTES',
      ),
    })
  : null;
const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '8kb' }));
app.get('/healthz', (_q, r) =>
  r.status(service.snapshot.lastSuccessfulRefresh ? 200 : 503).json({
    ready: !!service.snapshot.lastSuccessfulRefresh,
    stale: service.snapshot.stale,
    refreshing: service.snapshot.refreshing,
  }),
);
app.get('/api/snapshot', (_q, r) => r.json(service.snapshot));
app.post('/api/refresh', async (_q, r) => r.json(await service.refresh()));
app.use('/api/event-universe', createEventUniverseRouter(universe));
const web = path.resolve(here, '../../dist');
if (fs.existsSync(web)) {
  app.use(express.static(web));
  app.get('*splat', (_q, r) => r.sendFile(path.join(web, 'index.html')));
}
app.listen(port, '0.0.0.0', () =>
  console.log(
    `Targeter UI listening on port ${port} (${fixture ? 'fixture' : 'S3'} mode)`,
  ),
);
void service.refresh();
setInterval(() => void service.refresh(), refreshSeconds * 1000).unref();
function positive(raw: string | undefined, fallback: number, name: string) {
  const n = raw === undefined ? fallback : Number(raw);
  if (!Number.isInteger(n) || n <= 0)
    throw new Error(`${name} must be a positive integer`);
  return n;
}
