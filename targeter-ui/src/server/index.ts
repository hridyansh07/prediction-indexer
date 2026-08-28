import express from 'express';
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import {
  createEventUniverseRouter,
  EventUniverseClient,
} from './event-universe.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const port = positive(process.env.PORT, 3000, 'PORT');
const universeUrl = process.env.UNIVERSE_API_BASE_URL;
const universe = universeUrl
  ? new EventUniverseClient({
      baseUrl: universeUrl,
      authorization: process.env.UNIVERSE_API_AUTHORIZATION,
      timeoutMs: positive(
        process.env.UNIVERSE_API_TIMEOUT_MS,
        5000,
        'UNIVERSE_API_TIMEOUT_MS',
      ),
      maxResponseBytes: positive(
        process.env.UNIVERSE_API_MAX_RESPONSE_BYTES,
        2 * 1024 * 1024,
        'UNIVERSE_API_MAX_RESPONSE_BYTES',
      ),
    })
  : null;
const app = express();
app.disable('x-powered-by');
app.get('/healthz', (_q, r) =>
  r.json({ ready: true, universeConfigured: universe !== null }),
);
app.use('/api/event-universe', createEventUniverseRouter(universe));
const web = path.resolve(here, '../../dist');
if (fs.existsSync(web)) {
  app.use(express.static(web));
  app.get('*splat', (_q, r) => r.sendFile(path.join(web, 'index.html')));
}
app.listen(port, '0.0.0.0', () =>
  console.log(`Targeter UI listening on port ${port}`),
);

function positive(raw: string | undefined, fallback: number, name: string) {
  const n = raw === undefined ? fallback : Number(raw);
  if (!Number.isInteger(n) || n <= 0)
    throw new Error(`${name} must be a positive integer`);
  return n;
}
