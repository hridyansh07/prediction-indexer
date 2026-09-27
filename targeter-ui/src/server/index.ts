import express from 'express';
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const port = positive(process.env.PORT, 3000, 'PORT');
const app = express();
app.disable('x-powered-by');
app.get('/healthz', (_q, r) => r.json({ ready: true }));
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
