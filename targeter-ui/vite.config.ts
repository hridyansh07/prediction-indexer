import { createReadStream, statSync } from 'node:fs';
import path from 'node:path';
import { defineConfig, type Plugin } from 'vite';
import react from '@vitejs/plugin-react';

// Local research: RESEARCH_CORPUS_DIR=<build output> serves that directory
// read-only at /corpus in the dev server, so /research works offline.
function researchCorpus(): Plugin {
  const root = process.env.RESEARCH_CORPUS_DIR;
  return {
    name: 'research-corpus',
    apply: 'serve',
    configureServer(server) {
      if (!root) return;
      const base = path.resolve(root);
      server.middlewares.use('/corpus', (request, response, next) => {
        if (request.method !== 'GET' && request.method !== 'HEAD')
          return next();
        const relative = decodeURIComponent((request.url ?? '/').split('?')[0]);
        const file = path.resolve(base, '.' + relative);
        if (file !== base && !file.startsWith(base + path.sep)) {
          response.statusCode = 403;
          return response.end();
        }
        let size: number;
        try {
          const stat = statSync(file);
          if (!stat.isFile()) throw new Error('not a file');
          size = stat.size;
        } catch {
          response.statusCode = 404;
          return response.end();
        }
        const type = file.endsWith('.json')
          ? 'application/json'
          : file.endsWith('.md')
            ? 'text/markdown; charset=utf-8'
            : 'application/octet-stream';
        response.setHeader('Content-Type', type);
        response.setHeader('Content-Length', String(size));
        response.setHeader('Cache-Control', 'no-cache');
        if (request.method === 'HEAD') return response.end();
        createReadStream(file).pipe(response);
      });
    },
  };
}

export default defineConfig({
  plugins: [react(), researchCorpus()],
});
