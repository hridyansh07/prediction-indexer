// DuckDB-WASM over the corpus Parquet tables. Loaded only on /research.
// The Parquet bytes are verified against the corpus manifest first and registered
// as in-memory buffers, so no query ever reads an unverified byte.
import * as duckdb from '@duckdb/duckdb-wasm';
import ehWasm from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url';
import ehWorker from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';
import mvpWasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import mvpWorker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';
import { corpusFile, type Corpus } from './research-data';

export interface Tables {
  query<T>(
    template: string,
    ...params: (string | number | null)[]
  ): Promise<T[]>;
}

let opened: Promise<duckdb.AsyncDuckDB> | null = null;

function database(): Promise<duckdb.AsyncDuckDB> {
  opened ??= (async () => {
    const bundle = await duckdb.selectBundle({
      mvp: { mainModule: mvpWasm, mainWorker: mvpWorker },
      eh: { mainModule: ehWasm, mainWorker: ehWorker },
    });
    const worker = new Worker(bundle.mainWorker!);
    const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    return db;
  })();
  return opened;
}

function plain(value: unknown): unknown {
  if (typeof value === 'bigint') return Number(value);
  if (
    value &&
    typeof value === 'object' &&
    'toArray' in value &&
    typeof value.toArray === 'function'
  ) {
    return [...(value.toArray() as unknown[])].map(plain);
  }
  return value;
}

export async function openTables(corpus: Corpus): Promise<Tables> {
  const db = await database();
  const version = corpus.pointer.version;
  for (const name of ['events', 'episodes']) {
    const bytes = await corpusFile(corpus, `${name}.parquet`);
    await db.registerFileBuffer(`${version}-${name}.parquet`, bytes);
  }
  const connection = await db.connect();
  for (const name of ['events', 'episodes']) {
    await connection.query(
      `CREATE OR REPLACE VIEW ${name} AS SELECT * FROM read_parquet('${version}-${name}.parquet')`,
    );
  }
  return {
    async query<T>(template: string, ...params: (string | number | null)[]) {
      const statement = await connection.prepare(template);
      try {
        const table = await statement.query(...params);
        return table.toArray().map((row) => {
          const object = row.toJSON() as Record<string, unknown>;
          return Object.fromEntries(
            Object.entries(object).map(([k, v]) => [k, plain(v)]),
          ) as T;
        });
      } finally {
        await statement.close();
      }
    },
  };
}
