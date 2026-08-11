import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readdir, readFile, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import { GetObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';

import {
  S3ReadOnlyObjectStore,
  type ObjectRead,
  type ReadOnlyObjectStore,
  withStagedObject,
} from '../src/index.js';

class Body implements AsyncIterable<Uint8Array> {
  closed = 0;

  constructor(
    readonly chunks: Uint8Array[],
    readonly failure?: Error,
  ) {}

  async *[Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    for (const chunk of this.chunks) yield chunk;
    if (this.failure) throw this.failure;
  }

  destroy(): void {
    this.closed += 1;
  }
}

class FakeClient {
  readonly calls: Array<ListObjectsV2Command | GetObjectCommand> = [];
  readonly responses: unknown[];

  constructor(...responses: unknown[]) {
    this.responses = responses;
  }

  async send(command: ListObjectsV2Command | GetObjectCommand): Promise<any> {
    this.calls.push(command);
    const response = this.responses.shift();
    if (response instanceof Error) throw response;
    return response;
  }
}

function s3(client: FakeClient): S3ReadOnlyObjectStore {
  return new S3ReadOnlyObjectStore({
    bucket: 'bucket',
    expectedBucketOwner: '123456789012',
    region: 'us-east-1',
    client: client as never,
  });
}

test('requires an explicit region and canonical expected owner', () => {
  assert.throws(
    () =>
      new S3ReadOnlyObjectStore({
        bucket: 'bucket',
        expectedBucketOwner: 'owner',
        region: 'us-east-1',
      }),
    /12-digit/,
  );
  assert.throws(
    () =>
      new S3ReadOnlyObjectStore({
        bucket: 'bucket',
        expectedBucketOwner: '123456789012',
        region: '',
      }),
    /region/,
  );
});

test('lists every page and sends ExpectedBucketOwner on every request', async () => {
  const client = new FakeClient(
    {
      Contents: [{ Key: 'p/a' }],
      IsTruncated: true,
      NextContinuationToken: 'two',
    },
    { Contents: [{ Key: 'p/b' }], IsTruncated: false },
  );
  const keys: string[] = [];
  for await (const key of s3(client).listKeys('p/')) keys.push(key);
  assert.deepEqual(keys, ['p/a', 'p/b']);
  assert.equal(client.calls.length, 2);
  for (const call of client.calls) {
    assert(call instanceof ListObjectsV2Command);
    assert.equal(call.input.ExpectedBucketOwner, '123456789012');
  }
  assert.equal(
    (client.calls[1] as ListObjectsV2Command).input.ContinuationToken,
    'two',
  );
});

test('rejects missing and repeated pagination tokens', async () => {
  const missing = s3(new FakeClient({ IsTruncated: true }));
  await assert.rejects(async () => {
    for await (const ignored of missing.listKeys('')) void ignored;
  }, /no next token/);

  const repeated = s3(
    new FakeClient(
      { IsTruncated: true, NextContinuationToken: 'same' },
      { IsTruncated: true, NextContinuationToken: 'same' },
    ),
  );
  await assert.rejects(async () => {
    for await (const ignored of repeated.listKeys('')) void ignored;
  }, /repeated continuation token/);
});

test('GetObject sends owner and declared length fails before body exposure', async () => {
  const body = new Body([Buffer.from('abcdef')]);
  const client = new FakeClient({ Body: body, ContentLength: 6 });
  await assert.rejects(s3(client).open('key', { maxBytes: 5 }), /declared/);
  assert.equal(body.closed, 1);
  const call = client.calls[0];
  assert(call instanceof GetObjectCommand);
  assert.equal(call.input.ExpectedBucketOwner, '123456789012');
});

test('actual length is independently bounded and byte max+1 is never yielded', async () => {
  const body = new Body([Buffer.from('abcdef')]);
  const read = await s3(new FakeClient({ Body: body, ContentLength: 1 })).open(
    'key',
    {
      maxBytes: 5,
    },
  );
  const yielded: Uint8Array[] = [];
  await assert.rejects(async () => {
    for await (const chunk of read.body) yielded.push(chunk);
  }, /streamed body/);
  assert.equal(Buffer.concat(yielded).toString(), 'abcde');
  assert.equal(body.closed, 1);
});

test('validates maxBytes before an S3 request', async () => {
  for (const value of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1, Number.NaN]) {
    const client = new FakeClient();
    await assert.rejects(
      s3(client).open('key', { maxBytes: value }),
      /safe nonnegative/,
    );
    assert.equal(client.calls.length, 0);
  }
});

class MemoryStore implements ReadOnlyObjectStore {
  closes = 0;

  constructor(readonly bodyFactory: () => AsyncIterable<Uint8Array>) {}

  async *listKeys(): AsyncIterable<string> {}

  async open(): Promise<ObjectRead> {
    return {
      body: this.bodyFactory(),
      close: () => {
        this.closes += 1;
      },
    };
  }
}

async function emptyRoot(root: string): Promise<void> {
  assert.deepEqual(await readdir(root), []);
}

test('stages privately, hashes bytes, invokes callback, and cleans success', async () => {
  const root = await import('node:fs/promises').then(({ mkdtemp }) =>
    mkdtemp(join(tmpdir(), 'staged-test-')),
  );
  const store = new MemoryStore(async function* () {
    yield Buffer.from('abc');
  });
  try {
    const result = await withStagedObject(
      store,
      'key',
      { maxBytes: 3, tempRoot: root },
      async (staged) => {
        assert.equal((await stat(join(staged.path, '..'))).mode & 0o777, 0o700);
        assert.equal((await stat(staged.path)).mode & 0o777, 0o600);
        assert.equal((await readFile(staged.path)).toString(), 'abc');
        assert.deepEqual(staged.identity, {
          sha256: createHash('sha256').update('abc').digest('hex'),
          byteLength: 3,
        });
        return 42;
      },
    );
    assert.equal(result, 42);
    assert.equal(store.closes, 1);
    await emptyRoot(root);
  } finally {
    await import('node:fs/promises').then(({ rm }) =>
      rm(root, { recursive: true }),
    );
  }
});

test('identity mismatch occurs before callback and cleans', async () => {
  const { mkdtemp, rm } = await import('node:fs/promises');
  const root = await mkdtemp(join(tmpdir(), 'staged-test-'));
  let called = false;
  const store = new MemoryStore(async function* () {
    yield Buffer.from('abc');
  });
  try {
    await assert.rejects(
      withStagedObject(
        store,
        'key',
        {
          maxBytes: 3,
          expected: { sha256: '0'.repeat(64), byteLength: 3 },
          tempRoot: root,
        },
        () => {
          called = true;
        },
      ),
      /identity mismatch/,
    );
    assert.equal(called, false);
    assert.equal(store.closes, 1);
    await emptyRoot(root);
  } finally {
    await rm(root, { recursive: true });
  }
});

test('cleans and cancels on stream and callback failures', async () => {
  const { mkdtemp, rm } = await import('node:fs/promises');
  for (const kind of ['stream', 'callback'] as const) {
    const root = await mkdtemp(join(tmpdir(), 'staged-test-'));
    const store = new MemoryStore(async function* () {
      yield Buffer.from('a');
      if (kind === 'stream') throw new Error('stream failed');
    });
    try {
      await assert.rejects(
        withStagedObject(store, 'key', { maxBytes: 2, tempRoot: root }, () => {
          throw new Error('callback failed');
        }),
        new RegExp(`${kind} failed`),
      );
      assert.equal(store.closes, 1);
      await emptyRoot(root);
    } finally {
      await rm(root, { recursive: true });
    }
  }
});

test('abort interrupts a pending stream, cancels it, and cleans', async () => {
  const { mkdtemp, rm } = await import('node:fs/promises');
  const root = await mkdtemp(join(tmpdir(), 'staged-test-'));
  const controller = new AbortController();
  const store = new MemoryStore(() => ({
    [Symbol.asyncIterator]() {
      return { next: () => new Promise<IteratorResult<Uint8Array>>(() => {}) };
    },
  }));
  try {
    const operation = withStagedObject(
      store,
      'key',
      {
        maxBytes: 2,
        tempRoot: root,
        signal: controller.signal,
      },
      () => undefined,
    );
    setImmediate(() => controller.abort());
    await assert.rejects(
      operation,
      (error) => (error as Error).name === 'AbortError',
    );
    assert.equal(store.closes, 1);
    await emptyRoot(root);
  } finally {
    await rm(root, { recursive: true });
  }
});

test('cleans staging when object close rejects', async () => {
  const { mkdtemp, rm } = await import('node:fs/promises');
  const root = await mkdtemp(join(tmpdir(), 'staged-test-'));
  const store: ReadOnlyObjectStore = {
    async *listKeys() {},
    async open() {
      return {
        body: (async function* () {
          yield Buffer.from('a');
        })(),
        async close() {
          throw new Error('close failed');
        },
      };
    },
  };
  try {
    await assert.rejects(
      withStagedObject(
        store,
        'key',
        { maxBytes: 1, tempRoot: root },
        () => undefined,
      ),
      /close failed/,
    );
    await emptyRoot(root);
  } finally {
    await rm(root, { recursive: true });
  }
});
