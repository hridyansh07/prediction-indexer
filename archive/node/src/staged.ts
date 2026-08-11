import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { chmod, mkdtemp, open, rm, type FileHandle } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  type ByteIdentity,
  type ReadOnlyObjectStore,
  validateMaxBytes,
} from './types.js';

function abortError(): Error {
  const error = new Error('The operation was aborted');
  error.name = 'AbortError';
  return error;
}

function checkAbort(signal?: AbortSignal): void {
  if (signal?.aborted) throw signal.reason ?? abortError();
}

async function writeAll(file: FileHandle, chunk: Uint8Array): Promise<void> {
  let offset = 0;
  while (offset < chunk.byteLength) {
    const { bytesWritten } = await file.write(
      chunk,
      offset,
      chunk.byteLength - offset,
    );
    if (bytesWritten === 0)
      throw new Error('staged file write made no progress');
    offset += bytesWritten;
  }
}

async function nextWithAbort<T>(
  iterator: AsyncIterator<T>,
  signal?: AbortSignal,
): Promise<IteratorResult<T>> {
  checkAbort(signal);
  if (!signal) return iterator.next();
  return new Promise((resolve, reject) => {
    const onAbort = (): void => reject(signal.reason ?? abortError());
    signal.addEventListener('abort', onAbort, { once: true });
    void iterator
      .next()
      .then(resolve, reject)
      .finally(() => {
        signal.removeEventListener('abort', onAbort);
      });
  });
}

export async function withStagedObject<T>(
  store: ReadOnlyObjectStore,
  key: string,
  options: {
    maxBytes: number;
    expected?: ByteIdentity;
    tempRoot?: string;
    signal?: AbortSignal;
  },
  use: (staged: { path: string; identity: ByteIdentity }) => T | Promise<T>,
): Promise<T> {
  validateMaxBytes(options.maxBytes);
  checkAbort(options.signal);
  const directory = await mkdtemp(
    join(options.tempRoot ?? tmpdir(), 'object-store-'),
  );
  const path = join(directory, 'object');
  let objectRead: Awaited<ReturnType<ReadOnlyObjectStore['open']>> | undefined;

  try {
    await chmod(directory, 0o700);
    const file = await open(
      path,
      constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL,
      0o600,
    );
    const hash = createHash('sha256');
    let byteLength = 0;
    try {
      objectRead = await store.open(key, {
        maxBytes: options.maxBytes,
        signal: options.signal,
      });
      const iterator = objectRead.body[Symbol.asyncIterator]();
      while (true) {
        const item = await nextWithAbort(iterator, options.signal);
        if (item.done) break;
        checkAbort(options.signal);
        const chunk = item.value;
        if (byteLength + chunk.byteLength > options.maxBytes) {
          throw new RangeError('object exceeds maxBytes');
        }
        await writeAll(file, chunk);
        hash.update(chunk);
        byteLength += chunk.byteLength;
      }
      await file.sync();
    } finally {
      await file.close();
    }

    const identity = { sha256: hash.digest('hex'), byteLength };
    if (
      options.expected &&
      (options.expected.sha256 !== identity.sha256 ||
        options.expected.byteLength !== identity.byteLength)
    ) {
      throw new Error('staged object identity mismatch');
    }
    checkAbort(options.signal);
    return await use({ path, identity });
  } finally {
    try {
      await objectRead?.close();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }
}
