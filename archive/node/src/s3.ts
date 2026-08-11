import {
  GetObjectCommand,
  ListObjectsV2Command,
  S3Client,
  type GetObjectCommandOutput,
  type ListObjectsV2CommandOutput,
} from '@aws-sdk/client-s3';

import {
  type ObjectRead,
  type ReadOnlyObjectStore,
  validateMaxBytes,
} from './types.js';

type S3Sender = {
  send(
    command: ListObjectsV2Command,
    options?: { abortSignal?: AbortSignal },
  ): Promise<ListObjectsV2CommandOutput>;
  send(
    command: GetObjectCommand,
    options?: { abortSignal?: AbortSignal },
  ): Promise<GetObjectCommandOutput>;
};

function asAsyncBody(body: unknown): AsyncIterable<Uint8Array> {
  if (
    body === null ||
    typeof body !== 'object' ||
    !(Symbol.asyncIterator in body)
  ) {
    throw new Error('S3 GetObject returned a non-streaming body');
  }
  return body as AsyncIterable<Uint8Array>;
}

async function closeBody(body: unknown): Promise<void> {
  if (body && typeof body === 'object') {
    const candidate = body as { destroy?: () => void; cancel?: () => unknown };
    if (typeof candidate.destroy === 'function') candidate.destroy();
    else if (typeof candidate.cancel === 'function') await candidate.cancel();
  }
}

export interface S3ReadOnlyObjectStoreOptions {
  bucket: string;
  expectedBucketOwner: string;
  region: string;
  client?: S3Sender;
}

export class S3ReadOnlyObjectStore implements ReadOnlyObjectStore {
  readonly #bucket: string;
  readonly #expectedBucketOwner: string;
  readonly #client: S3Sender;

  constructor(options: S3ReadOnlyObjectStoreOptions) {
    if (!options.bucket) throw new TypeError('bucket is required');
    if (!/^\d{12}$/.test(options.expectedBucketOwner)) {
      throw new TypeError(
        'expectedBucketOwner must be a 12-digit AWS account ID',
      );
    }
    if (!options.region) throw new TypeError('region is required');
    this.#bucket = options.bucket;
    this.#expectedBucketOwner = options.expectedBucketOwner;
    this.#client = options.client ?? new S3Client({ region: options.region });
  }

  async *listKeys(
    prefix: string,
    options: { signal?: AbortSignal } = {},
  ): AsyncIterable<string> {
    let continuationToken: string | undefined;
    const tokens = new Set<string>();

    while (true) {
      const response = await this.#client.send(
        new ListObjectsV2Command({
          Bucket: this.#bucket,
          Prefix: prefix,
          ContinuationToken: continuationToken,
          ExpectedBucketOwner: this.#expectedBucketOwner,
        }),
        { abortSignal: options.signal },
      );
      for (const object of response.Contents ?? []) {
        if (typeof object.Key === 'string') yield object.Key;
      }
      if (!response.IsTruncated) return;

      const next = response.NextContinuationToken;
      if (typeof next !== 'string' || next.length === 0) {
        throw new Error(
          'malformed S3 pagination: truncated page has no next token',
        );
      }
      if (tokens.has(next)) {
        throw new Error('malformed S3 pagination: repeated continuation token');
      }
      tokens.add(next);
      continuationToken = next;
    }
  }

  async open(
    key: string,
    options: { maxBytes: number; signal?: AbortSignal },
  ): Promise<ObjectRead> {
    validateMaxBytes(options.maxBytes);
    const response = await this.#client.send(
      new GetObjectCommand({
        Bucket: this.#bucket,
        Key: key,
        ExpectedBucketOwner: this.#expectedBucketOwner,
      }),
      { abortSignal: options.signal },
    );
    const rawBody = response.Body;
    if (
      response.ContentLength !== undefined &&
      response.ContentLength > options.maxBytes
    ) {
      await closeBody(rawBody);
      throw new RangeError('object exceeds maxBytes (declared ContentLength)');
    }
    let source: AsyncIterable<Uint8Array>;
    try {
      source = asAsyncBody(rawBody);
    } catch (error) {
      await closeBody(rawBody);
      throw error;
    }
    let closed = false;
    const close = async (): Promise<void> => {
      if (!closed) {
        closed = true;
        await closeBody(rawBody);
      }
    };
    const body = (async function* (): AsyncIterable<Uint8Array> {
      let count = 0;
      try {
        for await (const value of source) {
          if (!(value instanceof Uint8Array)) {
            throw new TypeError('object stream yielded a non-Uint8Array chunk');
          }
          const remaining = options.maxBytes - count;
          if (value.byteLength > remaining) {
            if (remaining > 0) {
              yield value.subarray(0, remaining);
              count += remaining;
            }
            throw new RangeError('object exceeds maxBytes (streamed body)');
          }
          count += value.byteLength;
          yield value;
        }
      } finally {
        await close();
      }
    })();
    return { body, close };
  }
}
