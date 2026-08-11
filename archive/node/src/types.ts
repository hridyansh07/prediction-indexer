export interface ByteIdentity {
  sha256: string;
  byteLength: number;
}

export interface ObjectRead {
  body: AsyncIterable<Uint8Array>;
  close(): void | Promise<void>;
}

export interface ReadOnlyObjectStore {
  listKeys(
    prefix: string,
    options?: { signal?: AbortSignal },
  ): AsyncIterable<string>;
  open(
    key: string,
    options: { maxBytes: number; signal?: AbortSignal },
  ): Promise<ObjectRead>;
}

export function validateMaxBytes(maxBytes: number): void {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) {
    throw new RangeError('maxBytes must be a safe nonnegative integer');
  }
}
