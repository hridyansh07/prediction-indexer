import { spawn } from 'node:child_process';
import { chmod, lstat, mkdtemp, rm } from 'node:fs/promises';
import { isAbsolute, join } from 'node:path';
import { tmpdir } from 'node:os';

export interface StoredIdentity {
  sha256: string;
  byteLength: number;
}

export interface LogicalIdentity extends StoredIdentity {
  lineCount: number;
}

export interface DecodeExpectation {
  stored: StoredIdentity;
  logical: LogicalIdentity;
  maxDecodedBytes: number;
}

export interface RustV1DecoderOptions {
  binaryPath: string;
  tempRoot?: string;
}

const SHA256 = /^[0-9a-f]{64}$/;
const STDERR_LIMIT = 4096;

function validateInteger(
  value: unknown,
  name: string,
): asserts value is number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new TypeError(`${name} must be a safe nonnegative integer`);
  }
}

function validateHash(value: unknown, name: string): asserts value is string {
  if (typeof value !== 'string' || !SHA256.test(value)) {
    throw new TypeError(`${name} must be canonical lowercase SHA-256 hex`);
  }
}

function validateExpectation(expectation: DecodeExpectation): void {
  if (
    expectation === null ||
    typeof expectation !== 'object' ||
    expectation.stored === null ||
    typeof expectation.stored !== 'object' ||
    expectation.logical === null ||
    typeof expectation.logical !== 'object'
  ) {
    throw new TypeError(
      'expectation must contain stored and logical identities',
    );
  }
  validateHash(expectation.stored.sha256, 'stored.sha256');
  validateInteger(expectation.stored.byteLength, 'stored.byteLength');
  validateHash(expectation.logical.sha256, 'logical.sha256');
  validateInteger(expectation.logical.byteLength, 'logical.byteLength');
  validateInteger(expectation.logical.lineCount, 'logical.lineCount');
  validateInteger(expectation.maxDecodedBytes, 'maxDecodedBytes');
  if (expectation.logical.byteLength > expectation.maxDecodedBytes) {
    throw new RangeError('logical.byteLength exceeds maxDecodedBytes');
  }
}

function abortError(): Error {
  const error = new Error('The operation was aborted');
  error.name = 'AbortError';
  return error;
}

/** Quarantines output from the protocol-v1 Rust decoder until fully verified. */
export class RustV1Decoder {
  readonly #binaryPath: string;
  readonly #tempRoot: string;

  constructor({ binaryPath, tempRoot }: RustV1DecoderOptions) {
    if (
      typeof binaryPath !== 'string' ||
      binaryPath.length === 0 ||
      !isAbsolute(binaryPath)
    ) {
      throw new TypeError('binaryPath must be a nonempty absolute path');
    }
    if (
      tempRoot !== undefined &&
      (typeof tempRoot !== 'string' || tempRoot.length === 0)
    ) {
      throw new TypeError('tempRoot must be a nonempty path when supplied');
    }
    this.#binaryPath = binaryPath;
    this.#tempRoot = tempRoot ?? tmpdir();
  }

  async withDecodedFile<T>(
    storedPath: string,
    expectation: DecodeExpectation,
    use: (decodedPath: string) => T | Promise<T>,
    options: { signal?: AbortSignal } = {},
  ): Promise<T> {
    validateExpectation(expectation);
    if (typeof storedPath !== 'string' || storedPath.length === 0) {
      throw new TypeError('storedPath must be a nonempty path');
    }
    if (typeof use !== 'function')
      throw new TypeError('use must be a function');
    if (options.signal?.aborted) throw abortError();

    const quarantine = await mkdtemp(join(this.#tempRoot, 'rust-v1-decode-'));
    try {
      await chmod(quarantine, 0o700);
      const outputPath = join(quarantine, 'decoded.ndjson');
      const args = [
        '--protocol-version',
        '1',
        '--input',
        storedPath,
        '--output',
        outputPath,
        '--logical-sha256',
        expectation.logical.sha256,
        '--logical-byte-length',
        String(expectation.logical.byteLength),
        '--logical-line-count',
        String(expectation.logical.lineCount),
        '--stored-sha256',
        expectation.stored.sha256,
        '--stored-byte-length',
        String(expectation.stored.byteLength),
      ];

      if (options.signal?.aborted) throw abortError();
      const child = spawn(this.#binaryPath, args, {
        shell: false,
        stdio: ['ignore', 'ignore', 'pipe'],
      });
      let stderr = Buffer.alloc(0);
      child.stderr.on('data', (chunk: Buffer) => {
        if (stderr.length < STDERR_LIMIT) {
          stderr = Buffer.concat([
            stderr,
            chunk.subarray(0, STDERR_LIMIT - stderr.length),
          ]);
        }
      });

      let aborted = false;
      const onAbort = (): void => {
        aborted = true;
        child.kill('SIGTERM');
      };
      options.signal?.addEventListener('abort', onAbort, { once: true });
      try {
        const result = await new Promise<{
          code: number | null;
          signal: NodeJS.Signals | null;
        }>((resolve, reject) => {
          child.once('error', reject);
          child.once('close', (code, signal) => resolve({ code, signal }));
        });
        if (aborted || options.signal?.aborted) throw abortError();
        if (result.signal !== null) {
          throw new Error(
            `Rust decoder terminated by signal ${result.signal}: ${stderr.toString('utf8')}`,
          );
        }
        if (result.code !== 0) {
          throw new Error(
            `Rust decoder exited with code ${result.code}: ${stderr.toString('utf8')}`,
          );
        }
      } finally {
        options.signal?.removeEventListener('abort', onAbort);
      }

      const metadata = await lstat(outputPath);
      if (!metadata.isFile() || metadata.isSymbolicLink()) {
        throw new Error('Rust decoder output is not a regular file');
      }
      if (metadata.size !== expectation.logical.byteLength) {
        throw new Error(
          `Rust decoder output size ${metadata.size} does not match expected ${expectation.logical.byteLength}`,
        );
      }
      return await use(outputPath);
    } finally {
      await rm(quarantine, { recursive: true, force: true });
    }
  }
}
