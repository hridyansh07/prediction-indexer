import assert from 'node:assert/strict';
import {
  chmod,
  mkdtemp,
  readFile,
  readdir,
  stat,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { RustV1Decoder, type DecodeExpectation } from '../src/index.js';

const ZERO = '0'.repeat(64);
const ONE = '1'.repeat(64);
const expectation: DecodeExpectation = {
  stored: { sha256: ZERO, byteLength: 7 },
  logical: { sha256: ONE, byteLength: 3, lineCount: 1 },
  maxDecodedBytes: 3,
};

async function fixture(): Promise<{
  root: string;
  binary: string;
  input: string;
}> {
  const root = await mkdtemp(join(tmpdir(), 'node-decoder-test-'));
  const binary = join(root, 'fake-decoder');
  const input = join(root, 'input.json');
  await writeFile(
    binary,
    `#!/usr/bin/env node
const fs = require('node:fs');
const args = process.argv.slice(2); const at = n => args[args.indexOf(n) + 1];
const config = JSON.parse(fs.readFileSync(at('--input'), 'utf8'));
if (config.args) fs.writeFileSync(config.args, JSON.stringify(args));
const out = at('--output');
if (config.mode === 'nonzero') { process.stderr.write('x'.repeat(10000)); process.exit(9); }
if (config.mode === 'signal') process.kill(process.pid, 'SIGTERM');
if (config.mode === 'sleep') setTimeout(() => {}, 30000);
if (config.mode === 'missing') process.exit(0);
if (config.mode === 'directory') { fs.mkdirSync(out); process.exit(0); }
if (config.mode === 'symlink') { fs.symlinkSync(at('--input'), out); process.exit(0); }
fs.writeFileSync(out, config.mode === 'wrong-size' ? 'xx' : 'abc', { mode: 0o600 });
if (config.mode === 'delay') setTimeout(() => process.exit(0), 100);
`,
  );
  await chmod(binary, 0o700);
  return { root, binary, input };
}

async function runMode(
  mode: string,
  callback = async (path: string) => readFile(path, 'utf8'),
) {
  const f = await fixture();
  await writeFile(f.input, JSON.stringify({ mode }));
  const decoder = new RustV1Decoder({ binaryPath: f.binary, tempRoot: f.root });
  return {
    f,
    promise: decoder.withDecodedFile(f.input, expectation, callback),
  };
}

test('validates constructor and expectations before creating quarantine or spawning', async () => {
  assert.throws(() => new RustV1Decoder({ binaryPath: 'decoder' }), /absolute/);
  assert.throws(() => new RustV1Decoder({ binaryPath: '' }), /nonempty/);
  const f = await fixture();
  const decoder = new RustV1Decoder({ binaryPath: f.binary, tempRoot: f.root });
  for (const bad of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
    await assert.rejects(
      decoder.withDecodedFile(
        f.input,
        { ...expectation, maxDecodedBytes: bad },
        () => {},
      ),
      /safe nonnegative/,
    );
  }
  await assert.rejects(
    decoder.withDecodedFile(
      f.input,
      {
        ...expectation,
        logical: { ...expectation.logical, sha256: 'A'.repeat(64) },
      },
      () => {},
    ),
    /canonical/,
  );
  await assert.rejects(
    decoder.withDecodedFile(
      f.input,
      { ...expectation, maxDecodedBytes: 2 },
      () => {},
    ),
    /exceeds/,
  );
  assert.deepEqual(await readdir(f.root), ['fake-decoder']);
});

test('passes the closed protocol argument list and exposes output only after zero exit', async () => {
  const f = await fixture();
  const argsFile = join(f.root, 'args.json');
  await writeFile(f.input, JSON.stringify({ mode: 'delay', args: argsFile }));
  let called = false;
  let quarantineMode = 0;
  const result = await new RustV1Decoder({
    binaryPath: f.binary,
    tempRoot: f.root,
  }).withDecodedFile(f.input, expectation, async (path) => {
    called = true;
    quarantineMode = (await stat(join(path, '..'))).mode & 0o777;
    assert.equal(await readFile(path, 'utf8'), 'abc');
    return 42;
  });
  assert.equal(called, true);
  assert.equal(result, 42);
  assert.equal(quarantineMode, 0o700);
  const args = JSON.parse(await readFile(argsFile, 'utf8'));
  assert.deepEqual(args.slice(0, 4), [
    '--protocol-version',
    '1',
    '--input',
    f.input,
  ]);
  assert.equal(args[4], '--output');
  assert.deepEqual(args.slice(6), [
    '--logical-sha256',
    ONE,
    '--logical-byte-length',
    '3',
    '--logical-line-count',
    '1',
    '--stored-sha256',
    ZERO,
    '--stored-byte-length',
    '7',
  ]);
  assert.equal(
    (await readdir(f.root)).some((name) => name.startsWith('rust-v1-decode-')),
    false,
  );
});

for (const mode of [
  'missing',
  'directory',
  'symlink',
  'wrong-size',
  'nonzero',
  'signal',
]) {
  test(`rejects ${mode} output/process and cleans quarantine`, async () => {
    const { f, promise } = await runMode(mode);
    await assert.rejects(promise);
    assert.equal(
      (await readdir(f.root)).some((name) =>
        name.startsWith('rust-v1-decode-'),
      ),
      false,
    );
  });
}

test('bounds stderr diagnostics', async () => {
  const { promise } = await runMode('nonzero');
  await assert.rejects(
    promise,
    (error) => error instanceof Error && error.message.length < 4200,
  );
});

test('cleans after callback failure and spawn error', async () => {
  const { f, promise } = await runMode('ok', () => {
    throw new Error('callback');
  });
  await assert.rejects(promise, /callback/);
  assert.equal(
    (await readdir(f.root)).some((name) => name.startsWith('rust-v1-decode-')),
    false,
  );
  const decoder = new RustV1Decoder({
    binaryPath: join(f.root, 'absent'),
    tempRoot: f.root,
  });
  await assert.rejects(
    decoder.withDecodedFile(f.input, expectation, () => {}),
    /ENOENT/,
  );
  assert.equal(
    (await readdir(f.root)).some((name) => name.startsWith('rust-v1-decode-')),
    false,
  );
});

test('abort terminates, awaits settlement, skips callback, and cleans', async () => {
  const f = await fixture();
  await writeFile(f.input, JSON.stringify({ mode: 'sleep' }));
  const controller = new AbortController();
  let called = false;
  const promise = new RustV1Decoder({
    binaryPath: f.binary,
    tempRoot: f.root,
  }).withDecodedFile(
    f.input,
    expectation,
    () => {
      called = true;
    },
    { signal: controller.signal },
  );
  setTimeout(() => controller.abort(), 50);
  await assert.rejects(promise, { name: 'AbortError' });
  assert.equal(called, false);
  assert.equal(
    (await readdir(f.root)).some((name) => name.startsWith('rust-v1-decode-')),
    false,
  );
});
