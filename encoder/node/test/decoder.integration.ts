import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { RustV1Decoder, type DecodeExpectation } from '../src/index.js';

const binaryPath = process.env.PREDICTION_DECODER_TEST_BINARY;
if (!binaryPath) {
  throw new Error('PREDICTION_DECODER_TEST_BINARY is required');
}

const fixtures = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../fixtures',
);
const expectation: DecodeExpectation = {
  stored: {
    sha256: 'a8ca84b5ff2ab367e16e6918ae9a4d48650f6e1e30d6beb7c5827e9133653896',
    byteLength: 4531,
  },
  logical: {
    sha256: '61d8080cf357099a3dee50a642f0cb20650fe57ca69b5de55ae2b772e32e4dd4',
    byteLength: 121232,
    lineCount: 256,
  },
  maxDecodedBytes: 121232,
};

for (const producer of ['python', 'rust']) {
  test(`decodes the committed ${producer} fixture through the Node adapter`, async () => {
    const decoded = await new RustV1Decoder({ binaryPath }).withDecodedFile(
      join(fixtures, `roundtrip_v1.${producer}.ndjson.zst`),
      expectation,
      readFile,
    );
    assert.deepEqual(
      decoded,
      await readFile(join(fixtures, 'roundtrip_v1.ndjson')),
    );
  });
}
