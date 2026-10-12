// Loading research files: every byte is checked against its manifest before use (spec §5.1).
import { useQuery } from '@tanstack/react-query';
import {
  verifyIdentity,
  type AvailabilityDocument,
  type CorpusManifest,
  type CurrentPointer,
  type GameDocument,
  type GridDocument,
  type Identity,
  type OverlayDocument,
  type PackManifest,
  type RawTile,
} from './research-model';

const configured = import.meta.env.VITE_CORPUS_BASE_URL as string | undefined;
export const CORPUS_BASE = (
  configured ?? (import.meta.env.DEV ? '/corpus' : '')
).replace(/\/$/, '');
const IMMUTABLE = Number.POSITIVE_INFINITY;
const decoder = new TextDecoder();

export class CorpusUnavailable extends Error {}

async function fetchBytes(path: string): Promise<Uint8Array> {
  if (!CORPUS_BASE)
    throw new CorpusUnavailable(
      'VITE_CORPUS_BASE_URL is not configured for this build.',
    );
  const response = await fetch(`${CORPUS_BASE}/${path}`, {
    credentials: 'omit',
  });
  if (!response.ok)
    throw new CorpusUnavailable(`${path}: HTTP ${response.status}`);
  return new Uint8Array(await response.arrayBuffer());
}

/** Fetch ``path`` and refuse it unless its bytes match ``identity``. */
export async function fetchVerified(
  path: string,
  identity: Identity,
): Promise<Uint8Array> {
  const bytes = await fetchBytes(path);
  await verifyIdentity(bytes, identity, path);
  return bytes;
}

function parse<T>(bytes: Uint8Array): T {
  return JSON.parse(decoder.decode(bytes)) as T;
}

export interface Corpus {
  pointer: CurrentPointer;
  manifest: CorpusManifest;
}

export async function loadCorpus(): Promise<Corpus> {
  // current.json is the one mutable file and the root of the identity chain.
  const pointer = parse<CurrentPointer>(
    await fetchBytes(`current.json?t=${Date.now()}`),
  );
  const manifest = parse<CorpusManifest>(
    await fetchVerified(pointer.manifest_key, {
      sha256: pointer.manifest_sha256,
      byte_length: pointer.manifest_byte_length,
    }),
  );
  return { pointer, manifest };
}

export function corpusFile(corpus: Corpus, name: string): Promise<Uint8Array> {
  const identity = corpus.manifest.files[name];
  if (!identity)
    throw new CorpusUnavailable(
      `${name} is not in corpus version ${corpus.pointer.version}`,
    );
  return fetchVerified(`${corpus.pointer.version}/${name}`, identity);
}

export async function loadPack(
  corpus: Corpus,
  bundleId: string,
): Promise<PackManifest> {
  const entry = corpus.manifest.events[bundleId];
  if (!entry)
    throw new CorpusUnavailable(
      `No research pack for ${bundleId} in version ${corpus.pointer.version}`,
    );
  return parse<PackManifest>(
    await fetchVerified(`${corpus.pointer.version}/${entry.pack}`, entry),
  );
}

export async function packFile<T>(
  corpus: Corpus,
  pack: PackManifest,
  name: string,
): Promise<T> {
  const identity = pack.files[name];
  if (!identity)
    throw new CorpusUnavailable(
      `${name} is not in the pack for ${pack.bundle_id}`,
    );
  return parse<T>(
    await fetchVerified(
      `${corpus.pointer.version}/events/${pack.bundle_id}/${name}`,
      identity,
    ),
  );
}

export const researchKeys = {
  corpus: ['research', 'corpus'] as const,
  pack: (version: string, bundle: string) =>
    ['research', version, bundle, 'manifest'] as const,
  file: (version: string, bundle: string, name: string) =>
    ['research', version, bundle, name] as const,
};

export function useCorpus() {
  return useQuery({
    queryKey: researchKeys.corpus,
    queryFn: loadCorpus,
    staleTime: 60_000,
    retry: false,
  });
}

export function usePack(corpus: Corpus | undefined, bundleId: string) {
  return useQuery({
    queryKey: researchKeys.pack(corpus?.pointer.version ?? '', bundleId),
    queryFn: () => loadPack(corpus!, bundleId),
    enabled: corpus !== undefined,
    staleTime: IMMUTABLE,
    retry: false,
  });
}

export function usePackFile<T>(
  corpus: Corpus | undefined,
  pack: PackManifest | undefined,
  name: string | null,
) {
  return useQuery({
    queryKey: researchKeys.file(
      corpus?.pointer.version ?? '',
      pack?.bundle_id ?? '',
      name ?? '',
    ),
    queryFn: () => packFile<T>(corpus!, pack!, name!),
    enabled: corpus !== undefined && pack !== undefined && name !== null,
    staleTime: IMMUTABLE,
    gcTime: 10 * 60_000,
    retry: false,
  });
}

export type {
  AvailabilityDocument,
  GameDocument,
  GridDocument,
  OverlayDocument,
  RawTile,
};
