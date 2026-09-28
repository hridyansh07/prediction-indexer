import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { getAddress } from 'viem';
import {
  createUniverseClient,
  UniverseApiError,
  UniverseClient,
  type ReplaySession,
} from './universe-client';

type EthereumProvider = {
  request(args: { method: string; params?: unknown[] }): Promise<unknown>;
};

declare global {
  interface Window {
    ethereum?: EthereumProvider;
  }
}

export type SiweConfig = {
  domain: string;
  uri: string;
  statement: string;
  chainId: number;
};

type UniverseAuthValue = {
  client: UniverseClient;
  session: ReplaySession | null;
  expired: boolean;
  signingIn: boolean;
  error: string | null;
  signIn(): Promise<void>;
  signOut(): Promise<void>;
  clearSession(expired?: boolean): void;
};

const UniverseAuthContext = createContext<UniverseAuthValue | null>(null);

export function buildSiweMessage(
  address: string,
  nonce: string,
  issuedAt: string,
  config: SiweConfig,
) {
  return `${config.domain} wants you to sign in with your Ethereum account:\n${address}\n\n${config.statement}\n\nURI: ${config.uri}\nVersion: 1\nChain ID: ${config.chainId}\nNonce: ${nonce}\nIssued At: ${issuedAt}`;
}

export function normalizeWalletAddress(address: string) {
  try {
    return getAddress(address);
  } catch {
    throw new Error('The wallet provided an invalid Ethereum account.');
  }
}

export function validateSiweConfig(config: SiweConfig) {
  let uri: URL;
  try {
    uri = new URL(config.uri);
  } catch {
    throw new Error('Replay SIWE URI build configuration is invalid');
  }
  if (
    !config.domain ||
    !config.statement ||
    !Number.isSafeInteger(config.chainId) ||
    config.chainId <= 0 ||
    !['http:', 'https:'].includes(uri.protocol) ||
    uri.host !== config.domain ||
    uri.username ||
    uri.password ||
    uri.search ||
    uri.hash ||
    uri.toString() !== config.uri
  )
    throw new Error(
      'Replay SIWE public build configuration must use the exact canonical UI domain and URI (including its trailing slash)',
    );
  return config;
}

export function signInErrorMessage(cause: unknown) {
  return cause instanceof UniverseApiError && cause.status === 401
    ? 'Sign-in was rejected. The wallet may not be allowlisted, the signature may not match, or the SIWE domain/URI build configuration may differ from Universe.'
    : cause instanceof Error
      ? cause.message
      : 'Wallet sign-in failed.';
}

export function readSiweConfig(): SiweConfig {
  const domain = import.meta.env.VITE_REPLAY_SIWE_DOMAIN as string | undefined;
  const uri = import.meta.env.VITE_REPLAY_SIWE_URI as string | undefined;
  const statement = import.meta.env.VITE_REPLAY_SIWE_STATEMENT as
    | string
    | undefined;
  const chain = import.meta.env.VITE_REPLAY_SIWE_CHAIN_ID as string | undefined;
  const chainId = Number(chain);
  if (
    !domain ||
    !uri ||
    !statement ||
    !Number.isSafeInteger(chainId) ||
    chainId <= 0
  )
    throw new Error('Replay SIWE public build configuration is incomplete');
  return validateSiweConfig({ domain, uri, statement, chainId });
}

export function UniverseAuthProvider({
  children,
}: {
  children: React.ReactNode;
}) {
  const [session, setSession] = useState<ReplaySession | null>(null);
  const [expired, setExpired] = useState(false);
  const [signingIn, setSigningIn] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const sessionRef = useRef<ReplaySession | null>(null);
  sessionRef.current = session;
  const clearSession = useCallback((wasExpired = false) => {
    sessionRef.current = null;
    setSession(null);
    setExpired(wasExpired);
  }, []);
  const client = useMemo(
    () =>
      createUniverseClient(
        () => sessionRef.current?.token ?? null,
        () => clearSession(true),
      ),
    [clearSession],
  );

  useEffect(() => {
    if (!session) return;
    const expiresAt = Date.parse(session.expires_at);
    const delay = expiresAt - Date.now();
    if (!Number.isFinite(expiresAt) || delay <= 0) {
      clearSession(true);
      return;
    }
    const timer = window.setTimeout(() => clearSession(true), delay);
    return () => window.clearTimeout(timer);
  }, [clearSession, session]);

  const signIn = useCallback(async () => {
    setSigningIn(true);
    setError(null);
    try {
      const provider = window.ethereum;
      if (!provider)
        throw new Error('Install or enable an Ethereum wallet to sign in.');
      const config = readSiweConfig();
      const chain = await provider.request({ method: 'eth_chainId' });
      if (
        typeof chain !== 'string' ||
        Number.parseInt(chain, 16) !== config.chainId
      )
        throw new Error(
          `Switch your wallet to chain ${config.chainId} and try again.`,
        );
      const accounts = await provider.request({
        method: 'eth_requestAccounts',
      });
      if (!Array.isArray(accounts) || typeof accounts[0] !== 'string')
        throw new Error('The wallet did not provide an account.');
      const address = normalizeWalletAddress(accounts[0]);
      const { nonce } = await client.nonce();
      const message = buildSiweMessage(
        address,
        nonce,
        new Date().toISOString(),
        config,
      );
      const signature = await provider.request({
        method: 'personal_sign',
        params: [message, address],
      });
      if (typeof signature !== 'string')
        throw new Error('The wallet did not return a signature.');
      const next = await client.signIn(message, signature);
      sessionRef.current = next;
      setSession(next);
      setExpired(false);
    } catch (cause) {
      setError(signInErrorMessage(cause));
      throw cause;
    } finally {
      setSigningIn(false);
    }
  }, [client]);

  const signOut = useCallback(async () => {
    try {
      if (sessionRef.current) await client.logout();
    } catch (cause) {
      if (!(cause instanceof UniverseApiError && cause.status === 401))
        throw cause;
    } finally {
      clearSession(false);
    }
  }, [client, clearSession]);

  const value = useMemo(
    () => ({
      client,
      session,
      expired,
      signingIn,
      error,
      signIn,
      signOut,
      clearSession,
    }),
    [client, session, expired, signingIn, error, signIn, signOut, clearSession],
  );
  return (
    <UniverseAuthContext.Provider value={value}>
      {children}
    </UniverseAuthContext.Provider>
  );
}

export function useUniverseAuth() {
  const value = useContext(UniverseAuthContext);
  if (!value) throw new Error('UniverseAuthProvider is missing');
  return value;
}
