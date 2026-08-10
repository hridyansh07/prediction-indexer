export type Json =
  | null
  | boolean
  | number
  | string
  | Json[]
  | { [key: string]: Json };
export interface RunView {
  runId: string;
  generatedAt: string;
  inputComplete: boolean;
  strategyVersion: string | number | null;
  report: Record<string, any>;
  summary: RunSummary;
}
export interface RunSummary {
  candidates: number;
  selected: number;
  rejected: number;
  targets: number;
  catalogsComplete: number;
  catalogsTotal: number;
  rejectionReasons: Record<string, number>;
}
export interface Snapshot {
  generatedAt: string;
  stale: boolean;
  refreshing: boolean;
  lastSuccessfulRefresh: string | null;
  lastRefreshError: string | null;
  refreshSeconds: number;
  expectedRunSeconds: number;
  source: 's3' | 'fixture';
  runs: RunView[];
  config: {
    label: string;
    version: string | number | null;
    versionMatchesRunIds: string[];
    value: Json;
  };
}
