import type {
  CadenceFreshnessState,
  UniverseCadenceCandidate,
  UniverseCadenceRun,
  UniverseSelectionDetail,
} from '../event-universe';

export type CandidateDecisionState =
  | 'decision-unavailable'
  | 'selected'
  | 'not-selected'
  | 'rejected';

export const cadenceStatusLabel = (state: CadenceFreshnessState) =>
  `CADENCE ${state.toUpperCase()}`;

export const cadenceRunEmptyMessage = (run: UniverseCadenceRun) =>
  run.input_complete
    ? 'This complete indexed run contains no selected bundles.'
    : 'This run had incomplete input, so Universe admitted no selections.';

export const candidateDecisionState = (
  run: UniverseCadenceRun,
  candidate: UniverseCadenceCandidate,
): CandidateDecisionState => {
  if (!run.input_complete) return 'decision-unavailable';
  if (candidate.selected) return 'selected';
  return candidate.eligible ? 'not-selected' : 'rejected';
};

export const selectionDecisionEvidence = (
  run: UniverseCadenceRun,
  selection: UniverseSelectionDetail,
) => {
  if (selection.occurrence_kind === 'retained') {
    return {
      score: run.continuity.bundles.find(
        (bundle) => bundle.bundle_id === selection.bundle_id,
      )?.score,
      candidate: undefined,
    };
  }
  const candidate = run.candidates.find(
    (entry) => entry.bundle_id === selection.bundle_id,
  );
  return { score: candidate?.score, candidate };
};
