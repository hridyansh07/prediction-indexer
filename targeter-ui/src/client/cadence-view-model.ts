import type {
  CadenceFreshnessState,
  UniverseCadenceRun,
} from '../event-universe';

export const cadenceStatusLabel = (state: CadenceFreshnessState) =>
  `CADENCE ${state.toUpperCase()}`;

export const cadenceRunEmptyMessage = (run: UniverseCadenceRun) =>
  run.input_complete
    ? 'This complete indexed run contains no selected bundles.'
    : 'This run had incomplete input, so Universe admitted no selections.';
