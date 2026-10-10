"""Falsifying offline contracts for the independent implementation review."""
from copy import deepcopy
from fractions import Fraction
import json
from unittest.mock import patch

from replay.preparation import encoded
from replay.streams.protocol import ProtocolError
from replay.tests import test_milestone_leader_follower as base
from replay.tests.test_milestone_leader_follower import model, game_file, M, ladder
import unittest
from replay.strategies.milestone_leader_follower.output import validate_content
from replay.strategies.milestone_leader_follower.contract import policy
from replay.strategies.milestone_leader_follower.history import History


class RepairTests(unittest.TestCase):
    setUp = base.LeaderTests.setUp
    h = base.LeaderTests.h
    rewrite = base.LeaderTests.rewrite
    def audit(self, h):
        return validate_content(h.output, h.strategy.snapshot, json.loads((h.output / 'manifest.json').read_bytes()))

    def test_required_entry_cannot_be_deleted_with_its_entire_ledger(self):
        h = self.h(); h.prime(); h.leaders(14, 600); h.target(16, 610, 620); h.finish()
        self.rewrite(h, 'actions.ndjson', lambda rows: rows.clear())
        self.rewrite(h, 'positions.ndjson', lambda rows: rows.clear())
        with self.assertRaisesRegex(ProtocolError, 'required'):
            self.audit(h)

    def test_required_exit_cannot_be_deleted_and_relabelled_censored(self):
        h = self.h(); h.prime(); h.leaders(14, 600); h.target(16, 610, 620); h.finish()
        self.rewrite(h, 'actions.ndjson', lambda rows: rows.__setitem__(slice(None), [r for r in rows if r['kind'] not in ('EXIT_LATCHED', 'CLOSED')]))
        def censor(rows):
            for r in rows:
                r.update(state='CENSORED', exit_reason=None, exit_time_ns=None, closed_ns=None, holding_atoms=r['opening_atoms'], residual_basis=r['opening_cost'], closed_lot_pnl='0', holding_charge='0', hypothetical_closed_pnl=None, holding_cost_adjusted_closed_pnl=None, liquidation_mark='61', mark_status='PRICED')
        self.rewrite(h, 'positions.ndjson', censor)
        with self.assertRaisesRegex(ProtocolError, 'required'):
            self.audit(h)

    def test_book_only_game_release_cannot_exit_or_schedule_decisions(self):
        g = game_file(); g['segments'] = [{'index': 1, 'start_ns': None, 'start_estimated': False, 'end_ns': 16, 'settled_ns': None, 'winner': 'home', 'details': {}}]; g['match'] = None
        models = [model(role, '30') for role in ('target_only', 'target_milestones', 'leader_milestones')]
        for m in models[1:]:
            m['cohorts'] = [{**m['cohorts'][0], 'phase': 'unavailable', 'score_quality': 'book_only', 'prefix_quality': 'book_only'}]
        h = self.h(game=g, changes={'cohort': 'book_only_comparison', 'models': models, 'exit_horizon_ns': '30'})
        h.prime(); h.leaders(14, 600); h.group(18); h.finish()
        self.assertFalse(any(r.get('reason') in ('SEGMENT_END', 'MATCH_END') for r in h.records('actions.ndjson')))
        self.assertFalse(any(r['released'] for r in h.records('decisions.ndjson')))

    def test_overlapping_cohorts_rejected_before_consuming_books(self):
        h = self.h(); p = deepcopy(h.leader_config['policy']); p['models'][2]['cohorts'].append(deepcopy(p['models'][2]['cohorts'][0]))
        with self.assertRaisesRegex(ProtocolError, 'overlap'):
            policy(p)

    def test_history_overflow_is_visible_unavailability_then_warmup(self):
        history = History(30, 3)
        for t, v in enumerate((Fraction(1, 2), Fraction(3, 5), Fraction(7, 10), Fraction(4, 5)), 1):
            history.observe(t, v)
        self.assertIsNone(history.endpoints(4, 2))
        self.assertTrue(history.overflowed)
        self.assertIsNotNone(history.endpoints(6, 2))

    def test_history_overflow_interval_survives_full_independent_reader(self):
        h = self.h(changes={'history_changes':2}); h.prime(); h.leaders(14,600); h.leaders(15,610); h.leaders(16,620); h.group(19); h.finish()
        self.assertTrue(any(o['status'] == 'HISTORY_BOUND_EXCEEDED' for d in h.records('decisions.ndjson') for o in d['observations']))

    def test_smaller_qualifying_size_is_not_vetoed_by_larger_stop_loss(self):
        h = self.h(); h.prime()
        ladder(h, 12, 'polymarket:123', bids=((520, 10*M), (400, 100*M)), asks=((530, 200*M),))
        h.leaders(14, 690); h.finish()
        opens = [r for r in h.records('actions.ndjson') if r['kind'] == 'OPEN' and r['role'] == 'leader_milestones']
        self.assertEqual([r['order']['quantity_atoms'] for r in opens], [str(10*M)])

    def test_thin_depth_still_measures_price_forecast_error(self):
        h = self.h(); h.prime(); h.leaders(14, 600)
        h.target(22, 610, 620, M // 2); h.finish()
        outcomes = [r for r in h.records('predictions.ndjson') if r['role'] == 'leader_milestones' and r['decision_ns'] == '14']
        self.assertTrue(outcomes)
        self.assertEqual(outcomes[0]['forecast_error'], '0')
        self.assertIsNone(outcomes[0]['counterfactual_net'])

    def test_owned_original_outcome_space_settles_after_scope_departure(self):
        g = game_file(); g['segments'] = [{'index': i, 'start_ns': None, 'start_estimated': False, 'end_ns': 26, 'settled_ns': None, 'winner': 'home', 'details': {}} for i in (1, 2)]; g['match'] = {'end_ns': 26, 'winner': 'home', 'score': {'home': 2, 'away': 0}}
        h = self.h(scopes='uncaptured', game=g, changes={'exit_horizon_ns': '30', 'models': [model(role, '30') for role in ('target_only', 'target_milestones', 'leader_milestones')], 'exit_on_next_segment_end': False, 'exit_on_match_end': False, 'settlement': {'mode': 'NORMAL_RESOLUTION_SCENARIO', 'delay_ns': '2', 'immediate_availability': False, 'evidence': []}})
        h.prime(); h.leaders(14, 600); h.group(29)
        h.finish()
        settled = [r for r in h.records('actions.ndjson') if r['kind'] == 'SETTLED']
        self.assertEqual([r['time_ns'] for r in settled], ['28'])

    def test_unsupported_overlap_is_model_unavailable_even_without_history(self):
        h = self.h(); h.prime(); h.group(13)
        state = h.strategy.knowledge.state(h.strategy.timeline.view,13)
        _, routes = h.strategy._routes(13,state)
        route = next(r for r in routes if r['relation'] == 'IDENTITY')
        route = {**route,'relation':'OVERLAP','proof':None}
        result = h.strategy._observation(h.strategy.policy['models'][2],tuple(route['target']),route,13,state)
        self.assertEqual(result['status'],'MODEL_UNAVAILABLE')

    def test_price_only_forecast_when_no_size_is_executable_at_entry(self):
        h = self.h(); h.prime(); h.target(12,quantity=M//2); h.leaders(14,600); h.target(22,610,620,M//2); h.finish()
        forecasts = [r for r in h.records('predictions.ndjson') if r['role'] == 'leader_milestones' and r['decision_ns'] == '14' and r['target'] == ['polymarket:123','outcome']]
        self.assertEqual(len(forecasts),1)
        self.assertEqual(forecasts[0]['forecast_error'],'0')
        self.assertIsNone(forecasts[0]['counterfactual_net'])

    def test_reader_rejects_writer_suppression_of_a_required_economic_signal(self):
        h = self.h(); h.prime(); h.leaders(14,600); h.target(16,610,620); h.finish()
        def suppress(rows):
            for d in rows:
                for o in d['observations']:
                    if o['role'] == 'leader_milestones' and o['signal'] is True:
                        o.update(selected=None,signal=None,displacement=None,prediction_id=None,status='HISTORY_UNAVAILABLE')
        self.rewrite(h,'decisions.ndjson',suppress)
        with self.assertRaises(ProtocolError):
            self.audit(h)

    def test_large_native_ladder_is_not_repeated_in_every_order_proof(self):
        h = self.h(); h.prime(); h.leaders(14, 600)
        ladder(h, 16, 'polymarket:123', bids=tuple((520-i, M) for i in range(100)), asks=tuple((530+i, M) for i in range(100)))
        h.finish()
        raw = (h.output / 'decisions.ndjson').read_bytes()
        self.assertLess(len(raw.splitlines()[-1]), 20000)


if __name__ == '__main__':
    import unittest
    unittest.main()
