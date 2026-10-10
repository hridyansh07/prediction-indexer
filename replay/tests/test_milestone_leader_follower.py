"""Small offline native-book contracts; no live venues or retained evidence."""
from copy import deepcopy
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, Mock

from replay.preparation import encoded, prepare
from replay.streams.protocol import ProtocolError
from replay.tests.test_preparation_outcomes import document
from replay.tests.test_game_state_sdk import game_file, policy as game_policy
from replay.tests.test_same_venue_complement import strategy_config
from replay.tests.test_bundle_coverage import Harness as BaseHarness
from replay.tests.economic_scenarios import ladder, operations, M
from replay.strategies.milestone_leader_follower import build, read_provisional
from replay.strategies.milestone_leader_follower.contract import policy as parse_policy
from replay.strategies.milestone_leader_follower.history import History, Knowledge
from replay.strategies.milestone_leader_follower.economics import Account, Capacity


def model(role, horizon='8'):
    base = {'game': None, 'phase': 'unavailable' if role == 'target_only' else 'pre_match', 'score_quality': 'book_only' if role == 'target_only' else 'complete', 'prefix_quality': 'book_only' if role == 'target_only' else 'complete', 'home': None, 'away': None, 'elapsed_min_ns': '0', 'elapsed_max_ns': None, 'beta_L': '1' if role == 'leader_milestones' else '0', 'beta_T': '0', 'intercept': '0'}
    cohorts = [base] if role == 'target_only' else [{**base, 'phase': phase} for phase in ('pre_match', 'in_segment', 'between_segments', 'finished')]
    return {'version': 1, 'role': role, 'provenance': 'ASSUMED_RESPONSE_MODEL', 'assumption': 'Synthetic transparent response, not fitted or measured.', 'horizon_ns': horizon, 'features': ['delta_L', 'delta_T', 'released_milestones'], 'relations': ['IDENTITY', 'COMPLEMENT'], 'cohorts': cohorts, 'fallback': None, 'training': None}


def configuration(base, root, changes=None, game=None, fee_mode=None):
    configured = strategy_config(base, root, known=True)
    snapshot = json.loads((root / 'context/context.json').read_bytes())
    value = game_file() if game is None else game
    value['segments'] = [] if game is None else value['segments']
    value['match'] = None if game is None else value['match']
    path = root / 'game_state.json'; path.write_bytes(encoded(value) + b'\n')
    game_sha = hashlib.sha256(path.read_bytes()).hexdigest()
    fees = configured['fees']
    if fee_mode == 'cash_charge':
        from dataclasses import replace
        from replay.fees.artifacts import load_catalog, build_catalog
        from replay.fees.schedules import Catalog, Polymarket, Rounding
        from replay.fees.domain import Rate
        catalog = load_catalog(fees['catalog_directory'])
        schedules = tuple(replace(s, model=Polymarket(Rate.parse('0.0075'), 0, True, Rounding.EXACT), fee_scale=5) if s.scope.venue.value == 'polymarket' else s for s in catalog.schedules)
        charged = Catalog.build(schedules)
        source_bytes = b'synthetic zero-fee contract, not live evidence'
        source_map = {source.sha256: source_bytes for s in schedules for source in s.sources}
        fees['catalog_directory'] = str(build_catalog(root / 'charged-fees', charged, source_map)); fees['catalog_identity'] = charged.identity
    if fee_mode == 'missing_leader':
        fees['instrument_bindings'] = [x for x in fees['instrument_bindings'] if not x['instrument'].startswith('kalshi:')]
    p = {'version': 1, 'game': game_policy(path, game_sha, required=True, segment_start='at_segment_end'), 'cohort': 'milestone_primary', 'quantity_grid': ['1', '10', '100'], 'window_ns': '2', 'history_ns': '30', 'history_changes': 100000, 'exit_horizon_ns': '8', 'decision_delay_ns': '0', 'minimum_leader_move': '0.02', 'minimum_forecast_margin': '0.05', 'minimum_forecast_return_bps': '0', 'take_profit_fraction': '0.02', 'stop_loss_fraction': '0.05', 'exit_on_next_segment_end': True, 'exit_on_match_end': True, 'cooldown_after_close_ns': '3', 'rearm_false_ns': '1', 'max_entries_per_event': 1, 'max_open_per_target': 1, 'capacity_mode': 'CUMULATIVE_DISPLAY_CAP', 'trading_status_mode': 'TRADABLE_IF_USABLE_SCENARIO', 'valuation': {'mode': 'PARITY_SCENARIO', 'weights': {'kalshi': '1', 'polymarket': '1'}}, 'initial_cash': [{'venue': venue, 'asset': fees['assets'][venue], 'amount': '1000'} for venue in sorted(fees['assets'])], 'transaction_budget': '1000', 'event_budget': '1000', 'maximum_outstanding_cost': '1000', 'maximum_concurrent_positions': 10, 'holding_rate_per_ns': '0', 'settlement': {'mode': 'UNRESOLVED', 'delay_ns': '0', 'immediate_availability': False, 'evidence': []}, 'models': [model(role) for role in ('target_only', 'target_milestones', 'leader_milestones')], 'rule_bindings': [{'instrument': item['instrument'], 'orientation': item['orientation'], 'rule_sha256': 'a'*64, 'alignment_id': 'synthetic-reviewed-normal-resolution', 'price_increment_atoms': '1', 'quantity_increment_atoms': '1'} for item in snapshot['plans']], 'max_books': 256, 'max_routes': 4096, 'models_tried': 3, 'thresholds_tried': 1}
    if changes:
        p.update(changes)
    if p['cohort'] == 'book_only_comparison':
        p['game']['required'] = False
    return {**base, 'fees': fees, 'source_revision': 'a'*64, 'policy': p}


class Harness(BaseHarness):
    def __init__(self, root, changes=None, game=None, scopes=False, fee_mode=None, vendor_detail=None, outcomes=None, pin=None, end_ns=None):
        def factory(context):
            self.leader_config = configuration(dict(context['config']), root, changes, game, fee_mode)
            return build({**context, 'config': self.leader_config})
        def prepare_outcomes(*args, **kwargs):
            return prepare(*args, **kwargs, outcomes=lambda _: outcomes or document())
        from replay.tests.test_preparation import detail, config
        supplied_detail = vendor_detail or detail()
        def supplied_config():
            value = config(supplied_detail)
            if end_ns is not None:
                value['end_ns'] = str(end_ns); value['occurrences'][0]['end_ns'] = str(end_ns)
            return value
        with patch('replay.tests.test_bundle_coverage.detail', return_value=supplied_detail), patch('replay.tests.test_bundle_coverage.config', side_effect=supplied_config), patch('replay.tests.test_bundle_coverage.build', side_effect=factory), patch('replay.tests.test_bundle_coverage.prepare', side_effect=prepare_outcomes):
            super().__init__(root, mixed=True, scopes=scopes, pin=pin)

    def plan_index(self, instrument, orientation='outcome'):
        return next(i for i, p in enumerate(self.initial['plans']) if p['instrument'] == instrument and p['orientation'] == orientation)

    def leaders(self, time, midpoint=510, quantity=100*M):
        # Exactly .02-wide two-sided Kalshi book.
        ladder(self, time, 'kalshi:series', bids=((midpoint-10, quantity),))
        ladder(self, time, 'kalshi:series', 'complement', bids=((1000-midpoint-10, quantity),))

    def target(self, time, bid=520, ask=530, quantity=100*M):
        ladder(self, time, 'polymarket:123', bids=((bid, quantity),), asks=((ask, quantity),))

    def prime(self):
        self.window(); self.leaders(11); self.target(11)

    def finish(self):
        self.terminal(); self.decoder.finish(); self.strategy.finish()
        return read_provisional(self.output, self.root / 'context', expected_sha256=self.sha)

    def records(self, filename):
        from replay.strategies.milestone_leader_follower.output import rows
        manifest = json.loads((self.output / 'manifest.json').read_bytes())
        return list(rows(self.output,filename,manifest['files'][filename]))


class LeaderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup); self.root = Path(self.tmp.name)

    def h(self, **kwargs):
        h = Harness(self.root, **kwargs)
        for writer in h.strategy.writers.values():
            self.addCleanup(writer.stream.close)
        return h

    def test_positive_forecast_opens_and_actual_take_profit_closes(self):
        h = self.h(); h.prime(); h.leaders(14, 600); h.target(16, 610, 620)
        result = h.finish()
        row = next(x for x in result['summary']['rows'] if x['role'] == 'leader_milestones')
        self.assertEqual(row['opens'], 1); self.assertEqual(row['closed_position_net_total'], '8')
        positions = h.records('positions.ndjson'); position = next(x for x in positions if x['role'] == 'leader_milestones')
        self.assertEqual(position['exit_reason'], 'TAKE_PROFIT'); self.assertEqual(position['state'], 'CLOSED')
        self.assertEqual(position['opening_cost'], '53'); self.assertEqual(position['hypothetical_closed_pnl'], '8')

    def test_isolated_scenario_cannot_open_additive_positions(self):
        pricing = Mock(); pricing.plans = {('polymarket:123', 'outcome'): {'venue': 'polymarket'}}; pricing.weight.return_value = Fraction(1)
        p = {'initial_cash': [{'venue': 'polymarket', 'amount': '1000'}], 'capacity_mode': 'ISOLATED_NONADDITIVE', 'max_entries_per_event': 10, 'maximum_concurrent_positions': 10, 'transaction_budget': '1000', 'event_budget': '1000', 'maximum_outstanding_cost': '1000'}
        account = Account(p, pricing)
        self.assertEqual(account.gate({'key': ['polymarket:123', 'outcome'], 'cash': '-10'}), 'ISOLATED_DETECTION_ONLY')

    def test_git_sha1_revision_is_usable(self):
        h = self.h(); config = deepcopy(h.leader_config); config['source_revision'] = 'b'*40
        from replay.strategies.milestone_leader_follower.contract import configuration as validate
        self.assertEqual(validate(config, h.strategy.snapshot)['source_revision'], 'b'*40)

    def test_same_time_invalidation_requires_full_window_warmup(self):
        h = self.h(); h.prime(); h.leaders(14, 600)
        ladder(h, 14, 'kalshi:series', why={'kind': 'connection_closed'})
        h.leaders(14, 600); h.group(15); h.finish()
        opens = [x for x in h.records('actions.ndjson') if x['kind'] == 'OPEN' and x['role'] == 'leader_milestones']
        self.assertEqual(len(opens), 0)

    def test_partial_exit_does_not_retry_on_unrelated_leader_timer(self):
        h = self.h(); h.prime(); h.leaders(14, 600)
        h.target(22, 520, 530, 40*M); h.leaders(23, 620); h.group(24); h.finish()
        actions = [x for x in h.records('actions.ndjson') if x['role'] == 'leader_milestones' and x['kind'] in ('CLOSED', 'PARTIAL_EXIT', 'EXIT_UNAVAILABLE')]
        self.assertEqual([(x['kind'], x['time_ns']) for x in actions], [('PARTIAL_EXIT', '22')])

    def test_delayed_same_innovation_reprices_quantity_and_opens_smaller(self):
        h = self.h(changes={'decision_delay_ns': '1'}); h.prime(); h.leaders(14, 600); h.target(15, quantity=10*M); h.group(16); h.finish()
        actions = h.records('actions.ndjson'); opened = [x for x in actions if x['kind'] == 'OPEN' and x['role'] == 'leader_milestones']
        self.assertEqual(len(opened), 1); self.assertEqual(opened[0]['time_ns'], '15'); self.assertEqual(opened[0]['order']['quantity_atoms'], str(10*M))

    def test_delayed_price_revision_cancels_without_inheriting_new_signal(self):
        h = self.h(changes={'decision_delay_ns': '2'}); h.prime(); h.leaders(14, 600); h.leaders(15, 610); h.group(17); h.finish()
        actions = [x for x in h.records('actions.ndjson') if x['role'] == 'leader_milestones']
        self.assertEqual(sum(x['kind'] == 'SIGNALLED' for x in actions), 1); self.assertEqual(sum(x['kind'] == 'OPEN' for x in actions), 0)
        self.assertTrue(any(x['kind'] == 'CANCELLED' for x in actions))

    def test_nonzero_fee_hand_calculation_is_650_actual_closed_pnl(self):
        h = self.h(fee_mode='cash_charge'); h.prime(); h.leaders(14, 600); h.target(16, 610, 620); result = h.finish()
        row = next(x for x in result['summary']['rows'] if x['role'] == 'leader_milestones')
        self.assertEqual(row['closed_position_net_total'], '13/2')
        position = next(x for x in h.records('positions.ndjson') if x['role'] == 'leader_milestones')
        self.assertEqual(position['opening_cost'], '215/4')

    def test_missing_leader_economic_binding_is_inadmissible(self):
        h = self.h(fee_mode='missing_leader'); h.prime(); h.leaders(14, 600); h.finish()
        self.assertEqual(sum(x['kind'] == 'OPEN' for x in h.records('actions.ndjson')), 0)

    def test_initial_loss_gate_refuses_positive_forecast(self):
        h = self.h(); h.prime(); h.target(12, bid=490); h.leaders(14, 600); h.finish()
        self.assertEqual(sum(x['kind'] == 'OPEN' for x in h.records('actions.ndjson')), 0)
        observations = [o for d in h.records('decisions.ndjson') for o in d['observations'] if o['role'] == 'leader_milestones' and o['selected'] is not None]
        self.assertTrue(any(Fraction(o['sizes'][o['selected']]['forecast_net']) > 0 and not o['signal'] for o in observations))

    def test_spread_consumes_move_and_does_not_open(self):
        h = self.h(fee_mode='cash_charge'); h.prime(); h.target(12, bid=500, ask=580); h.leaders(14, 600); h.finish()
        self.assertEqual(sum(x['kind'] == 'OPEN' for x in h.records('actions.ndjson')), 0)

    def test_insufficient_cash_skip_is_one_attempt_on_surviving_signal(self):
        h = self.h(changes={'initial_cash': [{'venue': 'kalshi', 'asset': {'kind': 'USD', 'ledger': 'synthetic', 'token': 'kalshi'}, 'amount': '1000'}, {'venue': 'polymarket', 'asset': {'kind': 'USDC', 'ledger': 'synthetic', 'token': 'polymarket'}, 'amount': '0'}]})
        h.prime(); h.leaders(14, 600); h.target(15, quantity=200*M); h.group(16); h.finish()
        actions = [x for x in h.records('actions.ndjson') if x['role'] == 'leader_milestones']
        self.assertEqual(sum(x['kind'] == 'SIGNALLED' for x in actions), 1); self.assertEqual(sum(x['reason'] == 'INSUFFICIENT_CASH' for x in actions), 1)

    def test_partial_then_larger_display_closes_only_eligible_residual(self):
        h = self.h(); h.prime(); h.leaders(14, 600); h.target(22, quantity=40*M); h.target(24, quantity=100*M); result = h.finish()
        row = next(x for x in result['summary']['rows'] if x['role'] == 'leader_milestones')
        self.assertEqual(row['partial_exits'], 1); self.assertEqual(row['full_exits'], 1); self.assertEqual(row['closed_position_net_total'], '-1')

    def test_no_depth_exit_is_censored_and_forecast_never_credited(self):
        h = self.h(); h.prime(); h.leaders(14, 600)
        ladder(h, 22, 'polymarket:123', bids=(), asks=((530, 100*M),)); h.finish()
        p = next(x for x in h.records('positions.ndjson') if x['role'] == 'leader_milestones')
        self.assertEqual(p['state'], 'CENSORED'); self.assertEqual(p['holding_atoms'], str(100*M)); self.assertIsNone(p['hypothetical_closed_pnl']); self.assertEqual(p['mark_status'], 'NO_EXIT_DEPTH')

    def test_unknown_winner_preserves_feasible_space_and_visible_missing_cohort(self):
        game = game_file(); game['segments'][0]['winner'] = None; game['match'] = None
        h = self.h(game=game); h.prime(); h.group(21); h.finish()
        decisions = [x for x in h.records('decisions.ndjson') if int(x['time_ns']) >= 20]
        self.assertEqual(decisions[0]['knowledge']['score_quality'], 'unknown'); self.assertEqual(decisions[0]['knowledge']['prefix'], [])
        self.assertTrue(any(o['status'] == 'MODEL_UNAVAILABLE' for o in decisions[0]['observations'] if o['role'] != 'target_only'))

    def test_normal_resolution_posts_only_at_declared_availability(self):
        game = game_file(); game['segments'] = [{'index': i, 'start_ns': None, 'start_estimated': False, 'end_ns': 20, 'settled_ns': None, 'winner': 'home', 'details': {}} for i in (1, 2)]; game['match'] = {'end_ns': 20, 'winner': 'home', 'score': {'home': 2, 'away': 0}}
        h = self.h(game=game, changes={'exit_on_next_segment_end': False, 'exit_on_match_end': False, 'settlement': {'mode': 'NORMAL_RESOLUTION_SCENARIO', 'delay_ns': '2', 'immediate_availability': False, 'evidence': []}})
        h.prime(); h.leaders(14, 600); h.group(23); result = h.finish()
        settled = [x for x in h.records('actions.ndjson') if x['kind'] == 'SETTLED']
        self.assertEqual([x['time_ns'] for x in settled], ['22']); self.assertEqual(settled[0]['payout'], '100')
        row = next(x for x in result['summary']['rows'] if x['role'] == 'leader_milestones'); self.assertEqual(row['closed_position_net_total'], '47')

    def test_pinned_settlement_does_not_credit_before_acquisition(self):
        h = self.h(changes={'settlement': {'mode': 'PINNED_SETTLEMENT', 'delay_ns': '0', 'immediate_availability': True, 'evidence': [{'instrument': 'polymarket:123', 'orientation': 'outcome', 'payout': '1', 'available_ns': '12', 'evidence_sha256': 'b'*64}]}})
        h.prime(); h.leaders(14, 600); h.finish()
        actions = h.records('actions.ndjson'); self.assertEqual([x['kind'] for x in actions if x['time_ns'] == '14'], ['SIGNALLED', 'OPEN', 'SETTLED']); self.assertEqual([x['time_ns'] for x in actions if x['kind'] == 'SETTLED'], ['14'])

    def test_required_unavailable_file_fails_before_output_or_books(self):
        from replay.economic_sdk.game import GameStateUnavailable
        game = game_file(); game.update(state='unavailable', reason='no_fetch', segments=[], match=None, source=None, segment_kind=None)
        with self.assertRaises(GameStateUnavailable):
            Harness(self.root, game=game)
        self.assertEqual(list((self.root / 'output').iterdir()), [])

    def test_budget_and_capacity_do_not_reset_on_identical_snapshot(self):
        capacity = Capacity('CUMULATIVE_DISPLAY_CAP'); source = ('kalshi:x', 'complement', 'bid'); levels = ((500, 100),)
        capacity.consume({'source': list(source), 'source_taken': [[500, 60]]})
        self.assertEqual(capacity.eligible(source, levels), ((500, 40),)); self.assertEqual(capacity.eligible(source, ((490, 100),)), ((490, 40),)); self.assertEqual(capacity.eligible(source, ((490, 140),)), ((490, 80),))

    def test_saved_arm_cannot_bypass_current_continuous_false_duration(self):
        h = self.h(changes={'max_entries_per_event': 2, 'rearm_false_ns': '5'}); target = ('polymarket:123', 'outcome'); key = ('leader_milestones', target)
        h.strategy.signals[key] = {'truth': False, 'false_since': 29, 'armed': True, 'last_innovation': [['kalshi:series', 'outcome'], '14', 1, 'proof'], 'last_attempt': 14, 'pending': None, 'episode': None}
        observation = {'role': key[0], 'target': list(target), 'signal': True, 'innovation': [['kalshi:series', 'outcome'], '30', 2, 'proof'], 'route': {'target': list(target)}, 'selected': 0, 'sizes': [{'buy': {'quantity_atoms': str(100*M)}}]}
        h.strategy.current_predictions = {target: {}}
        with patch.object(h.strategy, '_enter') as enter:
            h.strategy._signal(observation, 30, {}, set())
        enter.assert_not_called()

    def test_reentry_innovation_must_postdate_previous_attempt(self):
        h = self.h(changes={'max_entries_per_event': 2}); target = ('polymarket:123', 'outcome'); key = ('leader_milestones', target)
        h.strategy.signals[key] = {'truth': False, 'false_since': 28, 'armed': True, 'last_innovation': [['kalshi:series', 'outcome'], '14', 1, 'proof'], 'last_attempt': 19, 'pending': None, 'episode': None}
        observation = {'role': key[0], 'target': list(target), 'signal': True, 'innovation': [['kalshi:series', 'outcome'], '18', 2, 'proof'], 'route': {'target': list(target)}, 'selected': 0, 'sizes': [{'buy': {'quantity_atoms': str(100*M)}}]}
        h.strategy.current_predictions = {target: {}}
        with patch.object(h.strategy, '_enter') as enter:
            h.strategy._signal(observation, 30, {}, set())
        enter.assert_not_called()

    def test_zero_delay_revalidates_selected_size_without_shrinking(self):
        h = self.h(changes={'max_entries_per_event': 2}); h.prime(); h.leaders(14, 600); h.target(16, 610, 620, 200*M); h.target(19, 520, 530, 140*M); h.leaders(20, 690); h.finish()
        actions = [x for x in h.records('actions.ndjson') if x['role'] == 'leader_milestones']
        self.assertEqual(sum(x['kind'] == 'OPEN' for x in actions), 1)
        self.assertTrue(any(x['kind'] == 'SKIPPED' and x['reason'] == 'CAPACITY_OR_PREDICATE' and x['time_ns'] == '20' for x in actions))

    def test_reentry_requires_false_cooldown_and_a_later_price_change(self):
        h = self.h(changes={'max_entries_per_event': 2}); h.prime(); h.leaders(14, 600); h.target(16, 610, 620, 200*M); h.target(19, 520, 530, 200*M); h.leaders(20, 690); result = h.finish()
        row = next(x for x in result['summary']['rows'] if x['role'] == 'leader_milestones')
        self.assertEqual(row['opens'], 2); self.assertEqual(len([x for x in h.records('positions.ndjson') if x['role'] == 'leader_milestones']), 2)
        self.assertEqual([x['time_ns'] for x in h.records('actions.ndjson') if x['kind'] == 'OPEN' and x['role'] == 'leader_milestones'], ['14', '20'])

    def test_dynamic_sibling_identity_warms_after_released_split_prefix(self):
        from replay.tests.test_preparation import detail
        from analysis.claims import claim_id
        d = detail(); doc = document(); shape = doc['spaces'][0]['space_shape_id']; keys = ['seq:AHH', 'seq:HAH']; cid = claim_id(keys, shape)
        doc['claims'].append({'claim_id': cid, 'space_shape_id': shape, 'outcome_keys': keys}); doc['claims'].sort(key=lambda x: x['claim_id'])
        doc['markets'].append({'market_id': 'kalshi:map3', 'venue': 'kalshi', 'market_type': 'map_moneyline', 'market_status': 'open', 'subscription_ids': ['map3'], 'outcome_labels': ['Alpha'], 'mask_status': 'MASKED', 'reason': None, 'claims': [{'claim_key': 'claim=0', 'claim_id': cid}], 'tokens': [{'subscription_id': 'map3', 'claim_key': 'claim=0', 'negated': False}]}); doc['markets'].sort(key=lambda x: x['market_id'])
        d['context']['markets'].append({'target_id': 'kalshi:map3', 'venue': 'kalshi', 'selected': True}); d['context']['markets'].sort(key=lambda x: x['target_id'])
        d['context']['targets'].append({'venue': 'kalshi', 'target_id': 'kalshi:map3', 'canonical_class': 'esports.map_moneyline', 'subscription_ids': ['map3'], 'source_ref': 'kalshi:event'}); d['context']['targets'].sort(key=lambda x: x['target_id'])
        game = game_file(); game['segments'] = [{'index': i, 'start_ns': None, 'start_estimated': False, 'end_ns': 20, 'settled_ns': None, 'winner': 'home' if i == 1 else 'away', 'details': {}} for i in (1, 2)]; game['match'] = None; game['market_types'] = ['map_moneyline', 'series_moneyline']
        h = self.h(game=game, vendor_detail=d, outcomes=doc); h.window(); h.target(11)
        ladder(h, 11, 'kalshi:map3', bids=((500, 100*M),)); ladder(h, 11, 'kalshi:map3', 'complement', bids=((480, 100*M),)); h.group(21)
        ladder(h, 23, 'kalshi:map3', bids=((590, 100*M),)); ladder(h, 23, 'kalshi:map3', 'complement', bids=((390, 100*M),)); h.finish()
        opens = [x for x in h.records('actions.ndjson') if x['kind'] == 'OPEN' and x['role'] == 'leader_milestones']
        self.assertEqual([x['time_ns'] for x in opens], ['23']); self.assertIn(opens[0]['order']['lineage']['relation'], ('IDENTITY', 'COMPLEMENT'))
        d20 = next(x for x in h.records('decisions.ndjson') if x['time_ns'] == '20'); self.assertEqual(d20['knowledge']['prefix'], [0, 1]); self.assertEqual(len(d20['knowledge']['released_results']), 2)
        self.assertTrue(any(x['status'] == 'MODEL_UNAVAILABLE' for x in h.records('decisions.ndjson')[0]['admission']['unsupported_routes']))

    def test_book_only_comparison_is_separate_and_ignores_released_score(self):
        models = [model(role) for role in ('target_only', 'target_milestones', 'leader_milestones')]
        for m in models[1:]:
            m['cohorts'] = [{**m['cohorts'][0], 'phase': 'unavailable', 'score_quality': 'book_only', 'prefix_quality': 'book_only'}]
        # The game input is available, but its facts may not enter this cohort.
        h = self.h(changes={'cohort': 'book_only_comparison', 'models': models})
        h.prime(); h.leaders(14, 600); h.finish()
        self.assertTrue(all(x['knowledge']['score_quality'] == 'book_only' for x in h.records('decisions.ndjson')))

    def test_reader_rejects_rehashed_history_and_unknown_fields(self):
        h = self.h(); h.prime(); h.leaders(14, 600); h.finish()
        def change(records):
            o = next(o for row in records for o in row['observations'] if o['role'] == 'leader_milestones' and o['selected'] is not None)
            o['leader_endpoint']['previous_ns'] = '10'
        self.rewrite(h, 'decisions.ndjson', change)
        with self.assertRaisesRegex(ProtocolError, 'history'):
            read_provisional(h.output, self.root / 'context', expected_sha256=h.sha)

    def test_past_event_models_pin_identical_splits_and_reject_leakage(self):
        h = self.h(); value = deepcopy(h.leader_config)
        from replay.strategies.milestone_leader_follower.contract import configuration as validate
        training = {'training_events': [{'event_id': 'earlier-a', 'outcome_available_ns': '5', 'horizon_end_ns': '5'}], 'validation_events': [{'event_id': 'earlier-b', 'outcome_available_ns': '6', 'horizon_end_ns': '6'}], 'cutoff_ns': '7', 'feature_definition': 'committed_asof_midpoint_changes_and_released_milestones_v1', 'target_definition': 'target_bid_ladder_displacement_at_horizon_v1', 'hyperparameters_sha256': 'b'*64, 'eligibility_sha256': 'c'*64}
        for m in value['policy']['models']:
            m.update(provenance='PAST_EVENT_FIT', training=deepcopy(training))
        validate(value, h.strategy.snapshot)
        value['policy']['models'][0]['training']['training_events'][0]['event_id'] = document()['event_id']
        for m in value['policy']['models'][1:]:
            m['training'] = deepcopy(value['policy']['models'][0]['training'])
        with self.assertRaisesRegex(ProtocolError, 'held out'):
            validate(value, h.strategy.snapshot)

    def rewrite(self, h, filename, mutate):
        records = h.records(filename); mutate(records)
        payload = b''.join(encoded(h.strategy.codec.encode(x,new=False,full_books=True)) + b'\n' for x in records)
        (h.output / filename).write_bytes(payload)
        manifest = json.loads((h.output / 'manifest.json').read_bytes())
        manifest['files'][filename] = {'sha256': hashlib.sha256(payload).hexdigest(), 'byte_length': len(payload), 'records': len(records)}
        (h.output / 'manifest.json').write_bytes(encoded(manifest) + b'\n')
        receipt = json.loads((h.output / 'content_receipt.json').read_bytes())
        from replay.preparation import digest
        receipt['semantic_sha256'] = digest(manifest)
        (h.output / 'content_receipt.json').write_bytes(encoded(receipt) + b'\n')

    def test_reader_recomputes_open_revalidation_after_rehashing(self):
        h = self.h(changes={'decision_delay_ns': '1'}); h.prime(); h.leaders(14, 600); h.group(15); h.target(17, 610, 620); h.finish()
        self.rewrite(h, 'actions.ndjson', lambda records: next(x for x in records if x['kind'] == 'OPEN')['revalidation'].update(displacement='999'))
        with self.assertRaisesRegex(ProtocolError, 'response-model'):
            read_provisional(h.output, self.root / 'context', expected_sha256=h.sha)

    def test_documented_config_and_bench_templates_have_closed_shapes(self):
        from replay.bench.specs import validate_run_spec
        directory = Path(__file__).parents[1] / 'strategies/milestone_leader_follower'
        config = json.loads((directory / 'config.example.json').read_bytes()); parse_policy(config['policy'])
        spec = json.loads((directory / 'bench.example.json').read_bytes()); validate_run_spec(spec, check_paths=False)
        self.assertEqual(spec['groups'][0]['factory'], 'replay.strategies.milestone_leader_follower:build')
        self.assertEqual(config['policy']['cohort'], 'milestone_primary')
        import tomllib
        project = tomllib.loads((directory.parents[2] / 'pyproject.toml').read_text())
        self.assertEqual(set(project['tool']['setuptools']['package-data']['replay.strategies.milestone_leader_follower']), {'README.md', 'SPEC.md', 'config.example.json', 'bench.example.json'})

    def test_known_outcome_purchase_uses_first_payoff_release(self):
        from types import SimpleNamespace
        h = self.h(changes={'settlement': {'mode': 'NORMAL_RESOLUTION_SCENARIO', 'delay_ns': '10', 'immediate_availability': False, 'evidence': []}})
        competitors = {'home': {'participant': 0}, 'away': {'participant': 1}}
        for time, index, winner in ((20, 1, 'home'), (25, 2, 'away')):
            h.strategy.knowledge.release([SimpleNamespace(kind='segment_end', release_ns=time, source_ns=time, index=index, value=(winner, {}))], competitors)
        keys = ('seq:HH', 'seq:HAH', 'seq:HAA', 'seq:AHH', 'seq:AHA', 'seq:AA')
        space = SimpleNamespace(coverage='EXHAUSTIVE', scope='series', keys=keys)
        position = {'shape_id': 'shape', 'claim_keys': frozenset(k for k in keys if k[4] == 'H'), 'target': ('polymarket:123', 'outcome'), 'opened': 26, 'settlement_due': None}
        with patch('replay.strategies.milestone_leader_follower.strategy.outcome_scope', return_value=SimpleNamespace(spaces={'shape': space})):
            h.strategy._resolution(position, 26)
        self.assertEqual(position['settlement_due'], 30)

    def test_bench_factory_completed_reader_and_check_are_real(self):
        from replay import supervisor
        from replay.bench.inside import execute, import_callable
        from replay.bench.specs import supervisor_config
        from replay.tests.test_bench import run_spec, resolved
        from replay.tests.test_supervisor import metadata_pin, config as base_config
        pin = metadata_pin(); h = self.h(pin=pin)
        spec = run_spec(self.root, self.root / 'context')
        group = {'name': 'coverage', 'factory': 'replay.strategies.milestone_leader_follower:build', 'revision': 'a'*40, 'config': h.leader_config, 'reader': 'replay.strategies.milestone_leader_follower:read_completed', 'checks': ['replay.strategies.milestone_leader_follower:check'], 'compare': {'rows': 'rows', 'key': ['role', 'event_id']}}
        spec['game_state_path'] = str(self.root / 'game_state.json')
        group['config']['policy']['game']['input']['path'] = '{game_state}'
        spec['groups'] = [group]
        # Temporary host paths stand in for container mounts; the shared bench
        # mount/token contracts are separately exercised by its own suite.
        spec = resolved(spec); spec['runtime']['run_id'] = h.context['run_id']
        base = base_config(); base['transport'].update(plans=h.initial['plans'], start_ns='10', end_ns='40', inputs=[pin]); spec['runtime']['base_run_config'] = base
        expected = supervisor_config(spec)
        h.strategy.config = spec['groups'][0]['config']; h.strategy.policy = h.strategy.config['policy']
        from replay.game_state import load as actual_game_load
        def mounted_game(path, *args):
            return actual_game_load(self.root / 'game_state.json' if str(path) == '/bench/game_state.json' else path, *args)
        def complete(config, directory, redis_url):
            self.assertEqual(config, expected); identity = supervisor.identity(config); h.strategy.binding['identity'] = identity
            h.window(); ladder(h, 11, 'kalshi:series', bids=((50, 100),)); ladder(h, 11, 'kalshi:series', 'complement', bids=((48, 100),)); ladder(h, 11, 'polymarket:123', bids=((52, 100),), asks=((53, 100),)); ladder(h, 14, 'kalshi:series', bids=((59, 100),)); ladder(h, 14, 'kalshi:series', 'complement', bids=((39, 100),)); ladder(h, 16, 'polymarket:123', bids=((61, 100),), asks=((62, 100),)); h.finish()
            participant = directory / h.context['attempt_id'] / 'coverage'; participant.mkdir(parents=True); h.output.rename(participant / 'output')
            supervisor.write_json_durable(directory / 'run.json', config)
            supervisor.write_json_durable(participant / 'complete.json', {'version': 1, 'identity': identity, 'attempt': h.context['attempt_id'], 'group': 'coverage', 'terminal': h.seq})
            supervisor.write_json_durable(participant.parent / 'result.json', {'version': 1, 'identity': identity, 'attempt': h.context['attempt_id'], 'outcome': 'success', 'fatal': False, 'progress': h.seq, 'terminal': h.seq, 'participants': {'publisher': 0, 'coverage': 0}})
            supervisor.write_json_durable(directory / 'SUCCESS.json', {'version': 1, 'identity': identity, 'attempt': h.context['attempt_id'], 'terminal': h.seq, 'outputs': {'coverage': h.context['attempt_id'] + '/coverage/output'}})
            return supervisor.read_success(directory)
        self.assertIs(import_callable(group['factory']), build)
        with patch.object(supervisor, '_strict_metadata_preflight'), patch('replay.strategies.milestone_leader_follower.output.load_game', side_effect=mounted_game):
            code, result = execute(spec, self.root, redis_url='redis://unused', supervisor_run=complete)
        self.assertEqual((code, result['status']), (0, 'SUCCESS'), result['error'])
        self.assertTrue(result['groups'][0]['checks'][group['checks'][0]])
        summary = json.loads((self.root / 'groups/coverage/summary.json').read_bytes())
        self.assertEqual(next(x for x in summary['rows'] if x['role'] == 'leader_milestones')['closed_position_net_total'], '8')

    def test_scope_departure_preserves_censored_holding_and_ends_detection(self):
        h = self.h(scopes='uncaptured', changes={'exit_horizon_ns': '30', 'models': [model(role, '30') for role in ('target_only', 'target_milestones', 'leader_milestones')]})
        h.prime(); h.leaders(14, 600); h.leaders(22, 690); h.group(24); h.finish()
        p = next(x for x in h.records('positions.ndjson') if x['role'] == 'leader_milestones')
        self.assertEqual(p['state'], 'CENSORED'); self.assertEqual(p['holding_atoms'], str(100*M)); self.assertEqual(p['mark_status'], 'UNUSABLE')
        episodes = [x for x in h.records('episodes.ndjson') if x['role'] == 'leader_milestones' and x['start_ns'] == '22' and x['target'] == ['polymarket:123', 'outcome']]
        self.assertEqual([(x['end_ns'], x['end_reason']) for x in episodes], [('23', 'UNAVAILABLE')])

    def test_unknown_score_cannot_use_complete_fallback(self):
        from replay.strategies.milestone_leader_follower.history import cohort
        m = model('leader_milestones'); m['fallback'] = 0
        state = {'game': 'lol', 'phase': 'between_segments', 'score_quality': 'unknown', 'prefix_quality': 'incomplete', 'score': [0, 0], 'elapsed_ns': '0'}
        self.assertIsNone(cohort(m, state))

    def test_holding_charge_stop_has_observer_deadline(self):
        h = self.h(changes={'holding_rate_per_ns': '0.01'}); h.prime(); h.leaders(14, 600); h.finish()
        latched = [x for x in h.records('actions.ndjson') if x['kind'] == 'EXIT_LATCHED']
        self.assertEqual([(x['time_ns'], x['reason']) for x in latched], [('18', 'STOP_LOSS')])

    def test_reader_rejects_early_rehashed_resolution_credit(self):
        game = game_file(); game['segments'] = [{'index': i, 'start_ns': None, 'start_estimated': False, 'end_ns': 20, 'settled_ns': None, 'winner': 'home', 'details': {}} for i in (1, 2)]; game['match'] = {'end_ns': 20, 'winner': 'home', 'score': {'home': 2, 'away': 0}}
        h = self.h(game=game, changes={'exit_on_next_segment_end': False, 'exit_on_match_end': False, 'settlement': {'mode': 'NORMAL_RESOLUTION_SCENARIO', 'delay_ns': '2', 'immediate_availability': False, 'evidence': []}})
        h.prime(); h.leaders(14, 600); h.group(21); h.finish()
        self.rewrite(h, 'actions.ndjson', lambda records: next(x for x in records if x['kind'] == 'SETTLED').update(time_ns='21'))
        self.rewrite(h, 'positions.ndjson', lambda records: next(x for x in records if x['role'] == 'leader_milestones').update(exit_time_ns='21', closed_ns='21'))
        with self.assertRaisesRegex(ProtocolError, 'availability'):
            read_provisional(h.output, self.root / 'context', expected_sha256=h.sha)

    def test_reader_rejects_rehashed_invented_censored_mark(self):
        h = self.h(); h.prime(); h.leaders(14, 600); ladder(h, 22, 'polymarket:123', bids=(), asks=((530, 100*M),)); h.finish()
        self.rewrite(h, 'positions.ndjson', lambda records: next(x for x in records if x['role'] == 'leader_milestones').update(liquidation_mark='999', mark_status='PRICED'))
        with self.assertRaisesRegex(ProtocolError, 'mark'):
            read_provisional(h.output, self.root / 'context', expected_sha256=h.sha)

    def test_reader_requires_every_maximal_positive_episode(self):
        h = self.h(); h.prime(); h.leaders(14, 600); h.finish()
        self.assertTrue(h.records('episodes.ndjson'))
        self.rewrite(h, 'episodes.ndjson', lambda records: records.clear())
        with self.assertRaisesRegex(ProtocolError, 'episode'):
            read_provisional(h.output, self.root / 'context', expected_sha256=h.sha)

    def test_reader_rejects_fabricated_repeat_without_false_transition(self):
        h = self.h(changes={'max_entries_per_event': 2}); h.prime(); h.leaders(14, 600); h.leaders(15, 610); h.target(17, 620, 630); h.finish()
        from replay.preparation import digest
        def fabricate(records):
            original = next(x for x in records if x['kind'] == 'SIGNALLED')
            observation = next(o for d in h.records('decisions.ndjson') if d['time_ns'] == '15' for o in d['observations'] if o['role'] == original['role'] and o['target'] == original['route']['target'])
            self.assertTrue(observation['signal'])
            duplicate = deepcopy(original); duplicate.update(time_ns='15', due_ns='15', innovation=observation['innovation'], route=observation['route'], maximum_quantity_atoms=observation['sizes'][observation['selected']]['buy']['quantity_atoms'])
            duplicate['attempt_id'] = digest([h.strategy.experiment, (duplicate['role'], tuple(observation['target'])), 15, observation['innovation']])
            duplicate['baseline_predictions'] = {}
            for d in h.records('decisions.ndjson'):
                if d['time_ns'] == '15':
                    for baseline in d['observations']:
                        if baseline['target'] == observation['target']:
                            duplicate['baseline_predictions'][baseline['role']] = {k: baseline[k] for k in ('model_id', 'prediction_id', 'status', 'displacement', 'signal')}
                            duplicate['baseline_predictions'][baseline['role']]['forecast_net'] = None if baseline['selected'] is None else baseline['sizes'][baseline['selected']]['forecast_net']
            skipped = {k: duplicate[k] for k in ('version', 'role', 'time_ns', 'attempt_id', 'position_id')}; skipped.update(kind='SKIPPED', reason='INSUFFICIENT_CASH')
            records.extend((duplicate, skipped)); records.sort(key=lambda x: int(x['time_ns']))
        self.rewrite(h, 'actions.ndjson', fabricate)
        with self.assertRaisesRegex(ProtocolError, 'false-to-true'):
            read_provisional(h.output, self.root / 'context', expected_sha256=h.sha)

    def test_reader_binds_actual_action_order_time_after_rehashing(self):
        h = self.h(); h.prime(); h.leaders(14, 600); h.finish()
        self.rewrite(h, 'actions.ndjson', lambda records: next(x for x in records if x['kind'] == 'OPEN')['order'].update(time_ns='15'))
        with self.assertRaises(ProtocolError):
            read_provisional(h.output, self.root / 'context', expected_sha256=h.sha)

    def test_history_predecessor_quiet_carry_reset_and_bound(self):
        history = History(30, 3); history.observe(1, Fraction(1, 2)); history.observe(4, Fraction(3, 5)); history.observe(6, Fraction(3, 5))
        self.assertEqual(history.endpoints(6, 2)['previous'], '3/5')
        self.assertEqual(history.endpoints(5, 2)['previous'], '1/2')
        history.observe(7, None); history.observe(7, Fraction(3, 5)); self.assertIsNone(history.endpoints(8, 2))
        history.observe(8, Fraction(7, 10)); history.observe(9, Fraction(4, 5))
        history.observe(10, Fraction(9, 10))
        self.assertTrue(history.overflowed)
        self.assertIsNone(history.endpoints(10, 2))


if __name__ == '__main__':
    unittest.main()
