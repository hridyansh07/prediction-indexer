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
from replay.strategies.milestone_leader_follower.economics import Pricing
from replay.strategies.milestone_leader_follower.strategy import MilestoneLeaderFollower
from replay.tests.test_same_venue_complement import paired_detail
from replay.tests.test_preparation_outcomes import document


class RepairTests(unittest.TestCase):
    setUp = base.LeaderTests.setUp
    h = base.LeaderTests.h
    rewrite = base.LeaderTests.rewrite
    def audit(self, h):
        return validate_content(h.output, h.strategy.snapshot, json.loads((h.output / 'manifest.json').read_bytes()))

    @staticmethod
    def omit_deadline(deadline):
        original = MilestoneLeaderFollower._next
        def next_time(strategy, limit):
            due = original(strategy, limit)
            if due != deadline:
                return due
            previous = strategy.last_decision
            try:
                strategy.last_decision = deadline
                return original(strategy, limit)
            finally:
                strategy.last_decision = previous
        return next_time

    def test_quiet_history_endpoint_closes_honest_episode(self):
        h = self.h(changes={'exit_horizon_ns':'30','models':[model(role,'30') for role in ('target_only','target_milestones','leader_milestones')]})
        h.prime(); h.leaders(14,600); h.group(35); h.finish()
        episodes = [r for r in h.records('episodes.ndjson') if r['role'] == 'leader_milestones' and r['target'] == ['polymarket:123','outcome']]
        self.assertEqual([(r['start_ns'],r['end_ns']) for r in episodes],[('14','16')])

    def test_reader_requires_omitted_quiet_history_endpoint(self):
        h = self.h(changes={'exit_horizon_ns':'30','models':[model(role,'30') for role in ('target_only','target_milestones','leader_milestones')]})
        with patch.object(History,'timers',return_value=[]):
            h.prime(); h.leaders(14,600); h.group(35)
            with self.assertRaisesRegex(ProtocolError,'required history-window decision time'):
                h.finish()

    def test_reader_requires_quiet_history_endpoint_before_terminal(self):
        h = self.h(changes={'exit_horizon_ns':'30','models':[model(role,'30') for role in ('target_only','target_milestones','leader_milestones')]})
        with patch.object(History,'timers',return_value=[]):
            h.prime(); h.leaders(14,600)
            with self.assertRaisesRegex(ProtocolError,'required history-window decision time'):
                h.finish()

    def test_reader_requires_exact_scope_deadline(self):
        h = self.h(scopes='uncaptured',changes={'exit_horizon_ns':'30','models':[model(role,'30') for role in ('target_only','target_milestones','leader_milestones')]})
        with patch.object(MilestoneLeaderFollower,'_next',self.omit_deadline(23)):
            h.prime(); h.leaders(14,600); h.group(35)
            with self.assertRaisesRegex(ProtocolError,'required scope decision time'):
                h.finish()

    def test_reader_requires_relevant_elapsed_model_deadline(self):
        g = game_file(); g['segments'] = [{'index':1,'start_ns':None,'start_estimated':False,'end_ns':18,'settled_ns':None,'winner':'home','details':{}}]; g['match'] = None
        models = [model(role,'30') for role in ('target_only','target_milestones','leader_milestones')]
        m = models[1]
        between = next(c for c in m['cohorts'] if c['phase'] == 'between_segments')
        between['elapsed_max_ns'] = '1'
        m['cohorts'].append({**between,'elapsed_min_ns':'1','elapsed_max_ns':None,'intercept':'0.1'})
        h = self.h(game=g,changes={'exit_horizon_ns':'30','models':models})
        with patch.object(MilestoneLeaderFollower,'_next',self.omit_deadline(19)):
            h.prime(); h.leaders(14,600); h.group(35)
            with self.assertRaisesRegex(ProtocolError,'required model-elapsed decision time'):
                h.finish()

    def test_reader_requires_pending_entry_deadline_before_late_cancellation(self):
        h = self.h(changes={'decision_delay_ns':'1','exit_horizon_ns':'30','models':[model(role,'30') for role in ('target_only','target_milestones','leader_milestones')]})
        with patch.object(MilestoneLeaderFollower,'_next',self.omit_deadline(15)):
            h.prime(); h.leaders(14,600); h.group(35)
            with self.assertRaisesRegex(ProtocolError,'required pending-entry decision time'):
                h.finish()

    def test_reader_requires_holding_charge_stop_deadline(self):
        h = self.h(changes={'holding_rate_per_ns':'0.01'})
        with patch.object(MilestoneLeaderFollower,'_next',self.omit_deadline(18)):
            h.prime(); h.leaders(14,600); h.group(35)
            with self.assertRaisesRegex(ProtocolError,'required holding-cost decision time'):
                h.finish()

    def test_reader_requires_settlement_deadline_before_terminal(self):
        g = game_file(); g['segments'] = [{'index':i,'start_ns':None,'start_estimated':False,'end_ns':18,'settled_ns':None,'winner':'home','details':{}} for i in (1,2)]; g['match'] = {'end_ns':18,'winner':'home','score':{'home':2,'away':0}}
        h = self.h(game=g,changes={'exit_on_next_segment_end':False,'exit_on_match_end':False,'settlement':{'mode':'NORMAL_RESOLUTION_SCENARIO','delay_ns':'2','immediate_availability':False,'evidence':[]},'exit_horizon_ns':'30','models':[model(role,'30') for role in ('target_only','target_milestones','leader_milestones')]})
        with patch.object(MilestoneLeaderFollower,'_next',self.omit_deadline(20)):
            h.prime(); h.leaders(14,600); h.group(19)
            with self.assertRaisesRegex(ProtocolError,'required settlement decision time'):
                h.finish()

    def test_reader_requires_signal_rearm_deadline(self):
        h = self.h(changes={'exit_horizon_ns':'30','models':[model(role,'30') for role in ('target_only','target_milestones','leader_milestones')]})
        with patch.object(MilestoneLeaderFollower,'_next',self.omit_deadline(17)):
            h.prime(); h.leaders(14,600); h.group(35)
            with self.assertRaisesRegex(ProtocolError,'required rearm decision time'):
                h.finish()

    def test_reader_requires_game_release_before_terminal(self):
        g = game_file(); g['segments'] = [{'index':1,'start_ns':None,'start_estimated':False,'end_ns':18,'settled_ns':None,'winner':'home','details':{}}]; g['match'] = None
        h = self.h(game=g,changes={'exit_horizon_ns':'30','models':[model(role,'30') for role in ('target_only','target_milestones','leader_milestones')]})
        with patch.object(MilestoneLeaderFollower,'_next',self.omit_deadline(18)):
            h.prime(); h.leaders(14,600)
            with self.assertRaisesRegex(ProtocolError,'required game-release decision time'):
                h.finish()

    def test_reader_requires_forecast_horizon_decision_even_with_backdated_outcome(self):
        original = MilestoneLeaderFollower._prediction_outcome
        def backdate(strategy, prediction, now, censored=False):
            return original(strategy,prediction,prediction['due'] if prediction['due'] == 22 and not censored else now,censored)
        h = self.h()
        with patch.object(MilestoneLeaderFollower,'_next',self.omit_deadline(22)), patch.object(MilestoneLeaderFollower,'_prediction_outcome',backdate):
            h.prime(); h.target(12,quantity=M//2); h.leaders(14,600); h.group(35)
            with self.assertRaisesRegex(ProtocolError,'required forecast-horizon decision time'):
                h.finish()

    def test_reader_rejects_writer_only_bypass_of_native_cash_asset_binding(self):
        from replay.streams.protocol import require
        def bypass(condition, message):
            if message != 'cash native asset binding':
                require(condition,message)
        changes = {'initial_cash':[{'venue':venue,'asset':{'kind':'USD','ledger':'synthetic','token':'kalshi'},'amount':'1000'} for venue in ('kalshi','polymarket')]}
        with patch('replay.strategies.milestone_leader_follower.economics.require',side_effect=bypass):
            h = self.h(changes=changes)
        h.prime(); h.leaders(14,600); h.target(16,610,620)
        with self.assertRaisesRegex(ProtocolError,'cash native asset binding'):
            h.finish()

    def elapsed_models(self, phase='between_segments'):
        models = [model(role,'30') for role in ('target_only','target_milestones','leader_milestones')]
        m = models[1]
        before = next(c for c in m['cohorts'] if c['phase'] == phase)
        before['elapsed_max_ns'] = '1'
        m['cohorts'].append({**before,'elapsed_min_ns':'1','elapsed_max_ns':None,'intercept':'0.1'})
        return models

    def test_honest_elapsed_model_deadline_opens_baseline_position(self):
        g = game_file(); g['segments'] = [{'index':1,'start_ns':None,'start_estimated':False,'end_ns':18,'settled_ns':None,'winner':'home','details':{}}]; g['match'] = None
        h = self.h(game=g,changes={'exit_horizon_ns':'30','models':self.elapsed_models()})
        h.prime(); h.leaders(14,600); h.group(35); h.finish()
        self.assertTrue(any(r['kind'] == 'OPEN' and r['role'] == 'target_milestones' and r['time_ns'] == '19' for r in h.records('actions.ndjson')))

    def test_irrelevant_elapsed_cohort_does_not_require_quiet_decision(self):
        g = game_file(); g['segments'] = [{'index':1,'start_ns':None,'start_estimated':False,'end_ns':18,'settled_ns':None,'winner':'home','details':{}}]; g['match'] = None
        h = self.h(game=g,changes={'exit_horizon_ns':'30','models':self.elapsed_models('in_segment')})
        with patch.object(MilestoneLeaderFollower,'_next',self.omit_deadline(19)):
            h.prime(); h.leaders(14,600); h.group(35); h.finish()
        self.assertNotIn('19',[r['time_ns'] for r in h.records('decisions.ndjson')])

    def test_unusable_histories_do_not_require_elapsed_or_window_timers(self):
        g = game_file(); g['segments'] = [{'index':1,'start_ns':None,'start_estimated':False,'end_ns':18,'settled_ns':None,'winner':'home','details':{}}]; g['match'] = None
        h = self.h(game=g,changes={'exit_horizon_ns':'30','models':self.elapsed_models()})
        with patch.object(MilestoneLeaderFollower,'_next',self.omit_deadline(19)):
            h.prime(); h.leaders(14,600)
            for instrument, orientation in (('kalshi:series','outcome'),('kalshi:series','complement'),('polymarket:123','outcome')):
                ladder(h,16,instrument,orientation,why={'kind':'connection_closed'})
            h.group(35); h.finish()
        self.assertNotIn('19',[r['time_ns'] for r in h.records('decisions.ndjson')])

    def test_honest_nonempty_book_only_decisions_cover_pinned_run_start(self):
        h = self.h(changes={'cohort':'book_only_comparison'})
        h.prime(); h.leaders(14,600); h.target(16,610,620); h.finish()
        self.assertEqual(h.records('decisions.ndjson')[0]['time_ns'],'10')
        denominator = next(r for r in h.records('denominators.ndjson') if r['role'] == 'target_only' and r['target'] == ['polymarket:123','outcome'])
        self.assertEqual(sum(int(x) for x in denominator['status_ns'].values()),30)
        self.assertEqual(denominator['status_ns']['HISTORY_UNAVAILABLE'],'3')

    def test_reader_requires_initial_decision_for_nonempty_book_only_transcript(self):
        original = MilestoneLeaderFollower._decision
        def omit_start(strategy, time):
            if time == strategy.clock.start:
                strategy.last_decision = time
                return
            return original(strategy,time)
        h = self.h(changes={'cohort':'book_only_comparison'})
        with patch.object(MilestoneLeaderFollower,'_decision',omit_start):
            h.prime(); h.leaders(14,600); h.target(16,610,620)
            with self.assertRaisesRegex(ProtocolError,'required initial decision time'):
                h.finish()

    def test_no_input_zero_row_book_only_transcript_remains_accepted(self):
        h = self.h(changes={'cohort':'book_only_comparison'})
        with patch.object(MilestoneLeaderFollower,'_decision',lambda strategy,time:setattr(strategy,'last_decision',time)):
            h.window(); h.finish()
        self.assertEqual(h.records('decisions.ndjson'),[])
        self.assertEqual(h.records('denominators.ndjson'),[])

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

    def test_scope_change_requires_pending_cancellation(self):
        h = self.h(scopes='uncaptured',changes={'decision_delay_ns':'2'}); h.prime(); h.leaders(21,600); h.group(24); h.finish()
        self.assertTrue(any(r['kind'] == 'CANCELLED' and r['time_ns'] == '23' for r in h.records('actions.ndjson')))
        self.rewrite(h,'actions.ndjson',lambda rows:rows.__setitem__(slice(None),[r for r in rows if r['kind'] != 'CANCELLED']))
        with self.assertRaisesRegex(ProtocolError,'required pending'):
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

    def test_false_contradiction_cannot_suppress_every_required_action(self):
        h = self.h(); h.prime(); h.leaders(14,600); h.target(16,610,620); h.finish()
        def suppress(rows):
            for d in rows:
                d['knowledge']['contradiction'] = True
                d['observations'] = []
                d['admission']['captured_books'] = 0
                for book in d['admission']['books']:
                    if book['status'] == 'ADMITTED':
                        book['status'] = 'GAME_CONTRADICTION'
        self.rewrite(h,'decisions.ndjson',suppress)
        for name in ('actions.ndjson','positions.ndjson','predictions.ndjson','episodes.ndjson','denominators.ndjson'):
            self.rewrite(h,name,lambda rows:rows.clear())
        with self.assertRaisesRegex(ProtocolError,'contradiction'):
            self.audit(h)

    def test_honest_impossible_released_series_marks_contradiction(self):
        game = game_file(); game['match'] = None
        game['segments'] = [{'index':i,'start_ns':None,'start_estimated':False,'end_ns':20+i,'settled_ns':None,'winner':'home','details':{}} for i in (1,2,3)]
        h = self.h(game=game); h.prime(); h.group(25); h.finish()
        decisions = h.records('decisions.ndjson')
        self.assertTrue(next(d for d in decisions if d['time_ns'] == '23')['knowledge']['contradiction'])
        self.assertFalse(any(d['observations'] for d in decisions if int(d['time_ns']) >= 23))

    def test_alternates_and_route_counts_cannot_be_rewritten(self):
        h = self.h(); h.prime(); h.leaders(14,600); h.target(16,610,620); h.finish()
        self.rewrite(h,'decisions.ndjson',lambda rows:rows[0]['admission'].__setitem__('routes',0))
        with self.assertRaisesRegex(ProtocolError,'candidate admission'):
            self.audit(h)

    def test_known_executable_size_cannot_be_falsely_fee_unavailable(self):
        original = Pricing.sizes
        def lie(pricing,*args,**kwargs):
            sizes = deepcopy(original(pricing,*args,**kwargs))
            for size in sizes:
                if size['quantity'] == '100':
                    size.update(status='FEE_UNKNOWN',buy=None,forecast_sale=None,initial_sale=None,forecast_charge=None,forecast_net=None,initial_net=None,qualifies=False)
            return sizes
        with patch.object(Pricing,'sizes',lie):
            h = self.h(changes={'minimum_forecast_margin':'5'}); h.prime(); h.leaders(14,600); h.target(16,610,620)
            with self.assertRaisesRegex(ProtocolError,'size availability'):
                h.finish()

    def test_valid_weaker_leader_cannot_replace_required_best_candidate(self):
        doc = document()
        for venue, subscriptions in (('kalshi',['series-two']),('polymarket',['456','654'])):
            market = deepcopy(next(m for m in doc['markets'] if m['venue'] == venue))
            market.update(market_id=venue+':series-two',subscription_ids=subscriptions)
            for token, subscription in zip(market['tokens'],subscriptions):
                token['subscription_id'] = subscription
            doc['markets'].append(market)
        doc['markets'].sort(key=lambda m:m['market_id'])
        original = MilestoneLeaderFollower._observation
        def lie(strategy,model,target,route,now,state,*args,**kwargs):
            if model['role'] == 'leader_milestones' and target == ('polymarket:123','outcome') and route is not None and route['leader'][0].startswith('kalshi:'):
                _, routes = strategy._routes(now,state)
                route = next(r for r in routes if r['target'] == list(target) and r['leader'] == ['polymarket:456','outcome'])
            return original(strategy,model,target,route,now,state,*args,**kwargs)
        with patch.object(MilestoneLeaderFollower,'_observation',lie):
            h = self.h(vendor_detail=paired_detail(),outcomes=doc,changes={'max_entries_per_event':2}); h.prime()
            ladder(h,11,'polymarket:456',bids=((520,100*M),),asks=((530,100*M),))
            h.leaders(14,600); h.target(16,610,620)
            with self.assertRaisesRegex(ProtocolError,'candidate'):
                h.finish()

    def test_executable_counterfactual_cannot_be_falsely_unavailable(self):
        original = MilestoneLeaderFollower._prediction_outcome
        order = Pricing.order
        def unavailable(pricing,key,side,quantity,books,capacity,*args,**kwargs):
            if side == 'SELL' and capacity is None and kwargs.get('forecast') is None:
                return None,'FEE_UNKNOWN'
            return order(pricing,key,side,quantity,books,capacity,*args,**kwargs)
        def lie(strategy,*args,**kwargs):
            with patch.object(Pricing,'order',unavailable):
                return original(strategy,*args,**kwargs)
        with patch.object(MilestoneLeaderFollower,'_prediction_outcome',lie):
            h = self.h(); h.prime(); h.leaders(14,600); h.target(16,610,620)
            with self.assertRaisesRegex(ProtocolError,'counterfactual availability'):
                h.finish()

    def test_executable_exit_cannot_be_falsely_unavailable(self):
        original = MilestoneLeaderFollower._exit
        order = Pricing.order
        def lie(strategy,*args,**kwargs):
            calls = 0
            def unavailable(pricing,key,side,quantity,books,capacity,*order_args,**order_kwargs):
                nonlocal calls
                if side == 'SELL':
                    calls += 1
                    if calls > 1:
                        return None,'FEE_UNKNOWN'
                return order(pricing,key,side,quantity,books,capacity,*order_args,**order_kwargs)
            with patch.object(Pricing,'order',unavailable):
                return original(strategy,*args,**kwargs)
        with patch.object(MilestoneLeaderFollower,'_exit',lie):
            h = self.h(); h.prime(); h.leaders(14,600); h.target(16,610,620)
            with self.assertRaisesRegex(ProtocolError,'exit availability'):
                h.finish()

    def test_entry_revalidation_cannot_invent_missing_history(self):
        h = self.h(); h.prime(); h.leaders(14,600); h.target(16,610,620); h.finish()
        def lie(rows):
            for row in rows:
                if row['kind'] == 'OPEN':
                    observation = row['revalidation']
                    observation.update(target_endpoint=None,status='HISTORY_UNAVAILABLE',selected=None,signal=None,displacement=None,prediction_id=None,sizes=[],innovation=None)
                    for field in ('order','ledger','knowledge','horizon_ns'):
                        del row[field]
                    row.update(kind='SKIPPED',reason='CAPACITY_OR_PREDICATE',position_id=None)
            rows[:] = [r for r in rows if r['kind'] not in ('EXIT_LATCHED','CLOSED')]
        self.rewrite(h,'actions.ndjson',lie)
        self.rewrite(h,'positions.ndjson',lambda rows:rows.clear())
        with self.assertRaisesRegex(ProtocolError,'repricing.*history'):
            self.audit(h)

    def test_large_native_ladder_is_not_repeated_in_every_order_proof(self):
        h = self.h(); h.prime(); h.leaders(14, 600)
        ladder(h, 16, 'polymarket:123', bids=tuple((520-i, M) for i in range(100)), asks=tuple((530+i, M) for i in range(100)))
        h.finish()
        raw = (h.output / 'decisions.ndjson').read_bytes()
        self.assertLess(len(raw.splitlines()[-1]), 20000)

    def test_deep_counterfactual_walk_is_compact_across_all_output_files(self):
        h = self.h(changes={'stop_loss_fraction':'0.5'}); h.prime()
        ladder(h,12,'polymarket:123',bids=tuple((520-i,M) for i in range(100)),asks=tuple((530+i,M) for i in range(100)))
        h.leaders(14,700); h.group(23); h.finish()
        self.assertTrue(any(r['sale'] is not None and int(r['sale']['quantity_atoms']) == 100*M for r in h.records('predictions.ndjson')))
        self.assertLess(max(map(len,(h.output/'predictions.ndjson').read_bytes().splitlines())),15000)


if __name__ == '__main__':
    import unittest
    unittest.main()
