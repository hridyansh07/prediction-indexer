"""Bounded, staged, offline milestone-conditioned leader/follower scenario runtime."""
from fractions import Fraction
from pathlib import Path

from replay.economic_intervals import CutClock
from replay.economic_sdk.game import Timeline
from replay.economic_sdk.outcomes import outcome_scope
from replay.game_state import load as load_game
from replay.preparation import digest, encoded
from replay.strategy_sdk import PreparedInput, LineWriter, plain
from replay.strategies._shared.fee_bridge import FeeBridge
from replay.streams.protocol import require
from replay.supervisor import write_json_durable

from .contract import STRATEGY, ROLES, configuration, identity, number, r
from .economics import Account, Capacity, Pricing, atoms
from .history import History, Knowledge, cohort, displacement, relation

LIMITS = {'max_bytes': 512 * 1024 * 1024, 'max_records': 1000000, 'max_line_bytes': 4 * 1024 * 1024}
FILES = ('decisions.ndjson', 'actions.ndjson', 'episodes.ndjson', 'predictions.ndjson', 'positions.ndjson', 'denominators.ndjson')


class MilestoneLeaderFollower:
    def __init__(self, context):
        raw = plain(context['config'])
        self.input = PreparedInput({k: raw[k] for k in ('version', 'snapshot_directory', 'snapshot_sha256')})
        self.snapshot = plain(self.input.snapshot)
        self.config = configuration(raw, self.snapshot)
        self.policy = self.config['policy']
        self.experiment = identity(self.config)
        self.timeline = Timeline(load_game(self.policy['game']['input']['path'], self.policy['game']['input']['sha256'], self.snapshot), self.policy['game'], int(self.snapshot['config']['start_ns']))
        self.knowledge = Knowledge()
        self.fees = FeeBridge(self.config['fees'], self.snapshot['plans'])
        self.pricing = Pricing(self.snapshot, self.fees, self.policy, self.experiment)
        require(len(self.snapshot['plans']) <= self.policy['max_books'], 'book bound exceeded')
        self.accounts = {role: Account(self.policy, self.pricing) for role in ROLES}
        self.all_positions = {role: [] for role in ROLES}
        self.root = Path(context['output_directory'])
        require(self.root.is_dir() and not any(self.root.iterdir()), 'output directory must be empty')
        self.writers = {name: LineWriter(self.root / name, **LIMITS) for name in FILES}
        self.binding = {k: context[k] for k in ('run_id', 'attempt_id', 'group', 'identity')}
        self.clock = CutClock(self.snapshot)
        self.books, self.history, self.routes_history, self.signals = {}, {}, {}, {}
        self.scope, self.sequence, self.stage = 0, -1, None
        self.dirty, self.transcript_updates = False, {}
        self.history_resets = set()
        self.terminal = self.finished = self.poisoned = False
        self.last_decision = None
        self.previous_observations = {}
        self.denominators = {}
        self.pending_predictions = []
        self.pending_prediction_bytes = 0
        self.episode_count = 0

    def __call__(self, cut):
        try:
            require(not self.poisoned and not self.terminal, 'closed leader strategy')
            require(cut.sequence == self.sequence + 1, 'strategy sequence')
            self.sequence = cut.sequence
            if cut.kind == 'initial':
                self.input.bind(cut.body)
                return
            require(self.input.bound, 'missing initial')
            if cut.kind == 'terminal':
                end = self.clock.terminal()
                self._advance(end, terminal=True)
                self.terminal = True
                return
            require(cut.kind == 'cut', 'cut kind')
            raw, time = self.clock.observe(cut)
            self._advance(time)
            # Detach only callback-owned changed books after committing earlier time.
            for transition in cut.body['book_transitions']:
                key = (transition['key']['instrument'], transition['key']['orientation'])
                book = cut.books[key]
                if transition['decision']['kind'] == 'invalidation':
                    self.history_resets.add(key)
                bids, asks = book.levels('bid'), book.levels('ask')
                require(max(len(bids), len(asks)) <= 1024, 'book level bound exceeded')
                value = {'key': list(key), 'validity': book.validity, 'revision': book.revision,
                         'bids': [list(x) for x in bids], 'asks': [list(x) for x in asks],
                         'last_change_ns': str(raw), 'reason': None if book.reason is None else plain(book.reason)['kind']}
                prior = self.books.get(key)
                # Identical snapshots do not create innovations or retry exits.
                if prior is None or any(value[k] != prior[k] for k in ('validity', 'bids', 'asks', 'reason')):
                    self.books[key] = value
                    self.transcript_updates[key] = value
                    self.dirty = True
            self.stage = time
        except Exception:
            self.poisoned = True
            raise

    def _next(self, limit):
        times = []
        if self.scope + 1 < len(self.snapshot['scopes']):
            times.append(int(self.snapshot['scopes'][self.scope]['end_ns']))
        if self.timeline.next_time is not None:
            times.append(max(self.clock.start, self.timeline.next_time))
        now = self.last_decision if self.last_decision is not None else self.clock.start - 1
        for history in (*self.history.values(), *self.routes_history.values()):
            times.extend(history.timers(int(self.policy['window_ns']), now))
        for signal in self.signals.values():
            if signal['pending'] is not None:
                times.append(signal['pending']['due'])
            if signal['false_since'] is not None:
                times.append(signal['false_since'] + int(self.policy['rearm_false_ns']))
        for account in self.accounts.values():
            for position in account.positions.values():
                if position['holding_atoms']:
                    if position['exit_reason'] is None:
                        times.append(position['horizon'])
                        rate = number(self.policy['holding_rate_per_ns'])
                        if rate > 0:
                            mark, _ = self.pricing.order(position['target'], 'SELL', position['holding_atoms'], self.books, account.capacity, now, self.sequence, self.scope, position['route'])
                            if mark is not None and position['basis'] > 0:
                                delay = (Fraction(mark['cash']) - position['basis'] + position['cost'] * number(self.policy['stop_loss_fraction'])) / (position['basis'] * rate)
                                times.append(position['opened'] + max(0, -(-delay.numerator // delay.denominator)))
                    if position['settlement_due'] is not None:
                        times.append(position['settlement_due'])
        for prediction in self.pending_predictions:
            times.append(prediction['due'])
        if self.knowledge.last_result_release is not None:
            for model in self.policy['models']:
                for item in model['cohorts']:
                    for boundary in (item['elapsed_min_ns'], item['elapsed_max_ns']):
                        if boundary is not None:
                            times.append(self.knowledge.last_result_release + int(boundary))
        eligible = [t for t in times if now < t < limit]
        return min(eligible) if eligible else None

    def _advance(self, time, terminal=False):
        if self.stage is not None and self.stage < time:
            self._decision(self.stage)
            self.stage = None
        while True:
            due = self._next(time)
            if due is None:
                break
            self._decision(due)
        if terminal:
            if self.stage is not None:
                self._decision(self.stage)
                self.stage = None
            self._close_intervals(time)
            for prediction in self.pending_predictions:
                self._prediction_outcome(prediction, time, censored=True)
            self.pending_predictions.clear(); self.pending_prediction_bytes = 0
            for role, account in self.accounts.items():
                for position in self.all_positions[role]:
                    if position['holding_atoms']:
                        position['state'] = 'CENSORED'
                    row = account.record(position, time)
                    mark = None
                    if position['holding_atoms']:
                        mark, reason = self.pricing.order(position['target'], 'SELL', position['holding_atoms'], self.books, account.capacity, time, self.sequence, self.scope, position['route'])
                        row['liquidation_mark'] = None if mark is None else mark['cash']
                        row['mark_status'] = reason or 'PRICED'
                    else:
                        row['liquidation_mark'] = '0'; row['mark_status'] = 'CLOSED'
                    self.writers['positions.ndjson'].append({'version': 1, 'role': role, **row})
            return
        self.stage = time

    def _history(self, key, now):
        plan = self.pricing.plans[key]
        rule = self.pricing.rules.get(key)
        authority = digest([key, plan['lane'], plan['price_scale'], plan['quantity_scale'], rule])
        ident = (key, authority)
        if ident not in self.history:
            require(len(self.history) < self.policy['max_books'] * 128, 'history authority bound')
            self.history[ident] = History(int(self.policy['history_ns']), self.policy['history_changes'])
        history = self.history[ident]
        history.observe(now, self.pricing.midpoint(key, self.books))
        return history

    def _routes(self, now, state):
        scope = outcome_scope(self.snapshot, self.scope)
        books = [(x['instrument'], x['orientation']) for x in self.snapshot['scopes'][self.scope]['outcome_books']] if self.snapshot['version'] == 2 else []
        targets = {}
        routes = []
        for key in books:
            leg = scope.leg(key)
            if leg is None:
                continue
            rule = self.pricing.rules.get(key)
            if rule is None or self.fees.economics(key) is None:
                continue
            feasible = self.knowledge.feasible(scope.spaces[leg.shape_id])
            if feasible is None or not feasible or self.knowledge.contradiction:
                continue
            targets[key] = (leg, feasible)
            self._history(key, now)
        for target, (tleg, feasible) in sorted(targets.items()):
            for leader, (lleg, _) in sorted(targets.items()):
                if lleg.market_id == tleg.market_id:
                    continue
                if self.pricing.rules[leader]['alignment_id'] != self.pricing.rules[target]['alignment_id']:
                    continue
                kind, proof = relation(lleg, tleg, feasible)
                route = {'leader': list(leader), 'target': list(target), 'market_id': tleg.market_id,
                         'leader_market_id': lleg.market_id, 'shape_id': tleg.shape_id,
                         'leader_keys': sorted(lleg.keys), 'target_keys': sorted(tleg.keys),
                         'feasible_keys': sorted(feasible), 'relation': kind, 'proof': None,
                         'rule_ids': [self.pricing.rules[k]['rule_sha256'] for k in (leader, target)]}
                if proof is not None:
                    proof = digest([proof, route['rule_ids'], [self.pricing.plans[k]['lane'] for k in (leader, target)]])
                    route['proof'] = proof
                    ident = (leader, target, proof)
                    if ident not in self.routes_history:
                        require(len(self.routes_history) < self.policy['max_routes'] * 128, 'relationship history bound')
                        self.routes_history[ident] = History(int(self.policy['history_ns']), self.policy['history_changes'])
                    coordinate = self.pricing.midpoint(leader, self.books)
                    if coordinate is not None and kind == 'COMPLEMENT':
                        coordinate = 1 - coordinate
                    self.routes_history[ident].observe(now, coordinate)
                route['candidate_id'] = digest(route)
                routes.append(route)
                require(len(routes) <= self.policy['max_routes'], 'route bound exceeded')
        active_proofs = {(tuple(route['leader']), tuple(route['target']), route['proof']) for route in routes if route['proof'] is not None}
        for ident in list(self.routes_history):
            if ident not in active_proofs:
                del self.routes_history[ident]
        for ident in list(self.history):
            if ident[0] not in targets:
                del self.history[ident]
        history_entries = sum(len(h.values) for h in (*self.history.values(), *self.routes_history.values()))
        book_levels = sum(len(b['bids']) + len(b['asks']) for b in self.books.values())
        require(160 * (history_entries + book_levels) + 4096 * len(active_proofs) <= 128 * 1024 * 1024, 'detached-state bound exceeded')
        return targets, routes

    def _observation(self, model, target, route, now, state, capacity=None, maximum=None):
        target_history = self._history(target, now)
        target_endpoint = target_history.endpoints(now, int(self.policy['window_ns']))
        leader_endpoint = None
        if route is not None and route['proof'] is not None:
            leader_endpoint = self.routes_history[tuple(route['leader']), target, route['proof']].endpoints(now, int(self.policy['window_ns']))
        index = cohort(model, state)
        row = {'role': model['role'], 'target': list(target), 'route': route, 'model_id': digest(model), 'cohort': index,
               'target_endpoint': target_endpoint, 'leader_endpoint': leader_endpoint,
               'displacement': None, 'status': 'MODEL_UNAVAILABLE', 'sizes': [], 'selected': None, 'signal': None,
               'innovation': None, 'prediction_id': None, 'alternates': []}
        if self.knowledge.contradiction:
            row['status'] = 'GAME_CONTRADICTION'; return row
        if index is None:
            return row
        if target_endpoint is None or model['role'] == 'leader_milestones' and leader_endpoint is None:
            row['status'] = 'HISTORY_UNAVAILABLE'; return row
        if model['role'] == 'leader_milestones' and route['relation'] not in ('IDENTITY', 'COMPLEMENT'):
            return row
        delta_l = Fraction(leader_endpoint['delta']) if model['role'] == 'leader_milestones' else Fraction(0)
        delta_t = Fraction(target_endpoint['delta'])
        move = displacement(model, index, delta_l, delta_t)
        row['displacement'] = r(move)
        if route is None:
            leg = outcome_scope(self.snapshot, self.scope).leg(target)
            route = {'leader': None, 'target': list(target), 'market_id': leg.market_id, 'leader_market_id': None,
                     'shape_id': leg.shape_id, 'leader_keys': [], 'target_keys': sorted(leg.keys),
                     'feasible_keys': sorted(self.knowledge.feasible(outcome_scope(self.snapshot, self.scope).spaces[leg.shape_id])),
                     'relation': 'TARGET_BASELINE', 'proof': None, 'rule_ids': [self.pricing.rules[target]['rule_sha256']], 'candidate_id': digest([model['role'], target])}
            row['route'] = route
        sizes = self.pricing.sizes(target, self.books, capacity, now, self.sequence, self.scope, route, move, maximum)
        row['sizes'] = sizes
        available = [i for i, size in enumerate(sizes) if size['status'] == 'AVAILABLE']
        if not available:
            row['status'] = sizes[0]['status'] if sizes else 'SIZE_UNAVAILABLE'; return row
        selected = min(available, key=lambda i: (-Fraction(sizes[i]['forecast_net']), -Fraction(sizes[i]['buy']['cash']), number(sizes[i]['quantity'])))
        row['selected'], row['status'] = selected, 'AVAILABLE'
        row['signal'] = sizes[selected]['qualifies'] and (model['role'] != 'leader_milestones' or abs(delta_l) >= number(self.policy['minimum_leader_move'])) and state['phase'] != 'finished'
        if model['role'] == 'leader_milestones':
            hist = self.routes_history[tuple(route['leader']), target, route['proof']]
        else:
            hist = target_history
        row['innovation'] = [route['leader'] if model['role'] == 'leader_milestones' else list(target), str(hist.innovation_time), hist.price_revision, route['proof']]
        return row

    def _decision(self, now):
        if self.last_decision == now and not self.dirty:
            return
        require(self.last_decision is None or now > self.last_decision, 'one committed decision per time')
        old_scope = self.scope
        while self.scope + 1 < len(self.snapshot['scopes']) and int(self.snapshot['scopes'][self.scope]['end_ns']) <= now:
            self.scope += 1
        if old_scope != self.scope:
            observable = {(b['instrument'], b['orientation']) for b in self.snapshot['scopes'][self.scope]['required_books']}
            for key, book in list(self.books.items()):
                if key not in observable:
                    value = {**book, 'validity': 'unusable', 'bids': [], 'asks': [], 'reason': 'scope_unavailable', 'last_change_ns': str(now)}
                    self.books[key] = value; self.transcript_updates[key] = value; self.history_resets.add(key)
        facts = self.timeline.advance(now)
        self.knowledge.release(facts, self.timeline.view.competitors)
        book_only = self.policy['cohort'] == 'book_only_comparison'
        state = self.knowledge.state(self.timeline.view, now, book_only)
        if book_only:
            # Optional comparison explicitly ignores released constraints as well.
            self.knowledge = Knowledge()
        for (native, authority), history in self.history.items():
            if any(k == native or k[0] == native[0] and self.pricing.plans[native]['venue'] == 'kalshi' for k in self.history_resets):
                history.observe(now, None)
        for (leader, target, proof), history in self.routes_history.items():
            if any(k in (leader, target) or k[0] in (leader[0], target[0]) and self.pricing.plans[k]['venue'] == 'kalshi' for k in self.history_resets):
                history.observe(now, None)
        targets, routes = self._routes(now, state)
        state['contradiction'] = self.knowledge.contradiction
        self._close_intervals(now)
        for prediction in list(self.pending_predictions):
            if prediction['due'] <= now:
                self._prediction_outcome(prediction, now)
                self.pending_predictions.remove(prediction)
                self.pending_prediction_bytes -= prediction['byte_cost']
        observations = []
        for model in self.policy['models']:
            for target in sorted(targets):
                if model['role'] == 'leader_milestones':
                    candidates = [self._observation(model, target, route, now, state) for route in routes if tuple(route['target']) == target]
                    usable = [x for x in candidates if x['selected'] is not None]
                    positive = [x for x in usable if x['signal']]
                    pending = self.signals.get((model['role'], target), {}).get('pending')
                    if pending is not None:
                        frozen = [x for x in candidates if x['route']['leader'] == pending['route']['leader'] and x['route']['proof'] == pending['route']['proof']]
                        usable = [x for x in frozen if x['selected'] is not None]
                        positive = [x for x in usable if x['signal']]
                        candidates = frozen or candidates
                    selected = min(positive or usable, key=lambda x: (-Fraction(x['sizes'][x['selected']]['forecast_net']), -Fraction(x['sizes'][x['selected']]['buy']['cash']), x['route']['candidate_id'])) if usable else (candidates[0] if candidates else self._observation(model, target, None, now, state))
                    selected['alternates'] = [{'candidate_id': x['route']['candidate_id'], 'status': x['status'], 'forecast_net': None if x['selected'] is None else x['sizes'][x['selected']]['forecast_net']} for x in candidates if x is not selected]
                else:
                    selected = self._observation(model, target, None, now, state)
                    selected['alternates'] = []
                if selected['selected'] is not None:
                    selected['prediction_id'] = digest([self.experiment, model['role'], list(target), now, selected['route']['candidate_id']])
                    require(len(self.pending_predictions) < 100000, 'prediction horizon bound exceeded')
                    size = selected['sizes'][selected['selected']]
                    lightweight = {k: selected[k] for k in ('role', 'target', 'route', 'prediction_id', 'signal', 'displacement')}
                    lightweight.update(selected=0, sizes=[{'buy': {'retained_atoms': size['buy']['retained_atoms'], 'cash': size['buy']['cash']}, 'initial_sale': {'taken': [size['initial_sale']['taken'][0]]}, 'forecast_net': size['forecast_net']}])
                    byte_cost = len(encoded(lightweight)) + 512
                    require(self.pending_prediction_bytes + byte_cost <= 64 * 1024 * 1024, 'pending forecast state bound exceeded')
                    self.pending_prediction_bytes += byte_cost
                    self.pending_predictions.append({'row': lightweight, 'due': now + int(self.policy['exit_horizon_ns']), 'time': now, 'scope': self.scope, 'byte_cost': byte_cost})
                observations.append(selected)
        exited = {role: set() for role in ROLES}
        for role, account in self.accounts.items():
            for position in list(account.positions.values()):
                if position['holding_atoms']:
                    if self._exit(role, position, now, state, facts):
                        exited[role].add(position['target'])
        self.current_predictions = {}
        for observation in observations:
            self.current_predictions.setdefault(tuple(observation['target']), {})[observation['role']] = {k: observation[k] for k in ('model_id', 'prediction_id', 'status', 'displacement', 'signal')}
            self.current_predictions[tuple(observation['target'])][observation['role']]['forecast_net'] = None if observation['selected'] is None else observation['sizes'][observation['selected']]['forecast_net']
        for observation in observations:
            self._signal(observation, now, state, exited[observation['role']])
        active = {(o['role'], tuple(o['target'])) for o in observations}
        for key, signal in self.signals.items():
            if key not in active:
                self._end_episode(key, signal, now, 'UNAVAILABLE')
                if signal['pending'] is not None:
                    self._action(key[0], now, 'CANCELLED', 'SCOPE_UNAVAILABLE', signal['pending']['attempt_id'], None)
                    signal['pending'] = None
                signal['false_since'] = None
                signal['truth'] = None
        self.previous_observations = {(o['role'], tuple(o['target'])): (now, o) for o in observations}
        self.writers['decisions.ndjson'].append({'history_resets': [list(k) for k in sorted(self.history_resets)], 'version': 1, 'time_ns': str(now), 'sequence': self.sequence, 'scope': self.scope, 'knowledge': state,
            'released': [{'kind': f.kind, 'release_ns': str(f.release_ns), 'source_ns': str(f.source_ns), 'index': f.index,
                          'winner': f.value[0] if f.kind == 'segment_end' else None,
                          'score': dict(f.value['score']) if f.kind == 'match_end' else None} for f in facts],
            'book_updates': [self.transcript_updates[k] for k in sorted(self.transcript_updates)], 'observations': observations,
            'admission': {'books': [{'key': [x['instrument'], x['orientation']], 'status': x['status'] if x['status'] != 'MASKED' else 'RULE_UNAVAILABLE' if (x['instrument'], x['orientation']) not in self.pricing.rules else 'FEE_UNKNOWN' if self.fees.economics((x['instrument'], x['orientation'])) is None else 'GAME_CONTRADICTION' if self.knowledge.contradiction else 'ADMITTED'} for x in self.snapshot['scopes'][self.scope].get('outcome_books', [])], 'captured_books': len(targets), 'unresolved_members': len(self.snapshot['scopes'][self.scope]['unresolved_market_ids']), 'routes': len(routes), 'unsupported_routes': [{'candidate_id': x['candidate_id'], 'relation': x['relation'], 'status': 'MODEL_UNAVAILABLE'} for x in routes if x['relation'] not in ('IDENTITY', 'COMPLEMENT')]}})
        self.last_decision = now
        self.transcript_updates.clear(); self.history_resets.clear(); self.dirty = False

    def _close_intervals(self, now):
        for key, (start, observation) in self.previous_observations.items():
            if start >= now:
                continue
            status = observation['status']; duration = now - start
            values = self.denominators.setdefault(key, {})
            values[status] = values.get(status, 0) + duration
            signal = self.signals.get(key)
            if observation['signal']:
                if signal is not None and signal['episode'] is None:
                    signal['episode'] = start
            elif signal is not None and signal['episode'] is not None:
                self._end_episode(key, signal, start, 'PREDICATE_FALSE')
        self.previous_observations = {}

    def _end_episode(self, key, signal, now, reason):
        start = signal['episode']
        if start is not None and now > start:
            self.writers['episodes.ndjson'].append({'version': 1, 'role': key[0], 'target': list(key[1]), 'episode_id': digest([self.experiment, key[0], key[1], start]), 'start_ns': str(start), 'end_ns': str(now), 'end_reason': reason, 'censored': reason == 'RUN_END'})
            self.episode_count += 1
        signal['episode'] = None

    def _action(self, role, now, kind, reason, attempt, position, **extra):
        self.writers['actions.ndjson'].append({'version': 1, 'role': role, 'time_ns': str(now), 'kind': kind, 'reason': reason,
            'attempt_id': attempt, 'position_id': None if position is None else position['id'], **extra})

    def _signal(self, observation, now, state, exited):
        key = observation['role'], tuple(observation['target'])
        account = self.accounts[key[0]]
        signal = self.signals.setdefault(key, {'truth': False, 'false_since': None, 'armed': True, 'last_innovation': None, 'last_attempt': None, 'pending': None, 'episode': None})
        truth = observation['signal']
        if truth is not True and signal['episode'] is not None:
            self._end_episode(key, signal, now, 'UNAVAILABLE' if truth is None else 'PREDICATE_FALSE')
        if truth is None:
            signal['false_since'] = None
        elif truth is False:
            if signal['false_since'] is None:
                signal['false_since'] = now
        else:
            if signal['episode'] is None:
                signal['episode'] = now
        false_interval_ready = signal['false_since'] is not None and now - signal['false_since'] >= int(self.policy['rearm_false_ns'])
        if not signal['armed'] and signal['false_since'] is not None and now - signal['false_since'] >= int(self.policy['rearm_false_ns']):
            signal['armed'] = True
        if truth is True:
            signal['false_since'] = None
        pending = signal['pending']
        if pending is not None and pending['due'] <= now:
            signal['pending'] = None
            if not truth or observation['innovation'] != pending['innovation'] or observation['route']['proof'] != pending['route']['proof']:
                self._action(key[0], now, 'CANCELLED', 'INNOVATION_CHANGED_OR_NONQUALIFYING', pending['attempt_id'], None)
            else:
                self._enter(observation, pending, now, state, exited)
        elif pending is not None and (truth is None or observation['innovation'] != pending['innovation']):
            signal['pending'] = None
            self._action(key[0], now, 'CANCELLED', 'INNOVATION_CHANGED_OR_UNAVAILABLE', pending['attempt_id'], None)
        position = account.positions.get(key[1])
        close_time = position.get('closed') if position is not None else None
        reentry = signal['last_attempt'] is not None
        later_change = observation['innovation'] is not None and (signal['last_attempt'] is None or int(observation['innovation'][1]) > signal['last_attempt'])
        cooldown = close_time is None or now >= close_time + int(self.policy['cooldown_after_close_ns'])
        # False intervals are only economic false; unknown intervals never rearm.
        if truth and signal['armed'] and signal['truth'] is not True and signal['pending'] is None and (not reentry or false_interval_ready and later_change and cooldown and (position is None or position['holding_atoms'] == 0)):
            intended = observation['sizes'][observation['selected']]['buy']
            attempt = {'attempt_id': digest([self.experiment, key, now, observation['innovation']]), 'innovation': observation['innovation'],
                       'route': observation['route'], 'maximum': int(intended['quantity_atoms']), 'baseline_predictions': self.current_predictions[key[1]], 'due': now + int(self.policy['decision_delay_ns'])}
            signal['armed'] = False; signal['last_attempt'] = now; signal['last_innovation'] = observation['innovation']; signal['false_since'] = None
            self._action(key[0], now, 'SIGNALLED', 'FALSE_TO_TRUE', attempt['attempt_id'], None, innovation=attempt['innovation'], route=attempt['route'], maximum_quantity_atoms=str(attempt['maximum']), due_ns=str(attempt['due']), baseline_predictions=attempt['baseline_predictions'])
            if int(self.policy['decision_delay_ns']):
                signal['pending'] = attempt
                self._action(key[0], now, 'PENDING', 'DECISION_DELAY', attempt['attempt_id'], None)
            else:
                self._enter(observation, attempt, now, state, exited)
        signal['truth'] = truth

    def _enter(self, observation, attempt, now, state, exited):
        role = observation['role']; target = tuple(observation['target']); account = self.accounts[role]
        if target in exited:
            self._action(role, now, 'SKIPPED', 'SAME_TIME_EXIT', attempt['attempt_id'], None); return
        model = next(m for m in self.policy['models'] if m['role'] == role)
        # Freeze the selected leader/model/innovation; never adopt another route.
        route = attempt['route'] if role == 'leader_milestones' else None
        repriced = self._observation(model, target, route, now, state, account.capacity, attempt['maximum'])
        if repriced['signal'] is not True or repriced['innovation'] != attempt['innovation']:
            self._action(role, now, 'SKIPPED', 'CAPACITY_OR_PREDICATE', attempt['attempt_id'], None, revalidation=repriced); return
        order = repriced['sizes'][repriced['selected']]['buy']
        if not int(self.policy['decision_delay_ns']) and int(order['quantity_atoms']) != attempt['maximum']:
            self._action(role, now, 'SKIPPED', 'CAPACITY_OR_PREDICATE', attempt['attempt_id'], None, revalidation=repriced); return
        reason = account.gate(order)
        if reason:
            self._action(role, now, 'SKIPPED', reason, attempt['attempt_id'], None, revalidation=repriced); return
        position, ledger = account.open(order, attempt, now, state)
        self.all_positions[role].append(position)
        self._action(role, now, 'OPEN', 'SIMULTANEOUS_DISPLAYED_SCENARIO', attempt['attempt_id'], position, order=order, ledger=ledger, revalidation=repriced, knowledge=state, horizon_ns=str(position['horizon']))
        # Known-outcome cash never predates acquisition and posts after the opening.
        self._resolution(position, now)
        if position['settlement_due'] is not None and position['settlement_due'] <= now:
            self._settle(role, position, now)

    def _resolution(self, position, now):
        if self.knowledge.contradiction:
            return
        mode = self.policy['settlement']['mode']
        if mode == 'UNRESOLVED' or position['settlement_due'] is not None:
            return
        if mode == 'PINNED_SETTLEMENT':
            item = next((x for x in self.policy['settlement']['evidence'] if (x['instrument'], x['orientation']) == position['target']), None)
            if item is not None:
                position.update(settlement_due=max(position['opened'], int(item['available_ns'])), payout=number(item['payout']), settlement_identity=item['evidence_sha256'])
            return
        scope = outcome_scope(self.snapshot, self.scope)
        space = scope.spaces.get(position['shape_id'])
        if space is None:
            return
        resolved = self.knowledge.first_resolution(space, position['claim_keys'])
        if resolved is not None:
            release, payout, settlement_identity = resolved
            position.update(settlement_due=max(position['opened'], release + int(self.policy['settlement']['delay_ns'])), payout=payout, settlement_identity=settlement_identity)

    def _settle(self, role, position, now):
        account = self.accounts[role]
        scale = int(self.pricing.plans[position['target']]['quantity_scale'])
        payout = position['payout'] * Fraction(position['holding_atoms'], 10 ** scale)
        position.update(exit_reason='SETTLEMENT', exit_time=now)
        ledger = account.dispose(position, None, now, payout=payout)
        self._action(role, now, 'SETTLED', self.policy['settlement']['mode'], position['attempt_id'], position, ledger=ledger, payout=r(payout), payout_per_contract=r(position['payout']), settlement_identity=position['settlement_identity'])

    def _exit(self, role, position, now, state, facts):
        account = self.accounts[role]
        self._resolution(position, now)
        if position['settlement_due'] is not None and position['settlement_due'] <= now and not self.knowledge.contradiction:
            self._settle(role, position, now); return True
        reasons = []
        if self.policy['exit_on_match_end'] and any(f.kind == 'match_end' and digest([f.kind, f.release_ns, f.source_ns, f.index]) not in position['milestones'] for f in facts):
            reasons.append('MATCH_END')
        if self.policy['exit_on_next_segment_end'] and any(f.kind == 'segment_end' and digest([f.kind, f.release_ns, f.source_ns, f.index]) not in position['milestones'] for f in facts):
            reasons.append('SEGMENT_END')
        if now >= position['horizon']:
            reasons.append('HORIZON')
        mark, mark_reason = self.pricing.order(position['target'], 'SELL', position['holding_atoms'], self.books, account.capacity, now, self.sequence, self.scope, position['route'])
        if mark is not None:
            charge = position['basis'] * number(self.policy['holding_rate_per_ns']) * (now - position['opened'])
            net = Fraction(mark['cash']) - position['basis'] - charge
            if net <= -position['cost'] * number(self.policy['stop_loss_fraction']):
                reasons.append('STOP_LOSS')
            if net >= position['cost'] * number(self.policy['take_profit_fraction']):
                reasons.append('TAKE_PROFIT')
        if position['exit_reason'] is None and reasons:
            position.update(exit_reason=reasons[0], exit_time=now, state='EXIT_PENDING')
            self._action(role, now, 'EXIT_LATCHED', reasons[0], position['attempt_id'], position)
        if position['exit_reason'] is None:
            return False
        source, bids = self.pricing.ladder(position['target'], 'bid', self.books, account.capacity)
        _, observed_bids = self.pricing.ladder(position['target'], 'bid', self.books)
        fingerprint = (source, None if observed_bids is None else tuple(observed_bids), self.fees.engine_identity)
        if position.get('last_exit_fingerprint') == fingerprint:
            return False
        position['last_exit_fingerprint'] = fingerprint
        if bids is None:
            self._action(role, now, 'EXIT_UNAVAILABLE', 'UNUSABLE', position['attempt_id'], position); return True
        increment = int(self.pricing.rules[position['target']]['quantity_increment_atoms'])
        available = min(position['holding_atoms'], sum(q for _, q in bids))
        quantity = available // increment * increment
        order, reason = (None, 'NO_EXIT_DEPTH') if not quantity else self.pricing.order(position['target'], 'SELL', quantity, self.books, account.capacity, now, self.sequence, self.scope, position['route'])
        if order is None:
            self._action(role, now, 'EXIT_UNAVAILABLE', reason, position['attempt_id'], position); return True
        ledger = account.dispose(position, order, now)
        self._action(role, now, 'CLOSED' if position['state'] == 'CLOSED' else 'PARTIAL_EXIT', position['exit_reason'], position['attempt_id'], position, order=order, ledger=ledger, lateness_ns=str(max(0, now - position['horizon'])))
        return True

    def _prediction_outcome(self, prediction, now, censored=False):
        row = prediction['row']; target = tuple(row['target'])
        size = row['sizes'][row['selected']]
        sale = None; reason = 'RUN_END' if censored else None
        if not censored:
            sale, reason = self.pricing.order(target, 'SELL', int(size['buy']['retained_atoms']), self.books, None, now, self.sequence, self.scope, row['route'])
        initial_bid = size['initial_sale']['taken'][0][0]
        actual_bid = None if sale is None else sale['taken'][0][0]
        scale = 10 ** int(self.pricing.plans[target]['price_scale'])
        error = None if actual_bid is None else r(Fraction(actual_bid - initial_bid, scale) - Fraction(row['displacement']))
        cost = -Fraction(size['buy']['cash'])
        charge = cost * number(self.policy['holding_rate_per_ns']) * (now - prediction['time'])
        self.writers['predictions.ndjson'].append({'version': 1, 'prediction_id': row['prediction_id'], 'role': row['role'], 'target': row['target'], 'decision_ns': str(prediction['time']), 'desired_ns': str(prediction['due']), 'observed_ns': str(now), 'signal': row['signal'], 'forecast_displacement': row['displacement'], 'forecast_net': size['forecast_net'], 'forecast_error': error, 'counterfactual_net': None if sale is None else r((Fraction(sale['cash']) - cost - charge) * self.pricing.weight(target)), 'status': reason or 'OBSERVED', 'capacity_mode': 'ISOLATED_NONADDITIVE', 'sale': sale})

    def finish(self):
        try:
            require(self.terminal and not self.poisoned and not self.finished, 'missing terminal/failed strategy')
            for key, signal in self.signals.items():
                self._end_episode(key, signal, self.clock.end, 'RUN_END')
                if signal['pending'] is not None:
                    self._action(key[0], self.clock.end, 'CANCELLED', 'RUN_END', signal['pending']['attempt_id'], None)
            for (role, target), statuses in sorted(self.denominators.items()):
                self.writers['denominators.ndjson'].append({'version': 1, 'role': role, 'target': list(target), 'status_ns': {k: str(v) for k, v in sorted(statuses.items())}})
            files = {name: writer.finish() for name, writer in self.writers.items()}
            manifest = {'version': 1, 'strategy': STRATEGY, 'config': self.config, 'experiment_sha256': self.experiment,
                        'fee_engine_identity': self.fees.engine_identity, 'settlement_model': 'normal_resolution_only',
                        'membership_basis': self.snapshot['membership_basis'], 'history_complete': self.snapshot['history_complete'],
                        'scenario': 'SIMULTANEOUS_DISPLAYED_SCENARIO', 'trading_status': 'trading_status_unknown',
                        'financing': 'ZERO_FINANCING_COST_SCENARIO' if number(self.policy['holding_rate_per_ns']) == 0 else 'ANALYTICAL_HOLDING_COST_SCENARIO',
                        'files': files}
            from .output import validate_content
            summary = validate_content(self.root, self.snapshot, manifest)
            write_json_durable(self.root / 'summary.json', summary)
            manifest['summary_sha256'] = digest(summary)
            write_json_durable(self.root / 'manifest.json', manifest)
            write_json_durable(self.root / 'content_receipt.json', {'version': 1, 'semantic_sha256': digest(manifest), **self.binding, 'terminal': self.sequence + 1})
            self.finished = True
        except Exception:
            self.poisoned = True
            raise


def build(context):
    return MilestoneLeaderFollower(context)
