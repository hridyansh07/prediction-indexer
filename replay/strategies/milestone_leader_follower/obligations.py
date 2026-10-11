"""Independent completeness checks derived from every committed observer time.

This module imports no runtime, pricing, history or account writer. Native marks
come from the independent reader, after its native book/fee proof checks.
"""
from fractions import Fraction
from itertools import groupby, chain
import json

from replay.economic_sdk.outcomes import outcome_scope
from replay.preparation import digest
from replay.streams.protocol import require
from .contract import number


def settlement(v, position, time):
    policy = v.policy['settlement']
    if policy['mode'] == 'UNRESOLVED':
        return None
    key = position['target']
    if policy['mode'] == 'PINNED_SETTLEMENT':
        evidence = next((x for x in policy['evidence'] if (x['instrument'], x['orientation']) == key), None)
        return None if evidence is None else max(position['opened'], int(evidence['available_ns']))
    if v.policy['cohort'] == 'book_only_comparison':
        return None
    if 'resolution_due' in position:
        return position['resolution_due']
    space = next((outcome_scope(v.snapshot, i).spaces[position['route']['shape_id']] for i in range(len(v.snapshot['scopes'])) if position['route']['shape_id'] in outcome_scope(v.snapshot, i).spaces), None)
    require(space is not None, 'required held claim original space')
    checked = position.get('resolution_checked_time',-1)
    position['resolution_checked_time'] = time
    for _, payload in v.db.execute('SELECT time,payload FROM decisions WHERE time<=? AND time>? ORDER BY time', (time,checked)):
        row = json.loads(payload); known = row['knowledge']
        if known['contradiction'] or not known['milestones']:
            continue
        results = dict(known['released_results'])
        feasible = [k for k in space.keys if all(w is None or i <= len(k[4:]) and k[4:][i-1] == ('H' if w == 0 else 'A') for i, w in results.items()) and (known['phase'] != 'finished' or known['score_quality'] != 'complete' or [k[4:].count('H'), k[4:].count('A')] == known['score'])]
        if feasible and len({int(k in position['route']['target_keys']) for k in feasible}) == 1:
            position['resolution_due'] = max(position['opened'], int(row['time_ns']) - int(known['elapsed_ns']) + int(policy['delay_ns']))
            return position['resolution_due']
    return None


def next_account_decision(v, positions, states, debits, totals, time, knowledge):
    """Independent deadlines after the preceding decision's audited ledger."""
    deadlines = []
    for state in states.values():
        if state['pending'] is not None:
            deadlines.append((state['pending']['due'], 'pending-entry'))
        if state['false_since'] is not None:
            deadlines.append((state['false_since'] + int(v.policy['rearm_false_ns']), 'rearm'))
    rate = number(v.policy['holding_rate_per_ns'])
    for position in positions.values():
        if not position['holding']:
            continue
        due = settlement(v, position, time)
        if due is not None and not knowledge['contradiction']:
            deadlines.append((due, 'settlement'))
        if position['exit_reason'] is not None:
            continue
        deadlines.append((position['horizon'], 'horizon'))
        if rate > 0 and position['basis'] > 0:
            mark = v.mark(position['target'], position['holding'], time, position['route'], debits[position['role']], totals[position['role']])
            if mark is not None:
                remaining = (mark - position['basis'] + position['cost'] * number(v.policy['stop_loss_fraction'])) / (position['basis'] * rate)
                deadline = position['opened'] + max(0, -(-remaining.numerator // remaining.denominator))
                deadlines.append((deadline, 'holding-cost'))
    return min((item for item in deadlines if item[0] > time), default=None)


def required_actions(v, stream, positions, debits, totals):
    """Yield supplied rows only after proving all obligations at that time exist."""
    groups = iter(groupby(stream, key=lambda r: int(r['time_ns'])))
    current = next(groups, None)
    states = {}
    deadline = None
    end = int(v.snapshot['config']['end_ns'])
    times = chain(v.db.execute('SELECT time,payload FROM decisions ORDER BY time'), ((end, None),))
    for time, payload in times:
        require(current is None or current[0] >= time, 'required action belongs to committed observer time')
        require(deadline is None or deadline[0] >= time, 'required ' + ('' if deadline is None else deadline[1]) + ' decision time')
        actions = list(current[1]) if current is not None and current[0] == time else []
        def exists(kind, *, attempt=None, position=None, reason=None):
            return any(r['kind'] in kind and (attempt is None or r['attempt_id'] == attempt) and (position is None or r['position_id'] == position) and (reason is None or r['reason'] == reason) for r in actions)
        decision = None if payload is None else json.loads(payload)
        observations = {} if decision is None else {(r['role'], tuple(r['target'])): r for r in decision['observations']}
        for key, observation in observations.items():
            candidates = json.loads(v.db.execute('SELECT payload FROM candidates WHERE time=? AND role=? AND target=?',(time,key[0],json.dumps(list(key[1])))).fetchone()[0])
            pending = states.get(key,{}).get('pending')
            if pending is not None and key[0] == 'leader_milestones':
                frozen = [c for c in candidates if c['leader'] == pending['route']['leader'] and c['proof'] == pending['route']['proof']]
                candidates = frozen or candidates
                usable = [c for c in frozen if c['available']]
            else:
                usable = [c for c in candidates if c['available']]
            positive = [c for c in usable if c['signal']]
            winner = min(positive or usable,key=lambda c:(-Fraction(c['forecast_net']),-Fraction(c['buy_cash']),c['candidate_id'])) if usable else candidates[0]
            alternates = [{'candidate_id':c['candidate_id'],'status':c['status'],'forecast_net':c['forecast_net']} for c in candidates if c is not winner] if key[0] == 'leader_milestones' else []
            require((None if observation['route'] is None else observation['route']['candidate_id']) == winner['candidate_id'] and observation['alternates'] == alternates,'independent best candidate selection and alternates')
        if decision is not None:
            for ident, p in positions.items():
                if not p['holding']:
                    continue
                due = settlement(v, p, time)
                if due is not None and due <= time and not decision['knowledge']['contradiction']:
                    require(time == due,'required settlement decision time')
                    require(exists(('SETTLED',), position=ident), 'required settlement missing')
                    continue
                reason = p['exit_reason']
                if reason is None:
                    candidates = []
                    if v.policy['cohort'] != 'book_only_comparison':
                        for kind, flag, label in (('match_end', 'exit_on_match_end', 'MATCH_END'), ('segment_end', 'exit_on_next_segment_end', 'SEGMENT_END')):
                            if v.policy[flag] and any(f['kind'] == kind and digest([f['kind'], int(f['release_ns']), int(f['source_ns']), f['index']]) not in p['milestones'] for f in decision['released']):
                                candidates.append(label)
                    if time >= p['horizon']:
                        require(time == p['horizon'],'required horizon decision time')
                        candidates.append('HORIZON')
                    mark = v.mark(p['target'], p['holding'], time, p['route'], debits[p['role']], totals[p['role']])
                    if mark is not None:
                        net = mark - p['basis'] - p['basis'] * number(v.policy['holding_rate_per_ns']) * (time - p['opened'])
                        if net <= -p['cost'] * number(v.policy['stop_loss_fraction']):
                            candidates.append('STOP_LOSS')
                        if net >= p['cost'] * number(v.policy['take_profit_fraction']):
                            candidates.append('TAKE_PROFIT')
                    if candidates:
                        reason = candidates[0]
                        require(exists(('EXIT_LATCHED',), position=ident, reason=reason), 'required exit latch missing')
                if reason is not None:
                    source, bids = v.ladder(p['target'], 'bid', time)
                    fingerprint = (source if bids is not None else None, None if bids is None else tuple(bids), v.fees.engine_identity)
                    if p.get('last_exit_fingerprint') != fingerprint:
                        require(exists(('CLOSED', 'PARTIAL_EXIT', 'EXIT_UNAVAILABLE'), position=ident), 'required exit disposal missing')
        for key in set(states) | set(observations):
            state = states.setdefault(key, {'truth': False, 'false_since': None, 'armed': True, 'last_attempt': None, 'pending': None})
            obs = observations.get(key); truth = None if obs is None else obs['signal']
            if truth is None:
                state['false_since'] = None
            elif truth is False and state['false_since'] is None:
                state['false_since'] = time
            ready = state['false_since'] is not None and time - state['false_since'] >= int(v.policy['rearm_false_ns'])
            if ready:
                state['armed'] = True
            if truth is True:
                state['false_since'] = None
            pending = state['pending']
            if pending is not None:
                changed = obs is None or truth is None or obs['innovation'] != pending['innovation']
                if time == end or changed or pending['due'] <= time:
                    if time == end or changed or truth is not True:
                        require(exists(('CANCELLED',), attempt=pending['id']), 'required pending cancellation missing')
                    else:
                        require(exists(('OPEN', 'SKIPPED'), attempt=pending['id']), 'required delayed entry disposition missing')
                    state['pending'] = None
            prior = state['last_attempt']
            held = [p for p in positions.values() if p['role'] == key[0] and p['target'] == key[1]]
            # Exits precede entries at a committed instant. Supplied closures are
            # still checked independently for native conservation by the caller.
            residual = any(p['holding'] and not exists(('CLOSED', 'SETTLED'), position=ident) for ident, p in positions.items() if p in held)
            closed = [time if exists(('CLOSED', 'SETTLED'), position=ident) else p['closed'] for ident, p in positions.items() if p in held]
            cooldown = all(t is None or time >= t + int(v.policy['cooldown_after_close_ns']) for t in closed)
            newer = obs is not None and obs['innovation'] is not None and (prior is None or int(obs['innovation'][1]) > prior)
            if truth is True and state['armed'] and state['truth'] is not True and state['pending'] is None and (prior is None or ready and newer and cooldown and not residual):
                ident = digest([v.experiment, key, time, obs['innovation']])
                require(exists(('SIGNALLED',), attempt=ident), 'required false-to-true signal missing')
                due = time + int(v.policy['decision_delay_ns'])
                if due > time:
                    require(exists(('PENDING',), attempt=ident), 'required pending entry missing')
                    state['pending'] = {'id': ident, 'innovation': obs['innovation'], 'route':obs['route'], 'due': due}
                else:
                    require(exists(('OPEN', 'SKIPPED'), attempt=ident), 'required immediate entry disposition missing')
                state['armed'] = False; state['last_attempt'] = time
            state['truth'] = truth
        for row in actions:
            yield row
        if decision is not None:
            deadline = next_account_decision(v, positions, states, debits, totals, time, decision['knowledge'])
        if current is not None and current[0] == time:
            current = next(groups, None)
    require(current is None, 'required action after run end')
