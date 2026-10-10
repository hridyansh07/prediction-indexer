"""Causal as-of coordinates, released knowledge and supported relation proofs."""
from collections import deque
from fractions import Fraction

from replay.preparation import digest
from replay.streams.protocol import require
from .contract import number, r


class History:
    def __init__(self, interval, changes):
        self.interval, self.limit = interval, changes
        self.values = deque()
        self.price_revision = 0
        self.innovation_time = None

    def observe(self, time, coordinate):
        if coordinate is None:
            self.values.clear()
            self.innovation_time = None
            return
        while len(self.values) > 1 and self.values[1][0] <= time - self.interval:
            self.values.popleft()
        if not self.values or self.values[-1][1] != coordinate:
            self.price_revision += 1
            self.innovation_time = time
            require(len(self.values) < self.limit, 'history change bound exceeded')
            self.values.append((time, coordinate, self.price_revision))

    def endpoints(self, time, window):
        if not self.values or self.values[0][0] > time - window:
            return None
        previous = next(x for x in reversed(self.values) if x[0] <= time - window)
        current = self.values[-1]
        return {'previous_ns': str(previous[0]), 'previous': r(previous[1]), 'current_ns': str(current[0]), 'current': r(current[1]), 'delta': r(current[1] - previous[1]), 'price_revision': current[2]}

    def timers(self, window, now):
        return [t + window for t, _, _ in self.values if t + window > now]


class Knowledge:
    def __init__(self):
        self.results = {}
        self.identities = []
        self.match = None
        self.last_result_release = None
        self.contradiction = False
        self.release_states = []

    def release(self, facts, competitors):
        # Retain every released result, including same-time/prologue facts. Details
        # are deliberately never inspected or retained by this strategy.
        for fact in facts:
            ident = digest([fact.kind, fact.release_ns, fact.source_ns, fact.index])
            if fact.kind in ('segment_end', 'match_end'):
                if ident not in self.identities:
                    require(len(self.identities) < 1024, 'released fact history bound')
                    self.identities.append(ident)
                self.last_result_release = fact.release_ns
            if fact.kind == 'segment_end':
                winner = fact.value[0]
                aligned = None if winner is None else competitors[winner]['participant']
                if aligned not in (0, 1):
                    aligned = None
                if fact.index in self.results and self.results[fact.index] != aligned:
                    self.contradiction = True
                self.results[fact.index] = aligned
            elif fact.kind == 'match_end':
                scores = fact.value['score']
                aligned_score = [None, None]
                for side in ('home', 'away'):
                    participant = competitors[side]['participant']
                    if participant in (0, 1):
                        aligned_score[participant] = scores[side]
                self.match = {'score': aligned_score, 'release_ns': fact.release_ns}
            if fact.kind in ('segment_end', 'match_end'):
                state = (fact.release_ns, dict(self.results), None if self.match is None else dict(self.match), tuple(self.identities))
                if self.release_states and self.release_states[-1][0] == fact.release_ns:
                    self.release_states[-1] = state
                else:
                    require(len(self.release_states) < 1024, 'released result state bound')
                    self.release_states.append(state)

    def first_resolution(self, space, claim_keys):
        for time, results, match, identities in self.release_states:
            historical = Knowledge(); historical.results = results; historical.match = match
            feasible = historical.feasible(space)
            if feasible:
                payoffs = {int(key in claim_keys) for key in feasible}
                if len(payoffs) == 1:
                    return time, Fraction(next(iter(payoffs))), digest([sorted(feasible), identities])
        return None

    def state(self, view, now, book_only=False):
        if book_only:
            return {'game': None, 'phase': 'unavailable', 'score': [0, 0], 'score_quality': 'book_only', 'prefix': [], 'prefix_quality': 'book_only', 'released_results': [], 'milestones': [], 'elapsed_ns': '0', 'contradiction': False}
        known = [sum(w == i for w in self.results.values()) for i in (0, 1)]
        prefix = []
        for i in range(1, len(self.results) + 1):
            if self.results.get(i) not in (0, 1):
                break
            prefix.append(self.results[i])
        complete = len(prefix) == len(self.results)
        score = self.match['score'] if self.match is not None and all(x is not None for x in self.match['score']) else known
        quality = 'complete' if complete or self.match is not None and all(x is not None for x in self.match['score']) else 'unknown'
        return {'game': view.game, 'phase': view.phase, 'score': score, 'score_quality': quality, 'prefix': prefix, 'prefix_quality': 'complete' if complete else 'incomplete', 'released_results': [[i, w] for i, w in sorted(self.results.items())], 'milestones': list(self.identities), 'elapsed_ns': str(0 if self.last_result_release is None else now - self.last_result_release), 'contradiction': self.contradiction}

    def feasible(self, space):
        if space.coverage != 'EXHAUSTIVE' or space.scope != 'series' or not all(k.startswith('seq:') for k in space.keys):
            return None
        result = []
        for key in space.keys:
            seq = key[4:]
            if any(w is not None and (i > len(seq) or seq[i - 1] != ('H' if w == 0 else 'A')) for i, w in self.results.items()):
                continue
            if self.match is not None and all(x is not None for x in self.match['score']):
                if [seq.count('H'), seq.count('A')] != self.match['score']:
                    continue
            result.append(key)
        if not result:
            self.contradiction = True
        return frozenset(result)


def relation(leader, target, feasible):
    if feasible is None or not feasible or leader.shape_id != target.shape_id:
        return 'UNSUPPORTED_SCOPE', None
    left, right = leader.keys & feasible, target.keys & feasible
    if not left or not right or left == feasible or right == feasible:
        return 'RESOLVED_OR_TAUTOLOGY', None
    if left == right:
        kind = 'IDENTITY'
    elif not left & right and left | right == feasible:
        kind = 'COMPLEMENT'
    elif left < right:
        kind = 'IMPLICATION'
    elif right < left:
        kind = 'REVERSE_IMPLICATION'
    elif not left & right:
        kind = 'MUTUAL_EXCLUSION'
    else:
        kind = 'OVERLAP'
    # Proof changes only when the transformation changes, not when a result
    # preserves an existing identity. This permits history across such milestones.
    proof = digest([leader.book, target.book, leader.shape_id, kind, sorted(leader.keys), sorted(target.keys)]) if kind in ('IDENTITY', 'COMPLEMENT') else None
    return kind, proof


def cohort(model, state):
    if model['role'] == 'target_only':
        state = {'game': None, 'phase': 'unavailable', 'score_quality': 'book_only', 'prefix_quality': 'book_only', 'score': [0, 0], 'elapsed_ns': '0'}
    found = []
    for index, item in enumerate(model['cohorts']):
        if (item['game'] is None or item['game'] == state['game']) and item['phase'] == state['phase'] and item['score_quality'] == state['score_quality'] and item['prefix_quality'] == state['prefix_quality'] and all(item[k] is None or item[k] == state['score'][i] for i, k in enumerate(('home', 'away'))) and int(item['elapsed_min_ns']) <= int(state['elapsed_ns']) and (item['elapsed_max_ns'] is None or int(state['elapsed_ns']) < int(item['elapsed_max_ns'])):
            found.append(index)
    require(len(found) <= 1, 'ambiguous model cohort')
    fallback = model['fallback']
    if state['score_quality'] == 'unknown' and fallback is not None and model['cohorts'][fallback]['score_quality'] != 'unknown':
        fallback = None
    return found[0] if found else fallback


def displacement(model, index, delta_l, delta_t):
    item = model['cohorts'][index]
    return number(item['beta_L']) * delta_l + number(item['beta_T']) * delta_t + number(item['intercept'])
