"""Exact bounded portfolio arithmetic, released knowledge and native depth capacity.

No callbacks, files, mutable books, clocks, or execution claims live here.
"""
from fractions import Fraction
from itertools import combinations, product, groupby
import sys


class Capacity:
    def __init__(self):
        self.levels, self.totals = {}, {}

    def eligible(self, source, levels):
        left = max(0, sum(q for _, q in levels) - self.totals.get(source, 0))
        result = []
        for p, q in levels:
            take = min(left, max(0, q - self.levels.get((source, p), 0)))
            if take:
                result.append((p, take)); left -= take
        return tuple(result)

    def consume(self, source, taken):
        for p, q in taken:
            assert q > 0
            self.levels[source, p] = self.levels.get((source, p), 0) + q
            self.totals[source] = self.totals.get(source, 0) + q

    def record(self):
        return [{'source': list(s), 'total_atoms': str(self.totals[s]),
                 'levels': [[str(p), str(q)] for (source,p),q in sorted(self.levels.items()) if source == s]}
                for s in sorted(self.totals)]


class Knowledge:
    """Only released facts. Unknown winners never become implicit zero results."""
    def __init__(self, participants):
        self.participants = participants
        self.results = {}
        self.final_score = None
        self.final_winner = None
        self.phase = 'unavailable'
        self.history = []
        self._determined = {}
        self.contradiction = False

    def apply(self, facts):
        for f in facts:
            kind = f['kind']
            if kind in ('segment_end', 'match_end'):
                self.history.append(dict(f))
                if len(self.history) > 4096: raise ValueError('released fact history bound')
            if kind == 'scheduled':
                self.phase = 'pre_match'
            elif kind == 'segment_start':
                self.phase = 'in_segment'
            elif kind == 'segment_end':
                self.phase = 'between_segments'
                index = f['index']
                winner = f['winner']
                side = self.participants.get(winner) if winner is not None else None
                symbol = 'H' if side == 0 else 'A' if side == 1 else None
                if index in self.results and self.results[index] != symbol:
                    self.contradiction = True
                self.results[index] = symbol
            elif kind == 'match_end':
                self.phase = 'finished'
                score = f['score']
                if set(self.participants.values()) == {0,1}:
                    self.final_score = {('H' if self.participants[s] == 0 else 'A'): score[s] for s in ('home','away')}
                    side = self.participants.get(f['winner'])
                    self.final_winner = 'H' if side == 0 else 'A' if side == 1 else None

    @property
    def prefix(self):
        prefix = []
        while self.results.get(len(prefix)+1) is not None:
            prefix.append(self.results[len(prefix)+1])
        return tuple(prefix)

    @property
    def complete(self):
        return not self.results or len(self.prefix) == max(self.results)

    def feasible(self, keys):
        if self.contradiction:
            return ()
        if not self.results and self.final_score is None:
            return tuple(keys)
        # Non-series semantics are never guessed from game scores.
        if any(not k.startswith('seq:') for k in keys):
            return tuple(keys)
        def admitted(key):
            seq = key[4:]
            for index, winner in self.results.items():
                if winner is not None and (index > len(seq) or seq[index-1] != winner):
                    return False
            if self.final_score is not None:
                if any(seq.count(s) != n for s,n in self.final_score.items()):
                    return False
                if self.final_winner is not None and seq[-1] != self.final_winner:
                    return False
            return True
        return tuple(k for k in keys if admitted(k))

    def determined(self, keys, outcomes):
        identity = frozenset(keys), tuple(outcomes)
        if identity not in self._determined:
            prior = Knowledge(self.participants)
            for time, facts in groupby(self.history, key=lambda f:f['release_ns']):
                prior.apply(list(facts))
                feasible = prior.feasible(outcomes)
                if feasible and len({w in keys for w in feasible}) == 1:
                    self._determined[identity] = time
                    break
        return self._determined.get(identity)

    def record(self):
        return {'released_phase': self.phase, 'released_results': [[i,w] for i,w in sorted(self.results.items())],
                'complete_prefix': ''.join(self.prefix), 'score_complete': self.complete,
                'known_score': {'participant_0':sum(w == 'H' for w in self.results.values()),
                                'participant_1':sum(w == 'A' for w in self.results.values())},
                'final_score': self.final_score, 'contradiction':self.contradiction}


def classify(books, outcomes):
    masks = [set(b['keys']) & set(outcomes) for b in books]
    if len(masks) == 1 and masks[0] == set(outcomes):
        return 'known_payout'
    if all(sum(w in m for m in masks) == 1 for w in outcomes):
        return 'partition'
    if len(masks) == 2 and set.union(*masks) == set(outcomes):
        return 'implication'
    return 'general_overlap'


def solve(books, outcomes, price, *, max_legs, max_nodes, feasible=None, charge=None, value=None,
          alternative_limit=8):
    """Exhaustively enumerate a declared discrete domain, seeded with useful covers.

    Every visited node is a complete vector evaluation (there are no concavity
    assumptions or unproved branch bounds). q=0 is part of the domain. ``price``
    returns native order evidence plus exact cash/retained values, or None.
    ``value`` returns the robust scalar margin and native vectors if supplied.
    """
    books = sorted(books, key=lambda b:b['key'])
    if not outcomes:
        raise ValueError('GAME_CONTRADICTION')
    def zero():
        return {'orders': [], 'margin': Fraction(0), 'cost': Fraction(0),
                'floor':Fraction(0), 'gross_floor':Fraction(0), 'gross_margin':Fraction(0),
                'holding_bound':Fraction(0), 'native_vectors':[], 'classification':'zero'}
    best = zero(); alternatives = []; frontier = []; seen = set(); visited = 0; unknown = set(); cache = {}; retained_bytes = 0
    def storage(value):
        if isinstance(value, dict): return sys.getsizeof(value)+sum(storage(k)+storage(v) for k,v in value.items())
        if isinstance(value, (list,tuple,set,frozenset)): return sys.getsizeof(value)+sum(storage(v) for v in value)
        if isinstance(value, Fraction): return sys.getsizeof(value)+sys.getsizeof(value.numerator)+sys.getsizeof(value.denominator)
        return sys.getsizeof(value)
    complete = True
    def evaluate(vector):
        nonlocal best, visited, complete, retained_bytes
        sparse = tuple((i,q) for i,q in enumerate(vector) if q)
        if sparse in seen:
            return True
        if visited == max_nodes:
            complete = False; return False
        seen.add(sparse); visited += 1
        retained_bytes += storage(sparse)+128
        if retained_bytes > 128*1024*1024: raise ValueError('optimizer retained search state budget')
        chosen = [b for b,q in zip(books,vector) if q]
        orders = []
        for b,q in zip(books,vector):
            if not q: continue
            identity = b['key'],q
            if identity not in cache:
                cache[identity] = price(b,q)
                retained_bytes += storage(cache[identity])+storage(identity)+128
                if retained_bytes > 128*1024*1024: raise ValueError('optimizer retained search state budget')
            order = cache[identity]
            if order is None:
                unknown.add(b['key']); return True
            orders.append(order)
        if feasible is not None and not feasible(orders):
            return True
        cost = -sum((o['cash'] for o in orders), Fraction(0))
        floor = min(sum((o['retained'] for b,o in zip(chosen,orders) if w in b['keys']),Fraction(0)) for w in outcomes)
        gross_floor = min(sum((o.get('gross_quantity',Fraction(o['quantity'])) for b,o in zip(chosen,orders)
                              if w in b['keys']),Fraction(0)) for w in outcomes)
        gross_cost = sum((o.get('gross_cost',-o['cash']) for o in orders),Fraction(0))
        holding = charge(orders) if charge else Fraction(0)
        native_vectors = []
        if value is not None:
            margin,cost,floor,gross_floor,gross_margin,native_vectors = value(chosen,orders,outcomes)
        else:
            margin = floor-cost; gross_margin = gross_floor-gross_cost
        candidate = {'orders':orders,'margin':margin-holding,'cost':cost,'floor':floor,
                     'gross_floor':gross_floor,'gross_margin':gross_margin,'holding_bound':holding,
                     'native_vectors':native_vectors,'classification': classify(chosen,outcomes) if chosen else 'zero'}
        key = lambda c: (-c['margin'],c['cost'],len(c['orders']),tuple((o['key'],o['quantity']) for o in c['orders']))
        if candidate['orders']:
            alternatives.append(candidate); alternatives.sort(key=key); del alternatives[alternative_limit:]
            if not any(c['cost']<=cost and c['margin']>=candidate['margin']for c in frontier):
                frontier[:]=[c for c in frontier if not (cost<=c['cost']and candidate['margin']>=c['margin'])]
                frontier.append(candidate);frontier.sort(key=key);del frontier[alternative_limit:]
        if key(candidate) < key(best): best = candidate
        return True
    n = len(books)
    if not evaluate((0,)*n):
        return None
    # Total masks, exact partitions and two-leg covers at each leg's full cap.
    # Seeds are bounded by the same node budget; duplicate quantity vectors are one node.
    seeds = []
    for i,b in enumerate(books):
        if set(outcomes) <= b['keys']:
            seeds.append((i,))
    for count in range(2,max_legs+1):
        # Seed bounded small supports; ordinary enumeration owns completeness.
        for support in combinations(range(n),count):
            masks = [set(books[i]['keys']) & set(outcomes) for i in support]
            if ((count == 2 and set.union(*masks) == set(outcomes)) or
                all(sum(w in m for m in masks) == 1 for w in outcomes)):
                seeds.append(support)
                if len(seeds) >= max_nodes: break
        if len(seeds) >= max_nodes: break
        # Avoid a combinatorial seed discovery scan: ordinary enumeration below
        # provides completeness; only pair and small-book partitions are seeded.
        if n > 16: break
    for support in seeds:
        vector = tuple((b['cap']//b['increment'])*b['increment'] if i in support else 0 for i,b in enumerate(books))
        if not evaluate(vector): break
    else:
        for count in range(1,max_legs+1):
            stopped = False
            for support in combinations(range(n),count):
                grids = [range(books[i]['increment'], books[i]['cap']+1, books[i]['increment']) for i in support]
                for quantities in product(*grids):
                    vector = [0]*n
                    for i,q in zip(support,quantities): vector[i] = q
                    if not evaluate(tuple(vector)):
                        stopped = True; break
                if stopped: break
            if stopped: break
    return {'best':best, 'alternatives':alternatives,'capital_frontier':sorted(frontier,key=lambda c:(c['cost'],-c['margin'])),
            'frontier_status':'BOUNDED_FEASIBLE_SAMPLES','visited':visited, 'complete':complete,
            'unknown_books':sorted(unknown)}
