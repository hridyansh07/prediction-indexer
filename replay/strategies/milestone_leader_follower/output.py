"""Independent closed-schema reader: native fees, models, conservation and capacity.

The reader never imports the strategy runtime, history writer or position writer.
It streams evidence and uses a disposable SQLite index for cross-file joins.
Recorded books remain reconstruction attestations; this is not another raw replay.
"""
import hashlib
import json
from fractions import Fraction
from collections import OrderedDict
from pathlib import Path
import sqlite3
import tempfile

from replay.economic_fills import Fill
from replay.economic_sdk.game import Timeline
from replay.economic_sdk.outcomes import outcome_scope
from replay.game_state import load as load_game
from replay.economic_sdk.reader import read_json
from replay.preparation import digest, encoded, load_snapshot, sha
from replay.strategies._shared.fee_bridge import FeeBridge, FeeEconomicsUnavailable
from replay.strategy_sdk import PreparedInput, plain
from replay.streams.protocol import decode, obj, require, freeze, uint
from replay.supervisor import initial, read_success
from replay.supervisor import read as read_run

from .contract import STRATEGY, ROLES, configuration, identity, number, rational, r
from .obligations import required_actions
from .wire import Unpacker, compact_observation

NAMES = ('decisions.ndjson', 'actions.ndjson', 'episodes.ndjson', 'predictions.ndjson', 'positions.ndjson', 'denominators.ndjson', 'objects.ndjson')
MAX_LINE = 4 * 1024 * 1024


def raw_rows(root, name, file):
    obj(file, 'sha256 byte_length records'); sha(file['sha256'])
    require(type(file['byte_length']) is int and 0 <= file['byte_length'] <= 512 * 1024 * 1024 and type(file['records']) is int and 0 <= file['records'] <= 1000000, 'file limits')
    path = root / name
    require(path.is_file() and not path.is_symlink(), 'regular output')
    total = count = 0; checksum = hashlib.sha256()
    with path.open('rb') as stream:
        while payload := stream.readline(MAX_LINE + 1):
            require(len(payload) <= MAX_LINE and payload.endswith(b'\n'), 'line bound/truncation')
            total += len(payload); count += 1; checksum.update(payload)
            require(total <= 512 * 1024 * 1024 and count <= 1000000, 'file bound')
            row = decode(payload, MAX_LINE)
            require(type(row['version']) is int and row['version'] == 2, 'row version')
            yield row
    require(file == {'sha256': checksum.hexdigest(), 'byte_length': total, 'records': count}, 'file identity')


def objects(root, file=None):
    path = root / 'objects.ndjson'
    require(path.stat().st_size <= 33*1024*1024,'relationship dictionary bound')
    if file is None:
        with path.open('rb') as stream:
            checksum = hashlib.sha256(); count = total = 0
            for line in stream:
                checksum.update(line); count += 1; total += len(line)
        file = {'sha256':checksum.hexdigest(),'byte_length':total,'records':count}
    result = {}
    for row in raw_rows(root,'objects.ndjson',file):
        obj(row,'version id sha256 value'); require(row['id'] == len(result) and type(row['id']) is int,'dense relationship identities')
        require(row['sha256'] == digest(row['value']),'relationship content identity')
        result[row['id']] = row['value']
    return result


def rows(root, name, file):
    unpacker = Unpacker(objects(root))
    for row in raw_rows(root,name,file):
        if name == 'predictions.ndjson' and 'batch' in row:
            obj(row,'version batch'); require(type(row['batch']) is list and 0 < len(row['batch']) <= 768,'forecast batch bound')
            for item in row['batch']:
                yield unpacker.decode(item)
        else:
            yield unpacker.decode(row)


def manifest_schema(manifest, snapshot):
    obj(manifest, 'version strategy config experiment_sha256 fee_engine_identity settlement_model membership_basis history_complete scenario trading_status financing files' + (' summary_sha256' if 'summary_sha256' in manifest else ''))
    require(type(manifest['version']) is int and manifest['version'] == 2 and manifest['strategy'] == STRATEGY, 'manifest version/strategy')
    config = configuration(manifest['config'], snapshot)
    require(manifest['experiment_sha256'] == identity(config), 'experiment identity')
    require(manifest['settlement_model'] == 'normal_resolution_only' and manifest['scenario'] == 'SIMULTANEOUS_DISPLAYED_SCENARIO' and manifest['trading_status'] == 'trading_status_unknown', 'scenario labels')
    require(manifest['membership_basis'] == snapshot['membership_basis'] and manifest['history_complete'] == snapshot['history_complete'], 'membership limitations')
    require(manifest['financing'] == ('ZERO_FINANCING_COST_SCENARIO' if number(config['policy']['holding_rate_per_ns']) == 0 else 'ANALYTICAL_HOLDING_COST_SCENARIO'), 'holding charges label')
    require(type(manifest['files']) is dict and set(manifest['files']) == set(NAMES), 'closed file set')
    return config


def asset(asset):
    return {'kind': asset.kind.value, 'ledger': asset.chain, 'token': asset.token}


class Verifier:
    def __init__(self, snapshot, config, experiment, db):
        self.snapshot, self.config, self.experiment, self.db = snapshot, config, experiment, db
        self.policy = config['policy']
        self.fees = FeeBridge(config['fees'], snapshot['plans'])
        for item in self.policy['initial_cash']:
            native = self.fees._assets.get(item['venue'])
            require(native is not None and item['asset'] == asset(native), 'cash native asset binding')
        self.plans = {(p['instrument'], p['orientation']): p for p in snapshot['plans']}
        self.rules = {(p['instrument'], p['orientation']): p for p in self.policy['rule_bindings']}
        self.models = {m['role']: m for m in self.policy['models']}
        self.counts = {role: {'decisions': 0, 'no_trade': 0, 'attempts': 0, 'opens': 0, 'full_exits': 0, 'partial_exits': 0, 'no_exits': 0, 'skips': {}, 'forecast_observations': 0, 'forecast_squared_error': Fraction(0), 'counterfactual_sum': Fraction(0)} for role in ROLES}
        self.denoms = {}
        self.episode_expected = {}
        self.book_histories = {}
        self.route_histories = {}
        self.books = OrderedDict()
        self.results = {}; self.milestones = []; self.match_score = None; self.last_result = None; self.contradiction = False
        self.timeline = Timeline(load_game(self.policy['game']['input']['path'], self.policy['game']['input']['sha256'], snapshot), self.policy['game'], int(snapshot['config']['start_ns']))
        self.db.executescript('CREATE TABLE observations(time INTEGER, role TEXT, target TEXT, payload TEXT, PRIMARY KEY(time,role,target)); CREATE TABLE predictions(id TEXT PRIMARY KEY,payload TEXT); CREATE TABLE decisions(time INTEGER PRIMARY KEY,payload TEXT); CREATE TABLE books(time INTEGER,key TEXT,payload TEXT, PRIMARY KEY(time,key)); CREATE TABLE attempts(id TEXT PRIMARY KEY,payload TEXT);')
        self.db.executescript('CREATE INDEX books_asof ON books(key,time DESC); CREATE INDEX observations_asof ON observations(role,target,time DESC);')
        self.db.execute('CREATE TABLE candidates(time INTEGER,role TEXT,target TEXT,payload TEXT,PRIMARY KEY(time,role,target))')
        self.last_time = None
        self.last_knowledge = None
        self.previous = {}
        self.valuation_cache = OrderedDict()
        self.size_cache = OrderedDict()

    def weight(self, key):
        return number(self.policy['valuation']['weights'][self.plans[key]['venue']])

    def book(self, key, time):
        identity = key,time
        if identity in self.books:
            self.books.move_to_end(identity)
            return self.books[identity]
        row = self.db.execute('SELECT payload FROM books WHERE key=? AND time<=? ORDER BY time DESC LIMIT 1', (json.dumps(list(key)), time)).fetchone()
        value = None if row is None else json.loads(row[0])
        self.books[identity] = value
        if len(self.books) > 128:
            self.books.popitem(last=False)
        return value

    def ladder(self, key, side, time):
        physical = key
        source_side = side
        if side == 'ask' and self.plans[key]['venue'] == 'kalshi':
            physical = (key[0], 'complement' if key[1] == 'outcome' else 'outcome'); source_side = 'bid'
        book = self.book(physical, time)
        if book is None or book['validity'] != 'usable':
            return (*physical, source_side), None
        levels = [tuple(x) for x in book[source_side + 's']]
        if physical != key:
            maximum = 10 ** int(self.plans[key]['price_scale'])
            levels = [(maximum - p, q) for p, q in levels]
        return (*physical, source_side), levels

    def midpoint(self, key, time):
        _, bids = self.ladder(key, 'bid', time); _, asks = self.ladder(key, 'ask', time)
        return None if not bids or not asks or bids[0][0] > asks[0][0] else Fraction(bids[0][0] + asks[0][0], 2 * 10 ** int(self.plans[key]['price_scale']))

    def eligible(self, source, levels, debits, totals):
        if self.policy['capacity_mode'] == 'ISOLATED_NONADDITIVE':
            return levels
        room = max(0, sum(q for _, q in levels) - totals.get(source, 0))
        answer = []
        for price, quantity in levels:
            amount = min(room, max(0, quantity - debits.get((source, price), 0)))
            if amount:
                answer.append((price, amount)); room -= amount
        return answer

    def order(self, row, time, *, debits=None, totals=None):
        obj(row, 'key side quantity_atoms price_scale quantity_scale taken consumed cost_atoms source source_taken cash outcome retained_atoms asset assessment_ids assumptions evidence time_ns sequence scope lineage forecast_displacement')
        decision = self.db.execute('SELECT payload FROM decisions WHERE time=?', (time,)).fetchone()
        require(decision is not None, 'order belongs to committed decision')
        decision = json.loads(decision[0])
        expected_time = time + int(self.policy['exit_horizon_ns']) if row['forecast_displacement'] is not None else time
        require(int(uint(row['time_ns'])) == expected_time and row['sequence'] == decision['sequence'] and row['scope'] == decision['scope'], 'order committed time/sequence/scope')
        key = tuple(row['key']); require(key in self.plans and key in self.rules, 'native order admission')
        plan = self.plans[key]; economics = self.fees.economics(key)
        require(economics is not None, 'known fee binding')
        require(row['side'] in ('BUY', 'SELL') and row['price_scale'] == plan['price_scale'] and row['quantity_scale'] == plan['quantity_scale'], 'order native scales')
        size = int(uint(row['quantity_atoms'])); require(size > 0 and size % int(self.rules[key]['quantity_increment_atoms']) == 0, 'native quantity rule')
        source, observed = self.ladder(key, 'ask' if row['side'] == 'BUY' else 'bid', time)
        require(observed is not None and row['source'] == list(source), 'captured usable native source')
        if debits is not None:
            # Capacity is kept in physical source prices, including Kalshi aliases.
            physical = [(10 ** int(plan['price_scale']) - p, q) for p, q in observed] if source[:2] != key else observed
            available = self.eligible(source, physical, debits, totals)
            observed = [(10 ** int(plan['price_scale']) - p, q) for p, q in available] if source[:2] != key else available
        levels = observed
        if row['forecast_displacement'] is not None:
            move = rational(row['forecast_displacement']); maximum = 10 ** int(plan['price_scale']); increment = int(self.rules[key]['price_increment_atoms']); merged = {}
            for p, q in levels:
                rounded = min(Fraction(maximum), max(Fraction(0), p + move * maximum)) // increment * increment
                merged[rounded] = merged.get(rounded, 0) + q
            levels = sorted(merged.items(), reverse=True)
        remaining = size; taken = []; consumed = []
        for p, q in levels:
            if remaining <= 0:
                break
            amount = min(remaining, q); taken.append((p, amount)); consumed.append((p, q)); remaining -= amount
        require(remaining == 0 and row['taken'] == [list(x) for x in taken] and row['consumed'] == [list(x) for x in consumed], 'best-first exact walk')
        cost = sum(p * q for p, q in taken); require(row['cost_atoms'] == str(cost), 'native cost arithmetic')
        expected_source_taken = [(10 ** int(plan['price_scale']) - p, q) for p, q in taken] if source[:2] != key else taken
        require(row['source_taken'] == [list(x) for x in expected_source_taken], 'physical source alias')
        assessed, reasons = self.fees.assess_orders(experiment=self.experiment, scope=row['scope'], basket={'lineage': row['lineage']}, direction=row['side'], size=size, time=int(uint(row['time_ns'])), sequence=row['sequence'], legs=({'market_id': row['lineage']['market_id'], 'key': key, 'fill': Fill(size, cost, False, tuple(taken), tuple(consumed)), 'price_scale': int(plan['price_scale']), 'quantity_scale': int(plan['quantity_scale']), 'side': row['side']},), account=STRATEGY)
        require(not reasons and all(x.net_deltas is not None for x in assessed[0][1]), 'fee known')
        cash = holdings = Fraction(0)
        for result in assessed[0][1]:
            for delta in result.net_deltas:
                value = Fraction(delta.atoms, 10 ** delta.scale)
                if delta.asset == economics.quote:
                    cash += value
                else:
                    require(delta.asset == economics.outcome, 'fee native assets'); holdings += value
        require(row['cash'] == r(cash) and row['outcome'] == r(holdings) and row['asset'] == asset(economics.quote), 'independent fee/native cash arithmetic')
        retained = (holdings if row['side'] == 'BUY' else -holdings) * 10 ** int(plan['quantity_scale'])
        require(retained.denominator == 1 and retained > 0 and row['retained_atoms'] == str(retained.numerator) and retained % int(self.rules[key]['quantity_increment_atoms']) == 0, 'retained quantity exactness')
        require(row['side'] != 'SELL' or retained <= size, 'sell does not debit excess holdings')
        require(row['assessment_ids'] == [x.identity for x in assessed[0][1]] and row['assumptions'] == sorted({a for x in assessed[0][1] for a in x.assumptions}) and row['evidence'] == sorted({x.evidence.value for x in assessed[0][1]}), 'fee assessment identity/evidence')
        return cash, retained.numerator

    def priced(self, key, side, quantity, time, route, *, forecast=None, debits=None, totals=None, status=False):
        """Build native evidence independently from committed inputs, not outputs."""
        def result(order, reason=None):
            return (order, reason) if status else order
        if key not in self.rules or self.fees.economics(key) is None:
            return result(None,'FEE_UNKNOWN')
        if quantity <= 0 or quantity % int(self.rules[key]['quantity_increment_atoms']):
            return result(None,'UNREPRESENTABLE_QUANTITY')
        source, levels = self.ladder(key, 'ask' if side == 'BUY' else 'bid', time)
        if levels is None:
            return result(None,'UNUSABLE')
        plan = self.plans[key]
        if debits is not None:
            physical = [(10 ** int(plan['price_scale'])-p,q) for p,q in levels] if source[:2] != key else levels
            available = self.eligible(source, physical, debits, totals)
            levels = [(10 ** int(plan['price_scale'])-p,q) for p,q in available] if source[:2] != key else available
        if forecast is not None:
            merged = {}; scale = 10 ** int(plan['price_scale']); increment = int(self.rules[key]['price_increment_atoms'])
            for p,q in levels:
                price = min(Fraction(scale), max(Fraction(0), p + forecast*scale)) // increment * increment
                merged[price] = merged.get(price,0) + q
            levels = sorted(merged.items(), reverse=True)
        if not levels:
            return result(None,'NO_EXIT_DEPTH' if side == 'SELL' else 'DEPTH_LIMITED')
        cache_key = None
        if debits is None:
            cache_key = (key,side,quantity,tuple(levels),digest(route),forecast)
            if cache_key in self.valuation_cache:
                self.valuation_cache.move_to_end(cache_key)
                return result(self.valuation_cache[cache_key])
        remaining = quantity; taken = []; consumed = []
        for price, amount in levels:
            use = min(remaining, amount)
            if use:
                taken.append((price,use)); consumed.append((price,amount)); remaining -= use
            if remaining == 0:
                break
        if remaining:
            return result(None,'DEPTH_LIMITED')
        decision = json.loads(self.db.execute('SELECT payload FROM decisions WHERE time=?', (time,)).fetchone()[0])
        native_time = time if forecast is None else time + int(self.policy['exit_horizon_ns'])
        cost = sum(p*q for p,q in taken)
        try:
            assessed, reasons = self.fees.assess_orders(experiment=self.experiment, scope=decision['scope'], basket={'lineage':route}, direction=side, size=quantity, time=native_time, sequence=decision['sequence'], legs=({'market_id':route['market_id'], 'key':key, 'fill':Fill(quantity,cost,False,tuple(taken),tuple(consumed)), 'price_scale':int(plan['price_scale']), 'quantity_scale':int(plan['quantity_scale']), 'side':side},), account=STRATEGY)
        except FeeEconomicsUnavailable:
            return result(None,'FEE_UNKNOWN')
        if reasons or assessed[0] is None or any(x.net_deltas is None for x in assessed[0][1]):
            return result(None,'FEE_UNKNOWN')
        economics, results = assessed[0]
        if any(d.asset not in (economics.quote,economics.outcome) for assessment in results for d in assessment.net_deltas):
            return result(None,'FEE_UNKNOWN')
        cash = sum((Fraction(d.atoms,10**d.scale) for result in results for d in result.net_deltas if d.asset == economics.quote),Fraction(0))
        outcome = sum((Fraction(d.atoms,10**d.scale) for result in results for d in result.net_deltas if d.asset == economics.outcome),Fraction(0))
        retained = (outcome if side == 'BUY' else -outcome) * 10 ** int(plan['quantity_scale'])
        if retained.denominator != 1 or retained <= 0 or retained % int(self.rules[key]['quantity_increment_atoms']):
            return result(None,'UNREPRESENTABLE_HOLDINGS')
        if side == 'SELL' and retained > quantity:
            return result(None,'OUTCOME_DEBIT_EXCEEDS_OWNED')
        order = {'key':list(key),'side':side,'quantity_atoms':str(quantity),'price_scale':plan['price_scale'],'quantity_scale':plan['quantity_scale'],'taken':[list(x) for x in taken],'consumed':[list(x) for x in consumed],'cost_atoms':str(cost),'source':list(source),'source_taken':[[10**int(plan['price_scale'])-p if source[:2]!=key else p,q] for p,q in taken],'cash':r(cash),'outcome':r(outcome),'retained_atoms':str(retained.numerator),'asset':asset(economics.quote),'assessment_ids':[x.identity for x in results],'assumptions':sorted({a for x in results for a in x.assumptions}),'evidence':sorted({x.evidence.value for x in results}),'time_ns':str(native_time),'sequence':decision['sequence'],'scope':decision['scope'],'lineage':route,'forecast_displacement':None if forecast is None else r(forecast)}
        if cache_key is not None:
            self.valuation_cache[cache_key] = order
            if len(self.valuation_cache) > 128:
                self.valuation_cache.popitem(last=False)
        return result(order)

    def sizes(self, target, time, route, forecast, *, debits=None, totals=None, maximum=None):
        """Derive every pinned size and its first unavailable reason independently."""
        cache_key = None
        if debits is None:
            _, asks = self.ladder(target,'ask',time); _, bids = self.ladder(target,'bid',time)
            cache_key = target,None if asks is None else tuple(asks),None if bids is None else tuple(bids),digest(route),forecast,maximum
            if cache_key in self.size_cache:
                self.size_cache.move_to_end(cache_key)
                return self.size_cache[cache_key]
        sizes = []
        for q in self.policy['quantity_grid']:
            quantity = number(q)*10**int(self.plans[target]['quantity_scale'])
            if maximum is not None and quantity.denominator == 1 and quantity > maximum:
                continue
            row = {'quantity':q,'status':'UNREPRESENTABLE_QUANTITY','buy':None,'forecast_sale':None,'initial_sale':None,'forecast_charge':None,'forecast_net':None,'initial_net':None,'qualifies':False}
            sizes.append(row)
            if quantity.denominator != 1:
                continue
            buy, reason = self.priced(target,'BUY',quantity.numerator,time,route,debits=debits,totals=totals,status=True)
            if reason:
                row['status'] = reason; continue
            row['buy'] = {k:buy[k] for k in ('cash','retained_atoms','quantity_atoms')}
            future, reason = self.priced(target,'SELL',int(buy['retained_atoms']),time,route,forecast=forecast,debits=debits,totals=totals,status=True)
            if reason:
                row['status'] = reason; continue
            row['forecast_sale'] = {'cash':future['cash']}
            current, reason = self.priced(target,'SELL',int(buy['retained_atoms']),time,route,debits=debits,totals=totals,status=True)
            if reason:
                row['status'] = 'INITIAL_MARK_'+reason; continue
            cost = -rational(buy['cash']); charge = cost*number(self.policy['holding_rate_per_ns'])*int(self.policy['exit_horizon_ns'])
            net = (rational(future['cash'])-cost-charge)*self.weight(target); mark = (rational(current['cash'])-cost)*self.weight(target)
            qualifies = net > 0 and net >= number(self.policy['minimum_forecast_margin']) and 10000*net >= number(self.policy['minimum_forecast_return_bps'])*cost*self.weight(target) and mark >= -number(self.policy['stop_loss_fraction'])*cost*self.weight(target)
            row.update(status='AVAILABLE',initial_sale={'cash':current['cash'],'taken':[current['taken'][0]]},forecast_charge=r(charge),forecast_net=r(net),initial_net=r(mark),qualifies=qualifies)
        if cache_key is not None:
            self.size_cache[cache_key] = sizes
            if len(self.size_cache) > 128:
                self.size_cache.popitem(last=False)
        return sizes

    def candidate(self, role, target, route, time, knowledge, target_data):
        """Reconstruct a candidate from pinned model and independent histories."""
        model = self.models[role]; leg, feasible, authority = target_data
        state = {'game':None,'phase':'unavailable','score_quality':'book_only','prefix_quality':'book_only','score':[0,0],'elapsed_ns':'0'} if role == 'target_only' else knowledge
        matching = [i for i,item in enumerate(model['cohorts']) if (item['game'] is None or item['game'] == state['game']) and item['phase'] == state['phase'] and item['score_quality'] == state['score_quality'] and item['prefix_quality'] == state['prefix_quality'] and all(item[k] is None or item[k] == state['score'][j] for j,k in enumerate(('home','away'))) and int(item['elapsed_min_ns']) <= int(state['elapsed_ns']) and (item['elapsed_max_ns'] is None or int(state['elapsed_ns']) < int(item['elapsed_max_ns']))]
        index = matching[0] if matching else model['fallback']
        if index is not None and state['score_quality'] == 'unknown' and model['cohorts'][index]['score_quality'] != 'unknown':
            index = None
        target_history = self.book_histories[target,authority]
        target_endpoint = self.endpoints(self.book_histories,(target,authority),time)
        leader_history = None; leader_endpoint = None
        if route is not None and route['proof'] is not None:
            history_key = tuple(route['leader']),target,route['proof']
            leader_history = self.route_histories[history_key]
            leader_endpoint = self.endpoints(self.route_histories,history_key,time)
        row = {'role':role,'target':list(target),'route':route,'model_id':digest(model),'cohort':index,'target_endpoint':target_endpoint,'leader_endpoint':leader_endpoint,'displacement':None,'status':'MODEL_UNAVAILABLE','sizes':[],'selected':None,'signal':None,'innovation':None,'prediction_id':None,'alternates':[]}
        if knowledge['contradiction']:
            row['status'] = 'GAME_CONTRADICTION'; return row
        if index is None or role == 'leader_milestones' and (route is None or route['relation'] not in ('IDENTITY','COMPLEMENT')):
            return row
        if target_endpoint is None or role == 'leader_milestones' and leader_endpoint is None:
            row['status'] = 'HISTORY_BOUND_EXCEEDED' if target_history['overflowed'] or leader_history is not None and leader_history['overflowed'] else 'HISTORY_UNAVAILABLE'
            return row
        item = model['cohorts'][index]; dl = rational(leader_endpoint['delta']) if role == 'leader_milestones' else Fraction(0)
        forecast = number(item['beta_L'])*dl + number(item['beta_T'])*rational(target_endpoint['delta']) + number(item['intercept'])
        row['displacement'] = r(forecast)
        if route is None:
            route = {'leader':None,'target':list(target),'market_id':leg.market_id,'leader_market_id':None,'shape_id':leg.shape_id,'leader_keys':[],'target_keys':sorted(leg.keys),'feasible_keys':sorted(feasible),'relation':'TARGET_BASELINE','proof':None,'rule_ids':[self.rules[target]['rule_sha256']],'candidate_id':digest([role,target])}
            row['route'] = route
        sizes = self.sizes(target,time,route,forecast); row['sizes'] = sizes
        available = [i for i,size in enumerate(sizes) if size['status'] == 'AVAILABLE']
        if available:
            qualified = [i for i in available if sizes[i]['qualifies']]
            selected = min(qualified or available,key=lambda i:(-rational(sizes[i]['forecast_net']),-rational(sizes[i]['buy']['cash']),number(sizes[i]['quantity'])))
            row.update(selected=selected,status='AVAILABLE',signal=sizes[selected]['qualifies'] and (role != 'leader_milestones' or abs(dl) >= number(self.policy['minimum_leader_move'])) and knowledge['phase'] != 'finished')
            history = leader_history if role == 'leader_milestones' else target_history
            current = history['values'][-1]
            row['innovation'] = [route['leader'] if role == 'leader_milestones' else list(target),str(current[0]),current[2],route['proof']]
        else:
            row['status'] = sizes[0]['status'] if sizes else 'SIZE_UNAVAILABLE'
        _, bids = self.ladder(target,'bid',time)
        if bids:
            row['prediction_id'] = digest([self.experiment,role,list(target),time,route['candidate_id']])
        return compact_observation(row)

    def revalidate(self, observation, original, knowledge, time, debits, totals):
        target = original['route']['target']; role = original['role']
        stored = self.db.execute('SELECT payload FROM observations WHERE time=? AND role=? AND target=?',(time,role,json.dumps(target))).fetchone()
        require(stored is not None,'entry repricing committed history exists')
        current = json.loads(stored[0])
        require(all(observation[k] == current[k] for k in ('role','target','route','model_id','cohort','target_endpoint','leader_endpoint')),'entry repricing committed history/model/route')
        self.model(observation,knowledge,time,debits=debits,totals=totals,maximum=int(original['maximum_quantity_atoms']))
        require(observation['selected'] is None or observation['innovation'] == current['innovation'],'entry repricing committed innovation')

    def mark(self, key, owned, time, lineage, debits, totals, *, status=False):
        source, observed = self.ladder(key, 'bid', time)
        if observed is None:
            return (None, 'UNUSABLE') if status else None
        levels = self.eligible(source, observed, debits, totals)
        remaining = owned; taken = []; consumed = []
        for price, quantity in levels:
            amount = min(remaining, quantity)
            if amount:
                taken.append((price, amount)); consumed.append((price, quantity)); remaining -= amount
            if remaining == 0:
                break
        if remaining:
            return (None, 'DEPTH_LIMITED' if levels else 'NO_EXIT_DEPTH') if status else None
        plan = self.plans[key]; decision = json.loads(self.db.execute('SELECT payload FROM decisions WHERE time<=? ORDER BY time DESC LIMIT 1', (time,)).fetchone()[0])
        try:
            assessed, reasons = self.fees.assess_orders(experiment=self.experiment, scope=decision['scope'], basket={'lineage': lineage}, direction='SELL', size=owned, time=time, sequence=decision['sequence'], legs=({'market_id': lineage['market_id'], 'key': key, 'fill': Fill(owned, sum(p*q for p,q in taken), False, tuple(taken), tuple(consumed)), 'price_scale': int(plan['price_scale']), 'quantity_scale': int(plan['quantity_scale']), 'side': 'SELL'},), account=STRATEGY)
        except ValueError:
            return (None, 'FEE_UNKNOWN') if status else None
        if reasons or assessed[0] is None or any(x.net_deltas is None for x in assessed[0][1]):
            return (None, 'FEE_UNKNOWN') if status else None
        quote = assessed[0][0].quote
        cash = sum((Fraction(delta.atoms, 10 ** delta.scale) for result in assessed[0][1] for delta in result.net_deltas if delta.asset == quote), Fraction(0))
        return (cash, 'PRICED') if status else cash

    def model(self, observation, knowledge, time, *, debits=None, totals=None, maximum=None):
        obj(observation, 'role target route model_id cohort target_endpoint leader_endpoint displacement status sizes selected signal innovation prediction_id alternates')
        role = observation['role']; require(role in ROLES and observation['model_id'] == digest(self.models[role]), 'model pin')
        model = self.models[role]
        target = tuple(observation['target']); require(target in self.plans, 'captured target')
        require(observation['signal'] is None or type(observation['signal']) is bool, 'signal truth')
        for endpoint in (observation['target_endpoint'], observation['leader_endpoint']):
            if endpoint is not None:
                obj(endpoint, 'previous_ns previous current_ns current delta price_revision')
                require(int(uint(endpoint['previous_ns'])) <= time - int(self.policy['window_ns']) and int(uint(endpoint['current_ns'])) <= time, 'causal as-of endpoints')
                require(rational(endpoint['current']) - rational(endpoint['previous']) == rational(endpoint['delta']), 'midpoint change arithmetic')
        state = {'game':None,'phase':'unavailable','score_quality':'book_only','prefix_quality':'book_only','score':[0,0],'elapsed_ns':'0'} if role == 'target_only' else knowledge
        matching = [i for i,item in enumerate(model['cohorts']) if (item['game'] is None or item['game'] == state['game']) and item['phase'] == state['phase'] and item['score_quality'] == state['score_quality'] and item['prefix_quality'] == state['prefix_quality'] and all(item[k] is None or item[k] == state['score'][i] for i,k in enumerate(('home','away'))) and int(item['elapsed_min_ns']) <= int(state['elapsed_ns']) and (item['elapsed_max_ns'] is None or int(state['elapsed_ns']) < int(item['elapsed_max_ns']))]
        expected_index = matching[0] if matching else model['fallback']
        if expected_index is not None and state['score_quality'] == 'unknown' and model['cohorts'][expected_index]['score_quality'] != 'unknown':
            expected_index = None
        require(observation['cohort'] == expected_index,'independent cohort availability')
        unsupported = role == 'leader_milestones' and (observation['route'] is None or observation['route']['relation'] not in ('IDENTITY','COMPLEMENT'))
        missing_history = observation['target_endpoint'] is None or role == 'leader_milestones' and observation['leader_endpoint'] is None
        if knowledge['contradiction'] or expected_index is None or unsupported or missing_history:
            overflowed = any(h.get('overflowed') for k,h in self.book_histories.items() if k[0] == target)
            if observation['route'] is not None:
                route = observation['route']
                overflowed = overflowed or self.route_histories.get((tuple(route['leader']) if route['leader'] is not None else None,target,route['proof']),{}).get('overflowed',False)
            expected_status = 'GAME_CONTRADICTION' if knowledge['contradiction'] else 'MODEL_UNAVAILABLE' if expected_index is None or unsupported else 'HISTORY_BOUND_EXCEEDED' if overflowed else 'HISTORY_UNAVAILABLE'
            require(observation['selected'] is None and observation['displacement'] is None and observation['signal'] is None and observation['prediction_id'] is None,'required unavailable observation')
            require(observation['status'] == expected_status,'independent unavailability reason')
            return
        index = observation['cohort']; require(type(index) is int and 0 <= index < len(model['cohorts']), 'cohort index')
        item = model['cohorts'][index]
        state = {'game': None, 'phase': 'unavailable', 'score_quality': 'book_only', 'prefix_quality': 'book_only', 'score': [0, 0], 'elapsed_ns': '0'} if role == 'target_only' else knowledge
        matched = (item['game'] is None or item['game'] == state['game']) and item['phase'] == state['phase'] and item['score_quality'] == state['score_quality'] and item['prefix_quality'] == state['prefix_quality'] and all(item[k] is None or item[k] == state['score'][i] for i, k in enumerate(('home', 'away'))) and int(item['elapsed_min_ns']) <= int(state['elapsed_ns']) and (item['elapsed_max_ns'] is None or int(state['elapsed_ns']) < int(item['elapsed_max_ns']))
        fallback = index == model['fallback'] and (state['score_quality'] != 'unknown' or item['score_quality'] == 'unknown')
        require(matched or fallback, 'released model cohort')
        dl = rational(observation['leader_endpoint']['delta']) if role == 'leader_milestones' else Fraction(0)
        dt = rational(observation['target_endpoint']['delta'])
        forecast = number(item['beta_L']) * dl + number(item['beta_T']) * dt + number(item['intercept'])
        require(observation['displacement'] == r(forecast), 'independent response-model arithmetic')
        expected_sizes = self.sizes(target,time,observation['route'],forecast,debits=debits,totals=totals,maximum=maximum)
        require(len(observation['sizes']) == len(expected_sizes),'complete independent size availability')
        for carried, expected_size in zip(observation['sizes'],expected_sizes):
            obj(carried,'quantity status buy forecast_sale initial_sale forecast_charge forecast_net initial_net qualifies')
            require(type(carried['qualifies']) is bool,'size predicate')
            require(all(carried[k] == expected_size[k] for k in ('quantity','status','forecast_charge','forecast_net','initial_net','qualifies')),'independent size availability and economics')
            for field in ('buy','forecast_sale','initial_sale'):
                actual, expected_order = carried[field], expected_size[field]
                require((actual is None) == (expected_order is None),'independent size availability proof')
                if actual is not None:
                    if 'key' not in actual:
                        obj(actual,{'buy':'cash retained_atoms quantity_atoms','forecast_sale':'cash','initial_sale':'cash taken'}[field])
                    # Compact valuation proof contains cash and holdings only;
                    # complete orders additionally receive their native audit.
                    require(all(actual[k] == expected_order[k] for k in actual if k in ('cash','retained_atoms','quantity_atoms')),'independent size valuation')
                    if 'taken' in actual and 'key' not in actual:
                        require(actual['taken'] == [expected_order['taken'][0]],'compact first-bid proof')
        if observation['selected'] is None:
            require(observation['signal'] is None,'unavailable economic quantity')
            require(not any(s['status'] == 'AVAILABLE' for s in expected_sizes),'independent required available size suppressed')
            require(observation['status'] == (expected_sizes[0]['status'] if expected_sizes else 'SIZE_UNAVAILABLE') and observation['innovation'] is None,'independent unavailable size reason')
            return
        require(observation['status'] == 'AVAILABLE','available economic observation status')
        available = []
        for i, size in enumerate(observation['sizes']):
            obj(size, 'quantity status buy forecast_sale initial_sale forecast_charge forecast_net initial_net qualifies')
            require(size['quantity'] in self.policy['quantity_grid'], 'pinned gross size')
            require(type(size['qualifies']) is bool, 'size predicate')
            if size['status'] != 'AVAILABLE':
                continue
            compact_size = 'key' not in size['buy']
            if compact_size:
                compact = [size[x] for x in ('buy','forecast_sale','initial_sale')]
                buy = self.priced(target,'BUY',int(size['buy']['quantity_atoms']),time,observation['route'],debits=debits,totals=totals)
                require(buy is not None, 'compact native purchase proof')
                future = self.priced(target,'SELL',int(buy['retained_atoms']),time,observation['route'],forecast=forecast,debits=debits,totals=totals)
                current = self.priced(target,'SELL',int(buy['retained_atoms']),time,observation['route'],debits=debits,totals=totals)
                require(future is not None and current is not None, 'compact native liquidation proof')
                expected = [{'cash':buy['cash'],'retained_atoms':buy['retained_atoms'],'quantity_atoms':buy['quantity_atoms']},{'cash':future['cash']},{'cash':current['cash'],'taken':[current['taken'][0]]}]
                require(compact == expected, 'compact independently priced cash/holdings')
                size['buy'],size['forecast_sale'],size['initial_sale'] = buy,future,current
            require(int(size['buy']['quantity_atoms']) == number(size['quantity']) * 10 ** int(self.plans[target]['quantity_scale']) and all(order['key'] == list(target) and order['lineage'] == observation['route'] for order in (size['buy'], size['forecast_sale'], size['initial_sale'])), 'native size and route lineage')
            if compact_size:
                buy = rational(size['buy']['cash']); retained = int(size['buy']['retained_atoms'])
                future = rational(size['forecast_sale']['cash']); current = rational(size['initial_sale']['cash'])
            else:
                buy, retained = self.order(size['buy'], time, debits=debits, totals=totals)
                future, _ = self.order(size['forecast_sale'], time, debits=debits, totals=totals)
                current, _ = self.order(size['initial_sale'], time, debits=debits, totals=totals)
            require(size['forecast_sale']['forecast_displacement'] == observation['displacement'] and (compact_size or int(size['forecast_sale']['time_ns']) == time + int(self.policy['exit_horizon_ns'])), 'forecast bid shift/horizon')
            require(int(size['forecast_sale']['quantity_atoms']) == retained and int(size['initial_sale']['quantity_atoms']) == retained, 'retained holdings valuation')
            cost = -buy; charge = cost * number(self.policy['holding_rate_per_ns']) * int(self.policy['exit_horizon_ns']); net = (future - cost - charge) * self.weight(target); mark = (current - cost) * self.weight(target)
            require(size['forecast_charge'] == r(charge) and size['forecast_net'] == r(net) and size['initial_net'] == r(mark), 'forecast/current net arithmetic')
            qualifying = net > 0 and net >= number(self.policy['minimum_forecast_margin']) and 10000 * net >= number(self.policy['minimum_forecast_return_bps']) * cost * self.weight(target) and mark >= -number(self.policy['stop_loss_fraction']) * cost * self.weight(target)
            require(size['qualifies'] == qualifying, 'economic/initial-loss gate')
            available.append(i)
        qualified = [i for i in available if observation['sizes'][i]['qualifies']]
        require(available and observation['selected'] == min(qualified or available, key=lambda i: (-rational(observation['sizes'][i]['forecast_net']), -rational(observation['sizes'][i]['buy']['cash']), number(observation['sizes'][i]['quantity']))), 'maximum-net eligible size/tie ordering')
        size = observation['sizes'][observation['selected']]
        require(observation['signal'] == (size['qualifies'] and (role != 'leader_milestones' or abs(dl) >= number(self.policy['minimum_leader_move'])) and knowledge['phase'] != 'finished'), 'directional signal')

    def remember(self, store, key, time, value, reset=False):
        if reset or value is None:
            store[key] = {'values': [], 'revision': store.get(key, {}).get('revision', 0),'overflowed':False}
        history = store.setdefault(key, {'values': [], 'revision': 0,'overflowed':False})
        if value is None:
            return
        values = history['values']
        while len(values) > 1 and values[1][0] <= time - int(self.policy['history_ns']):
            values.pop(0)
        if not values or values[-1][1] != value:
            history['revision'] += 1
            if len(values) >= self.policy['history_changes']:
                values.clear()
                history['overflowed'] = True
            values.append((time, value, history['revision']))

    def endpoints(self, store, key, time):
        values = store.get(key, {}).get('values', [])
        cutoff = time - int(self.policy['window_ns'])
        previous = [x for x in values if x[0] <= cutoff]
        if not previous:
            return None
        old, current = previous[-1], values[-1]
        store[key]['overflowed'] = False
        return {'previous_ns': str(old[0]), 'previous': r(old[1]), 'current_ns': str(current[0]), 'current': r(current[1]), 'delta': r(current[1] - old[1]), 'price_revision': current[2]}

    def causal_histories(self, scope_index, time, resets, knowledge):
        scope = outcome_scope(self.snapshot, scope_index)
        targets = {}
        for key in sorted((b['instrument'],b['orientation']) for b in self.snapshot['scopes'][scope_index]['outcome_books']):
            plan = self.plans[key]
            leg = scope.leg(key)
            if leg is None or key not in self.rules or self.fees.economics(key) is None:
                continue
            space = scope.spaces[leg.shape_id]
            if space.coverage != 'EXHAUSTIVE' or space.scope != 'series' or not all(k.startswith('seq:') for k in space.keys):
                continue
            results = {} if self.policy['cohort'] == 'book_only_comparison' else dict(knowledge['released_results'])
            feasible = [k for k in space.keys if all(w is None or i <= len(k[4:]) and k[4:][i - 1] == ('H' if w == 0 else 'A') for i, w in results.items()) and (self.policy['cohort'] == 'book_only_comparison' or knowledge['phase'] != 'finished' or knowledge['score_quality'] != 'complete' or [k[4:].count('H'), k[4:].count('A')] == knowledge['score'])]
            if not feasible or knowledge['contradiction']:
                continue
            authority = digest([key, plan['lane'], plan['price_scale'], plan['quantity_scale'], self.rules[key]])
            reset = any(k == key or k[0] == key[0] and plan['venue'] == 'kalshi' for k in resets)
            self.remember(self.book_histories, (key, authority), time, self.midpoint(key, time), reset)
            targets[key] = (leg, frozenset(feasible), authority)
        active = set(); routes = []
        for target, (tleg, feasible, authority) in sorted(targets.items()):
            for leader, (lleg, _, _) in sorted(targets.items()):
                if lleg.market_id == tleg.market_id or self.rules[leader]['alignment_id'] != self.rules[target]['alignment_id']:
                    continue
                left, right = lleg.keys & feasible, tleg.keys & feasible
                kind = 'UNSUPPORTED_SCOPE' if lleg.shape_id != tleg.shape_id else 'RESOLVED_OR_TAUTOLOGY' if not left or not right or left == feasible or right == feasible else 'IDENTITY' if left == right else 'COMPLEMENT' if not left & right and left | right == feasible else 'IMPLICATION' if left < right else 'REVERSE_IMPLICATION' if right < left else 'MUTUAL_EXCLUSION' if not left & right else 'OVERLAP'
                route = {'leader':list(leader),'target':list(target),'market_id':tleg.market_id,'leader_market_id':lleg.market_id,'shape_id':tleg.shape_id,'leader_keys':sorted(lleg.keys),'target_keys':sorted(tleg.keys),'feasible_keys':sorted(feasible),'relation':kind,'proof':None,'rule_ids':[self.rules[k]['rule_sha256'] for k in (leader,target)]}
                proof = None
                if kind in ('IDENTITY','COMPLEMENT'):
                    proof_base = digest([leader, target, lleg.shape_id, kind, sorted(lleg.keys), sorted(tleg.keys)])
                    proof = digest([proof_base, route['rule_ids'], [self.plans[k]['lane'] for k in (leader, target)]])
                    route['proof'] = proof
                route['candidate_id'] = digest(route); routes.append(route)
                if proof is None:
                    continue
                key = leader, target, proof; active.add(key)
                value = self.midpoint(leader, time)
                if value is not None and kind == 'COMPLEMENT':
                    value = 1 - value
                reset = any(k in (leader, target) or k[0] in (leader[0], target[0]) and self.plans[k]['venue'] == 'kalshi' for k in resets)
                self.remember(self.route_histories, key, time, value, reset)
        self.route_histories = {k: h for k, h in self.route_histories.items() if k in active}
        self.book_histories = {k: h for k, h in self.book_histories.items() if k[0] in targets}
        return targets, routes

    def next_required_decision(self):
        """Derive quiet deadlines from reader-owned inputs and causal histories."""
        now = self.last_time if self.last_time is not None else int(self.snapshot['config']['start_ns']) - 1
        deadlines = [(int(scope['start_ns']), 'scope') for scope in self.snapshot['scopes'][1:]]
        if self.policy['cohort'] != 'book_only_comparison' and self.timeline.next_time is not None:
            deadlines.append((max(int(self.snapshot['config']['start_ns']), self.timeline.next_time), 'game-release'))
        window = int(self.policy['window_ns'])
        for history in (*self.book_histories.values(), *self.route_histories.values()):
            deadlines.extend((sample[0] + window, 'history-window') for sample in history['values'])
        if self.last_result is not None and self.last_knowledge is not None:
            state = self.last_knowledge
            for model in self.policy['models']:
                if model['role'] == 'target_only':
                    continue
                # Elapsed cohorts are relevant only to matching released state
                # with usable causal features. No absent/invalid book invents a
                # timer, and an unrelated cohort cannot demand an observation.
                usable = any(
                    key[0] == model['role'] and observation['target_endpoint'] is not None
                    and (model['role'] != 'leader_milestones' or observation['leader_endpoint'] is not None and observation['route']['proof'] is not None)
                    for key, (_, observation) in self.previous.items()
                )
                if not usable:
                    continue
                for item in model['cohorts']:
                    if (
                        item['game'] is not None and item['game'] != state['game']
                        or any(item[k] != state[k] for k in ('phase','score_quality','prefix_quality'))
                        or any(item[k] is not None and item[k] != state['score'][i] for i,k in enumerate(('home','away')))
                    ):
                        continue
                    deadlines.extend((self.last_result + int(boundary), 'model-elapsed') for boundary in (item['elapsed_min_ns'],item['elapsed_max_ns']) if boundary is not None)
        return min((item for item in deadlines if item[0] > now), default=None)

    @staticmethod
    def check_required_decision(deadline, time):
        require(deadline is None or deadline[0] >= time, 'required ' + ('' if deadline is None else deadline[1]) + ' decision time')

    def decisions(self, row):
        obj(row, 'version time_ns sequence scope knowledge released book_updates history_resets observations admission')
        time = int(uint(row['time_ns'])); require(self.last_time is None or time > self.last_time, 'committed decision order')
        require(int(self.snapshot['config']['start_ns']) <= time < int(self.snapshot['config']['end_ns']), 'decision half-open run')
        deadline = self.next_required_decision()
        scope = row['scope']; require(type(scope) is int and 0 <= scope < len(self.snapshot['scopes']) and int(self.snapshot['scopes'][scope]['start_ns']) <= time < int(self.snapshot['scopes'][scope]['end_ns']), 'decision scope')
        for key, (start, observation) in self.previous.items():
            statuses = self.denoms.setdefault(key, {})
            statuses[observation['status']] = statuses.get(observation['status'], 0) + time - start
        self.previous = {}
        facts = () if self.policy['cohort'] == 'book_only_comparison' else self.timeline.advance(time)
        require(all(f.release_ns == time or f.release_ns <= int(self.snapshot['config']['start_ns']) for f in facts),'required game-release decision time')
        expected = [{'kind': f.kind, 'release_ns': str(f.release_ns), 'source_ns': str(f.source_ns), 'index': f.index, 'winner': f.value[0] if f.kind == 'segment_end' else None, 'score': dict(f.value['score']) if f.kind == 'match_end' else None} for f in facts]
        require(row['released'] == expected, 'every released fact; no future facts')
        for f in facts:
            if f.kind in ('segment_end', 'match_end'):
                ident = digest([f.kind, f.release_ns, f.source_ns, f.index]); self.milestones.append(ident); self.last_result = f.release_ns
            if f.kind == 'segment_end':
                winner = f.value[0]
                aligned = None if winner is None else self.timeline.view.competitors[winner]['participant']
                aligned = aligned if aligned in (0,1) else None
                if f.index in self.results and self.results[f.index] != aligned:
                    self.contradiction = True
                self.results[f.index] = aligned
            if f.kind == 'match_end':
                self.match_score = [None, None]
                for side in ('home', 'away'):
                    participant = self.timeline.view.competitors[side]['participant']
                    if participant in (0, 1):
                        self.match_score[participant] = f.value['score'][side]
        knowledge = obj(row['knowledge'], 'game phase score score_quality prefix prefix_quality released_results milestones elapsed_ns contradiction')
        if self.policy['cohort'] != 'book_only_comparison':
            # Pinned exhaustive series spaces are independent semantic authority;
            # a carried contradiction flag may never suppress the common schedule.
            for space in outcome_scope(self.snapshot,scope).spaces.values():
                if space.coverage != 'EXHAUSTIVE' or space.scope != 'series' or not all(k.startswith('seq:') for k in space.keys):
                    continue
                feasible = [k for k in space.keys if all(w is None or i <= len(k[4:]) and k[4:][i-1] == ('H' if w == 0 else 'A') for i,w in self.results.items()) and (self.match_score is None or any(x is None for x in self.match_score) or [k[4:].count('H'),k[4:].count('A')] == self.match_score)]
                if not feasible:
                    self.contradiction = True
            require(type(knowledge['contradiction']) is bool and knowledge['contradiction'] == self.contradiction,'independent game contradiction')
            prefix = []
            for i in range(1, len(self.results) + 1):
                if self.results.get(i) not in (0, 1):
                    break
                prefix.append(self.results[i])
            counts = [sum(x == i for x in self.results.values()) for i in (0, 1)]
            complete = len(prefix) == len(self.results)
            final = self.match_score is not None and all(x is not None for x in self.match_score)
            require(knowledge['game'] == self.timeline.view.game and knowledge['phase'] == self.timeline.view.phase and knowledge['score'] == (self.match_score if final else counts) and knowledge['score_quality'] == ('complete' if complete or final else 'unknown') and knowledge['prefix'] == prefix and knowledge['prefix_quality'] == ('complete' if complete else 'incomplete') and knowledge['released_results'] == [[i, w] for i, w in sorted(self.results.items())] and knowledge['milestones'] == self.milestones and knowledge['elapsed_ns'] == str(0 if self.last_result is None else time - self.last_result), 'released prefix/score completeness')
        else:
            require(knowledge == {'game': None, 'phase': 'unavailable', 'score': [0, 0], 'score_quality': 'book_only', 'prefix': [], 'prefix_quality': 'book_only', 'released_results': [], 'milestones': [], 'elapsed_ns': '0', 'contradiction': False}, 'separate book-only cohort')
        for book in row['book_updates']:
            obj(book, 'key validity revision bids asks last_change_ns reason')
            key = tuple(book['key']); require(key in self.plans and book['validity'] in ('usable', 'unusable', 'not_initialized'), 'book admission')
            for side, reverse in (('bids', True), ('asks', False)):
                levels = book[side]
                require(type(levels) is list and len(levels) <= 1024 and levels == sorted(levels, reverse=reverse), 'bounded ordered ladder')
                require(len({p for p, q in levels}) == len(levels) and all(type(p) is int and type(q) is int and 0 <= p <= 10 ** int(self.plans[key]['price_scale']) and q > 0 for p, q in levels), 'native ladder levels')
            require(book['validity'] == 'usable' or not book['bids'] and not book['asks'], 'invalid book has no levels')
            self.db.execute('INSERT INTO books VALUES(?,?,?)', (time, json.dumps(book['key']), json.dumps(book)))
        self.db.execute('INSERT INTO decisions VALUES(?,?)', (time, json.dumps(row)))
        admission = obj(row['admission'], 'books captured_books unresolved_members routes unsupported_routes')
        expected_books = [{'key': [x['instrument'], x['orientation']], 'status': x['status'] if x['status'] != 'MASKED' else 'RULE_UNAVAILABLE' if (x['instrument'], x['orientation']) not in self.rules else 'FEE_UNKNOWN' if self.fees.economics((x['instrument'], x['orientation'])) is None else 'GAME_CONTRADICTION' if knowledge['contradiction'] else 'ADMITTED'} for x in self.snapshot['scopes'][scope].get('outcome_books', [])]
        require(admission['books'] == expected_books and admission['unresolved_members'] == len(self.snapshot['scopes'][scope]['unresolved_market_ids']), 'closed admission facts')
        require(type(admission['captured_books']) is int and 0 <= admission['captured_books'] <= self.policy['max_books'] and type(admission['routes']) is int and 0 <= admission['routes'] <= self.policy['max_routes'], 'admission bounds')
        for unsupported in admission['unsupported_routes']:
            obj(unsupported, 'candidate_id relation status'); sha(unsupported['candidate_id'])
            require(unsupported['relation'] not in ('IDENTITY', 'COMPLEMENT') and unsupported['status'] == 'MODEL_UNAVAILABLE', 'explicit unsupported relation')
        scope_model = outcome_scope(self.snapshot, scope)
        targets_history, routes = self.causal_histories(scope, time, [tuple(x) for x in row['history_resets']], knowledge)
        require(admission['routes'] == len(routes) and admission['unsupported_routes'] == [{'candidate_id':r['candidate_id'],'relation':r['relation'],'status':'MODEL_UNAVAILABLE'} for r in routes if r['relation'] not in ('IDENTITY','COMPLEMENT')],'independent candidate admission counts')
        seen = set()
        for observation in row['observations']:
            role = observation['role']; target = tuple(observation['target']); key = role, target
            require(key not in seen, 'one selected leader per target/role'); seen.add(key)
            route = observation['route']
            require(target in targets_history,'independent captured candidate target')
            candidates = [self.candidate(role,target,r,time,knowledge,targets_history[target]) for r in routes if tuple(r['target']) == target] if role == 'leader_milestones' else []
            if not candidates:
                candidates = [self.candidate(role,target,None,time,knowledge,targets_history[target])]
            matching_candidates = [c for c in candidates if c['route'] == route]
            require(len(matching_candidates) == 1,'independent candidate membership')
            expected_candidate = matching_candidates[0]
            selection = [{'candidate_id':None if c['route'] is None else c['route']['candidate_id'],'leader':None if c['route'] is None else c['route']['leader'],'proof':None if c['route'] is None else c['route']['proof'],'status':c['status'],'available':c['selected'] is not None,'signal':c['signal'],'forecast_net':None if c['selected'] is None else c['sizes'][c['selected']]['forecast_net'],'buy_cash':None if c['selected'] is None else c['sizes'][c['selected']]['buy']['cash']} for c in candidates]
            self.db.execute('INSERT INTO candidates VALUES(?,?,?,?)',(time,role,json.dumps(list(target)),json.dumps(selection)))
            if route is not None:
                obj(route, 'leader target market_id leader_market_id shape_id leader_keys target_keys feasible_keys relation proof rule_ids candidate_id')
                require(route['target'] == list(target) and route['rule_ids'] == ([self.rules[tuple(route['leader'])]['rule_sha256'], self.rules[target]['rule_sha256']] if role == 'leader_milestones' else [self.rules[target]['rule_sha256']]), 'route committed rule lineage')
                require(route['candidate_id'] == (digest({k: value for k, value in route.items() if k != 'candidate_id'}) if role == 'leader_milestones' else digest([role, target])), 'candidate identity')
                tleg = scope_model.leg(target); require(tleg is not None and route['target_keys'] == sorted(tleg.keys) and route['market_id'] == tleg.market_id and route['shape_id'] == tleg.shape_id, 'captured target claim')
                space = scope_model.spaces[tleg.shape_id]
                feasible = []
                for outcome in space.keys:
                    seq = outcome[4:]
                    if all(w is None or i <= len(seq) and seq[i-1] == ('H' if w == 0 else 'A') for i, w in self.results.items()) and (self.match_score is None or any(x is None for x in self.match_score) or [seq.count('H'), seq.count('A')] == self.match_score):
                        feasible.append(outcome)
                if self.policy['cohort'] == 'book_only_comparison':
                    feasible = list(space.keys)
                require(route['feasible_keys'] == sorted(feasible), 'causal feasible outcome set')
                if role == 'leader_milestones':
                    leader = tuple(route['leader']); lleg = scope_model.leg(leader)
                    require(lleg is not None and lleg.market_id != tleg.market_id and route['leader_keys'] == sorted(lleg.keys) and lleg.shape_id == tleg.shape_id, 'independent compatible leader')
                    left, right, universe = lleg.keys & set(feasible), tleg.keys & set(feasible), set(feasible)
                    kind = 'RESOLVED_OR_TAUTOLOGY' if not left or not right or left == universe or right == universe else 'IDENTITY' if left == right else 'COMPLEMENT' if not left & right and left | right == universe else 'IMPLICATION' if left < right else 'REVERSE_IMPLICATION' if right < left else 'MUTUAL_EXCLUSION' if not left & right else 'OVERLAP'
                    require(route['relation'] == kind and self.rules[leader]['alignment_id'] == self.rules[target]['alignment_id'], 'relationship proof/rules')
            expected_target_endpoint = self.endpoints(self.book_histories, (target, targets_history[target][2]), time) if target in targets_history else None
            require(observation['target_endpoint'] == expected_target_endpoint, 'independent committed as-of target history')
            if role == 'leader_milestones' and route is not None and route['proof'] is not None:
                expected_leader_endpoint = self.endpoints(self.route_histories, (tuple(route['leader']), target, route['proof']), time)
                require(observation['leader_endpoint'] == expected_leader_endpoint, 'independent transformed history/warm-up')
                require(route['proof'] in {k[2] for k in self.route_histories if k[:2] == (tuple(route['leader']), target)}, 'relationship proof identity')
            self.model(observation, knowledge, time)
            require(all(compact_observation(observation)[k] == expected_candidate[k] for k in expected_candidate if k not in ('alternates','sizes')),'independent candidate state/model/innovation')
            _, prediction_bids = self.ladder(target,'bid',time)
            require((observation['prediction_id'] is not None) == (observation['displacement'] is not None and bool(prediction_bids)),'required common price forecast')
            self.db.execute('INSERT INTO observations VALUES(?,?,?,?)', (time, role, json.dumps(observation['target']), json.dumps(compact_observation(observation))))
            self.counts[role]['decisions'] += 1; self.counts[role]['no_trade'] += observation['signal'] is not True
            self.previous[key] = time, observation
            if observation['prediction_id'] is not None:
                require(observation['prediction_id'] == digest([self.experiment, role, list(target), time, route['candidate_id']]), 'prediction identity')
                self.db.execute('INSERT INTO predictions VALUES(?,?)', (observation['prediction_id'], json.dumps(compact_observation(observation))))
        require(seen == {(role, target) for role in ROLES for target in targets_history} and admission['captured_books'] == len(targets_history), 'complete common model schedule')
        self.check_required_decision(deadline, time)
        self.last_time = time
        self.last_knowledge = knowledge

    def prediction(self, row):
        obj(row, 'version prediction_id role target decision_ns desired_ns observed_ns signal forecast_displacement forecast_net forecast_error counterfactual_net status capacity_mode sale')
        stored = self.db.execute('SELECT payload FROM predictions WHERE id=?', (row['prediction_id'],)).fetchone(); require(stored is not None, 'prediction lineage')
        observation = json.loads(stored[0]); size = None if observation['selected'] is None else observation['sizes'][observation['selected']]
        require(row['role'] == observation['role'] and row['target'] == observation['target'] and row['signal'] == observation['signal'] and row['forecast_displacement'] == observation['displacement'] and row['forecast_net'] == (None if size is None else size['forecast_net']) and row['capacity_mode'] == 'ISOLATED_NONADDITIVE', 'common schedule prediction binding')
        time = int(uint(row['observed_ns'])); decision = int(uint(row['decision_ns'])); due = int(uint(row['desired_ns']))
        require(due == decision + int(self.policy['exit_horizon_ns']) and time == (int(self.snapshot['config']['end_ns']) if row['status'] == 'RUN_END' else due), 'forecast causal horizon')
        end = int(self.snapshot['config']['end_ns'])
        if due < end:
            require(self.db.execute('SELECT 1 FROM decisions WHERE time=?',(due,)).fetchone() is not None, 'required forecast-horizon decision time')
        expected_sale = None
        if due >= end:
            expected_status = 'RUN_END'
        elif size is None:
            expected_status = 'PRICE_ONLY'
        else:
            expected_sale, reason = self.priced(tuple(row['target']),'SELL',int(size['buy']['retained_atoms']),time,observation['route'],status=True)
            expected_status = reason or 'OBSERVED'
        require(row['status'] == expected_status and (row['sale'] is None) == (expected_sale is None),'independent counterfactual availability')
        _, bids = self.ladder(tuple(row['target']), 'bid', time)
        _, initial_bids = self.ladder(tuple(row['target']), 'bid', decision)
        error = None if row['status'] == 'RUN_END' or not bids else Fraction(bids[0][0] - initial_bids[0][0], 10 ** int(self.plans[tuple(row['target'])]['price_scale'])) - rational(row['forecast_displacement'])
        require(row['forecast_error'] == (None if error is None else r(error)), 'independent price forecast error')
        if error is not None:
            stats = self.counts[row['role']]; stats['forecast_observations'] += 1; stats['forecast_squared_error'] += error * error
        if row['sale'] is not None:
            require(size is not None,'price-only forecast has no hypothetical sale')
            cash, _ = self.order(row['sale'], time)
            target = tuple(row['target']); scale = 10 ** int(self.plans[target]['price_scale']); cost = -rational(size['buy']['cash'])
            error = Fraction(row['sale']['taken'][0][0] - size['initial_sale']['taken'][0][0], scale) - rational(row['forecast_displacement'])
            net = (cash - cost - cost * number(self.policy['holding_rate_per_ns']) * (time - decision)) * self.weight(target)
            require(row['forecast_error'] == r(error) and row['counterfactual_net'] == r(net), 'counterfactual/error arithmetic')
            stats = self.counts[row['role']]; stats['counterfactual_sum'] += net
        else:
            require(row['counterfactual_net'] is None, 'unavailable counterfactual is not profit')
        self.db.execute('DELETE FROM predictions WHERE id=?', (row['prediction_id'],))


def validate_content(directory, snapshot, manifest):
    root = Path(directory); snapshot = plain(snapshot); config = manifest_schema(manifest, snapshot)
    with tempfile.TemporaryDirectory(prefix='leader-reader-') as scratch:
        db = sqlite3.connect(str(Path(scratch) / 'index.sqlite3'))
        try:
            verifier = Verifier(snapshot, config, manifest['experiment_sha256'], db)
            objects(root,manifest['files']['objects.ndjson'])
            require(manifest['fee_engine_identity'] == verifier.fees.engine_identity, 'fee engine identity')
            for row in rows(root, 'decisions.ndjson', manifest['files']['decisions.ndjson']):
                verifier.decisions(row)
            end = int(snapshot['config']['end_ns'])
            verifier.check_required_decision(verifier.next_required_decision(), end)
            for key, (start, observation) in verifier.previous.items():
                statuses = verifier.denoms.setdefault(key, {})
                statuses[observation['status']] = statuses.get(observation['status'], 0) + end - start
            for row in rows(root, 'predictions.ndjson', manifest['files']['predictions.ndjson']):
                verifier.prediction(row)
            require(db.execute('SELECT COUNT(*) FROM predictions').fetchone()[0] == 0, 'every forecast has observed/censored outcome')
            summary = verify_actions(verifier, root, manifest)
            verify_intervals(verifier, root, manifest)
            return summary
        finally:
            db.close()


def verify_actions(v, root, manifest):
    policy = v.policy
    cash = {role: {x['venue']: number(x['amount']) for x in policy['initial_cash']} for role in ROLES}
    positions = {}; attempts = {}; debit = {role: {} for role in ROLES}; totals = {role: {} for role in ROLES}
    entries = {role: 0 for role in ROLES}; spends = {role: Fraction(0) for role in ROLES}; same_time_closed = set()
    last = -1
    prior_attempt = {}
    base = 'version role time_ns kind reason attempt_id position_id'
    for row in required_actions(v, rows(root, 'actions.ndjson', manifest['files']['actions.ndjson']), positions, debit, totals):
        role = row['role']; require(role in ROLES, 'action role'); time = int(uint(row['time_ns'])); require(time >= last, 'action order'); last = time
        kind = row['kind']; attempt = row['attempt_id']; sha(attempt)
        extras = {'SIGNALLED': ' innovation route maximum_quantity_atoms due_ns baseline_predictions', 'PENDING': '', 'CANCELLED': '', 'SKIPPED': ' revalidation' if 'revalidation' in row else '', 'OPEN': ' order ledger revalidation knowledge horizon_ns', 'EXIT_LATCHED': '', 'EXIT_UNAVAILABLE': '', 'CLOSED': ' order ledger lateness_ns', 'PARTIAL_EXIT': ' order ledger lateness_ns', 'SETTLED': ' ledger payout payout_per_contract settlement_identity'}
        require(kind in extras, 'closed action kind'); obj(row, base + extras[kind])
        if kind == 'SIGNALLED':
            require(attempt not in attempts, 'one immutable attempt')
            target = row['route']['target']
            observed = v.db.execute('SELECT payload FROM observations WHERE time=? AND role=? AND target=?', (time, role, json.dumps(target))).fetchone()
            require(observed is not None, 'signal observed schedule')
            observation = json.loads(observed[0]); require(observation['signal'] is True and row['innovation'] == observation['innovation'] and row['route'] == observation['route'], 'selected economic signal')
            require(attempt == digest([v.experiment, (role, tuple(target)), time, row['innovation']]) and int(row['due_ns']) == time + int(policy['decision_delay_ns']), 'attempt frozen identity/timer')
            require(row['maximum_quantity_atoms'] == observation['sizes'][observation['selected']]['buy']['quantity_atoms'], 'intended maximum quantity')
            baseline_predictions = {}
            for baseline_role, payload in v.db.execute('SELECT role,payload FROM observations WHERE time=? AND target=?', (time, json.dumps(target))):
                baseline = json.loads(payload)
                baseline_predictions[baseline_role] = {k: baseline[k] for k in ('model_id', 'prediction_id', 'status', 'displacement', 'signal')}
                baseline_predictions[baseline_role]['forecast_net'] = None if baseline['selected'] is None else baseline['sizes'][baseline['selected']]['forecast_net']
            require(row['baseline_predictions'] == baseline_predictions, 'committed baseline predictions')
            previous_observed = v.db.execute('SELECT payload FROM observations WHERE role=? AND target=? AND time<? ORDER BY time DESC LIMIT 1', (role, json.dumps(target), time)).fetchone()
            require(previous_observed is None or json.loads(previous_observed[0])['signal'] is not True, 'committed false-to-true transition')
            signal_key = role, tuple(target)
            if signal_key in prior_attempt:
                prior = prior_attempt[signal_key]
                require(int(row['innovation'][1]) > prior['time'], 're-entry later actual price innovation')
                previous_rows = v.db.execute('SELECT time,payload FROM decisions WHERE time<? AND time>=? ORDER BY time DESC', (time, prior['time']))
                false_start = time
                for prior_time, payload in previous_rows:
                    previous_signal = next((o for o in json.loads(payload)['observations'] if o['role'] == role and o['target'] == target), None)
                    if previous_signal is None or previous_signal['signal'] is not False:
                        break
                    false_start = prior_time
                require(time - false_start >= int(policy['rearm_false_ns']), 'continuous false signal rearm')
                held = [p for p in positions.values() if p['role'] == role and p['target'] == tuple(target)]
                require(not any(p['holding'] for p in held) and all(p['closed'] is None or time >= p['closed'] + int(policy['cooldown_after_close_ns']) for p in held), 're-entry residual/cooldown')
            prior_attempt[signal_key] = {'row': row, 'time': time}
            attempts[attempt] = {'row': row, 'time': time, 'state': 'SIGNALLED'}
            v.counts[role]['attempts'] += 1
            continue
        require(attempt in attempts and attempts[attempt]['row']['role'] == role, 'action attempt lineage')
        if kind == 'PENDING':
            require(attempts[attempt]['state'] == 'SIGNALLED' and time == attempts[attempt]['time'] and int(policy['decision_delay_ns']) > 0, 'pending lifecycle'); attempts[attempt]['state'] = 'PENDING'; continue
        if kind in ('SKIPPED', 'CANCELLED'):
            require(attempts[attempt]['state'] in ('SIGNALLED', 'PENDING'), 'attempt cancellation lifecycle'); attempts[attempt]['state'] = kind
            original = attempts[attempt]['row']; target = tuple(original['route']['target'])
            if kind == 'SKIPPED':
                require(time == int(original['due_ns']), 'skipped attempt due time')
                if row['reason'] == 'SAME_TIME_EXIT':
                    require((role, target, time) in same_time_closed, 'same-time exit skip')
                else:
                    revalidation = row.get('revalidation'); require(revalidation is not None, 'skip repricing evidence')
                    knowledge = json.loads(v.db.execute('SELECT payload FROM decisions WHERE time=?', (time,)).fetchone()[0])['knowledge']
                    v.revalidate(revalidation,original,knowledge,time,debit[role],totals[role])
                    if revalidation['signal'] is not True or revalidation['innovation'] != original['innovation'] or not int(policy['decision_delay_ns']) and int(revalidation['sizes'][revalidation['selected']]['buy']['quantity_atoms']) != int(original['maximum_quantity_atoms']):
                        expected = 'CAPACITY_OR_PREDICATE'
                    else:
                        order = revalidation['sizes'][revalidation['selected']]['buy']; key = tuple(order['key']); venue = v.plans[key]['venue']; cost = -rational(order['cash']); scalar = cost * v.weight(key)
                        checks = [('ISOLATED_DETECTION_ONLY', policy['capacity_mode'] == 'ISOLATED_NONADDITIVE'), ('TARGET_RESIDUAL', any(p['role'] == role and p['target'] == key and p['holding'] for p in positions.values())), ('MAX_ENTRIES', entries[role] >= policy['max_entries_per_event']), ('MAX_CONCURRENT', sum(p['role'] == role and p['holding'] > 0 for p in positions.values()) >= policy['maximum_concurrent_positions']), ('INSUFFICIENT_CASH', cash[role].get(venue, Fraction(0)) < cost), ('TRANSACTION_BUDGET', scalar > number(policy['transaction_budget'])), ('EVENT_BUDGET', spends[role] + scalar > number(policy['event_budget'])), ('OUTSTANDING_COST', scalar + sum(p['basis'] * v.weight(p['target']) for p in positions.values() if p['role'] == role and p['holding']) > number(policy['maximum_outstanding_cost']))]
                        expected = next((name for name, failed in checks if failed), None)
                    require(row['reason'] == expected, 'independent skipped entry gate')
            elif row['reason'] == 'RUN_END':
                require(time == int(v.snapshot['config']['end_ns']), 'run-end cancellation')
            else:
                current = v.db.execute('SELECT payload FROM observations WHERE time=? AND role=? AND target=?', (time, role, json.dumps(list(target)))).fetchone()
                require(current is None or json.loads(current[0])['signal'] is not True or json.loads(current[0])['innovation'] != original['innovation'], 'pending cancellation evidence')
            v.counts[role]['skips'][row['reason']] = v.counts[role]['skips'].get(row['reason'], 0) + 1
            continue
        if kind == 'OPEN':
            require(attempts[attempt]['state'] in ('SIGNALLED', 'PENDING') and time == int(attempts[attempt]['row']['due_ns']), 'entry anchored due time')
            order = row['order']; key = tuple(order['key']); venue = v.plans[key]['venue']
            require(key == tuple(attempts[attempt]['row']['route']['target']) and int(order['quantity_atoms']) <= int(attempts[attempt]['row']['maximum_quantity_atoms']) and (int(policy['decision_delay_ns']) > 0 or order['quantity_atoms'] == attempts[attempt]['row']['maximum_quantity_atoms']), 'frozen long-only target/size')
            require(not any(p['target'] == key and p['role'] == role and p['holding'] > 0 for p in positions.values()) and (role, key, time) not in same_time_closed, 'no pyramiding or same-time re-entry')
            money, retained = v.order(order, time, debits=debit[role], totals=totals[role]); require(money < 0 and order['side'] == 'BUY' and order['forecast_displacement'] is None, 'actual hypothetical buy')
            cost = -money; scalar = cost * v.weight(key)
            require(policy['capacity_mode'] != 'ISOLATED_NONADDITIVE', 'isolated detection cannot publish additive positions')
            require(entries[role] < policy['max_entries_per_event'] and sum(p['holding'] > 0 and p['role'] == role for p in positions.values()) < policy['maximum_concurrent_positions'], 'entry/concurrency limits')
            require(cash[role].get(venue, Fraction(0)) >= cost and scalar <= number(policy['transaction_budget']) and spends[role] + scalar <= number(policy['event_budget']) and scalar + sum(p['basis'] * v.weight(p['target']) for p in positions.values() if p['role'] == role and p['holding']) <= number(policy['maximum_outstanding_cost']), 'cash/budget gates')
            require(row['position_id'] == digest([attempt, time, key]), 'position identity')
            require(row['ledger'] == {'cash_before': r(cash[role][venue]), 'cash_after': r(cash[role][venue] + money), 'holdings_before': '0', 'holdings_after': str(retained), 'allocated_basis': r(cost), 'lot_pnl': '0', 'holding_charge': '0'}, 'opening cash/holding conservation')
            observed = v.db.execute('SELECT payload FROM decisions WHERE time=?', (time,)).fetchone(); require(observed is not None and row['knowledge'] == json.loads(observed[0])['knowledge'], 'opening knowledge')
            require(row['horizon_ns'] == str(time + int(policy['exit_horizon_ns'])), 'entry-anchored horizon')
            repriced = row['revalidation']
            v.revalidate(repriced,attempts[attempt]['row'],row['knowledge'],time,debit[role],totals[role])
            current = v.db.execute('SELECT payload FROM observations WHERE time=? AND role=? AND target=?', (time, role, json.dumps(list(key)))).fetchone()
            require(current is not None, 'entry current observation')
            current = json.loads(current[0])
            require(repriced['target_endpoint'] == current['target_endpoint'] and repriced['leader_endpoint'] == current['leader_endpoint'] and repriced['model_id'] == current['model_id'], 'delayed entry committed endpoints/model')
            require(order['lineage'] == repriced['route'] == current['route'], 'actual order route lineage')
            require(repriced['signal'] is True and repriced['innovation'] == attempts[attempt]['row']['innovation'] and repriced['sizes'][repriced['selected']]['buy'] == order, 'frozen pending innovation revalidation')
            positions[row['position_id']] = {'role': role, 'attempt': attempt, 'target': key, 'opened': time, 'horizon': int(row['horizon_ns']), 'opening': retained, 'holding': retained, 'cost': cost, 'basis': cost, 'pnl': Fraction(0), 'charge': Fraction(0), 'exit_reason': None, 'exit_time': None, 'closed': None, 'milestones': row['knowledge']['milestones'], 'route': order['lineage'], 'baseline_predictions': attempts[attempt]['row']['baseline_predictions']}
            attempts[attempt]['state'] = 'OPEN'; entries[role] += 1; spends[role] += scalar; cash[role][venue] += money; v.counts[role]['opens'] += 1
        else:
            ident = row['position_id']; require(ident in positions and positions[ident]['role'] == role and positions[ident]['attempt'] == attempt, 'owned position lineage')
            position = positions[ident]; require(position['holding'] > 0, 'closed holdings cannot transact')
            key = position['target']; venue = v.plans[key]['venue']
            if kind == 'EXIT_LATCHED':
                require(position['exit_reason'] is None and row['reason'] in ('MATCH_END', 'SEGMENT_END', 'HORIZON', 'STOP_LOSS', 'TAKE_PROFIT'), 'irrevocable exit latch')
                decision = json.loads(v.db.execute('SELECT payload FROM decisions WHERE time=?', (time,)).fetchone()[0])
                reasons = []
                for fact_kind, flag, label in (('match_end', 'exit_on_match_end', 'MATCH_END'), ('segment_end', 'exit_on_next_segment_end', 'SEGMENT_END')):
                    if policy[flag] and any(f['kind'] == fact_kind and digest([f['kind'], int(f['release_ns']), int(f['source_ns']), f['index']]) not in position['milestones'] for f in decision['released']):
                        reasons.append(label)
                if time >= position['horizon']:
                    reasons.append('HORIZON')
                mark = v.mark(key, position['holding'], time, position['route'], debit[role], totals[role])
                if mark is not None:
                    net = mark - position['basis'] - position['basis'] * number(policy['holding_rate_per_ns']) * (time - position['opened'])
                    if net <= -position['cost'] * number(policy['stop_loss_fraction']):
                        reasons.append('STOP_LOSS')
                    if net >= position['cost'] * number(policy['take_profit_fraction']):
                        reasons.append('TAKE_PROFIT')
                require(reasons and row['reason'] == reasons[0], 'independent actual exit trigger/tie priority')
                position['exit_reason'], position['exit_time'] = row['reason'], time
                continue
            if kind in ('EXIT_UNAVAILABLE', 'CLOSED', 'PARTIAL_EXIT'):
                source, observed_bids = v.ladder(key, 'bid', time)
                fingerprint = (source if observed_bids is not None else None, None if observed_bids is None else tuple(observed_bids), v.fees.engine_identity)
                require(position.get('last_exit_fingerprint') != fingerprint, 'no identical-state residual retry')
                position['last_exit_fingerprint'] = fingerprint
                expected_sale = None
                if observed_bids is None:
                    unavailable_reason = 'UNUSABLE'
                else:
                    eligible_bids = v.eligible(source,observed_bids,debit[role],totals[role])
                    increment = int(v.rules[key]['quantity_increment_atoms'])
                    quantity = min(position['holding'],sum(q for _,q in eligible_bids))//increment*increment
                    if not quantity:
                        unavailable_reason = 'NO_EXIT_DEPTH'
                    else:
                        expected_sale, unavailable_reason = v.priced(key,'SELL',quantity,time,position['route'],debits=debit[role],totals=totals[role],status=True)
                require((kind == 'EXIT_UNAVAILABLE') == (expected_sale is None),'independent exit availability')
                if kind == 'EXIT_UNAVAILABLE':
                    require(row['reason'] == unavailable_reason,'independent exit unavailable reason')
                else:
                    require(row['order']['quantity_atoms'] == expected_sale['quantity_atoms'],'independent maximum exit quantity')
            if kind == 'EXIT_UNAVAILABLE':
                require(position['exit_reason'] is not None, 'unavailable exit keeps holding'); v.counts[role]['no_exits'] += 1; continue
            if kind == 'SETTLED':
                require(policy['settlement']['mode'] == row['reason'] and row['reason'] != 'UNRESOLVED', 'declared settlement mode')
                payout = rational(row['payout_per_contract']); money = rational(row['payout']); sold = position['holding']
                require(money == payout * Fraction(sold, 10 ** int(v.plans[key]['quantity_scale'])), 'normal native payout arithmetic')
                if row['reason'] == 'PINNED_SETTLEMENT':
                    evidence = next((x for x in policy['settlement']['evidence'] if (x['instrument'], x['orientation']) == key), None)
                    require(evidence is not None and time == max(position['opened'], int(evidence['available_ns'])) and payout == number(evidence['payout']) and row['settlement_identity'] == evidence['evidence_sha256'], 'pinned payout availability')
                else:
                    first = None
                    for _, payload in v.db.execute('SELECT time,payload FROM decisions WHERE time<=? ORDER BY time', (time,)):
                        decision = json.loads(payload); knowledge = decision['knowledge']
                        if knowledge['contradiction'] or not knowledge['milestones']:
                            continue
                        space = next((outcome_scope(v.snapshot, i).spaces[position['route']['shape_id']] for i in range(len(v.snapshot['scopes'])) if position['route']['shape_id'] in outcome_scope(v.snapshot, i).spaces), None)
                        require(space is not None, 'held claim original space')
                        results = dict(knowledge['released_results'])
                        feasible = [k for k in space.keys if all(w is None or i <= len(k[4:]) and k[4:][i-1] == ('H' if w == 0 else 'A') for i, w in results.items()) and (knowledge['phase'] != 'finished' or knowledge['score_quality'] != 'complete' or [k[4:].count('H'), k[4:].count('A')] == knowledge['score'])]
                        payoffs = {int(k in position['route']['target_keys']) for k in feasible}
                        if len(payoffs) == 1:
                            first = (int(decision['time_ns']) - int(knowledge['elapsed_ns']), next(iter(payoffs)), digest([sorted(feasible), knowledge['milestones']]))
                            break
                    require(first is not None and payout == first[1] and time == max(position['opened'], first[0] + int(policy['settlement']['delay_ns'])) and row['settlement_identity'] == first[2], 'first supported payout availability/identity')
                position['exit_reason'], position['exit_time'] = 'SETTLEMENT', time
            else:
                require(position['exit_reason'] is not None and row['reason'] == position['exit_reason'], 'exit reason stays latched')
                order = row['order']; require(order['key'] == list(key) and order['side'] == 'SELL' and order['forecast_displacement'] is None, 'sell actually owned captured orientation')
                source, levels = v.ladder(key, 'bid', time); require(levels is not None, 'usable exit depth')
                available = min(position['holding'], sum(q for _, q in v.eligible(source, levels, debit[role], totals[role])))
                increment = int(v.rules[key]['quantity_increment_atoms'])
                require(int(order['quantity_atoms']) == available // increment * increment, 'largest eligible residual sale')
                money, sold = v.order(order, time, debits=debit[role], totals=totals[role]); require(0 < sold <= position['holding'], 'long-only sell')
                require(row['lateness_ns'] == str(max(0, time - position['horizon'])), 'residual elapsed horizon')
            before_h = position['holding']; basis = position['basis'] * Fraction(sold, before_h); charge = basis * number(policy['holding_rate_per_ns']) * (time - position['opened'])
            require(row['ledger'] == {'cash_before': r(cash[role][venue]), 'cash_after': r(cash[role][venue] + money), 'holdings_before': str(before_h), 'holdings_after': str(before_h - sold), 'allocated_basis': r(basis), 'lot_pnl': r(money - basis), 'holding_charge': r(charge)}, 'FIFO proportional basis/cash/holding conservation')
            position['holding'] -= sold; position['basis'] -= basis; position['pnl'] += money - basis; position['charge'] += charge; cash[role][venue] += money
            if position['holding'] == 0:
                require(kind in ('CLOSED', 'SETTLED'), 'full position closure only without residual'); position['closed'] = time; same_time_closed.add((role, key, time)); v.counts[role]['full_exits'] += 1
            else:
                require(kind == 'PARTIAL_EXIT', 'partial holdings remain open'); v.counts[role]['partial_exits'] += 1
        if kind in ('OPEN', 'CLOSED', 'PARTIAL_EXIT') and policy['capacity_mode'] == 'CUMULATIVE_DISPLAY_CAP':
            source = tuple(row['order']['source'])
            for price, quantity in row['order']['source_taken']:
                debit[role][source, price] = debit[role].get((source, price), 0) + quantity; totals[role][source] = totals[role].get(source, 0) + quantity
    seen = set(); outcomes = {role: [] for role in ROLES}; residuals = {role: 0 for role in ROLES}
    for row in rows(root, 'positions.ndjson', manifest['files']['positions.ndjson']):
        obj(row, 'version role position_id attempt_id target market_id claim_keys shape_id state opened_ns horizon_ns exit_reason exit_time_ns closed_ns holding_atoms opening_atoms opening_cost residual_basis closed_lot_pnl holding_charge hypothetical_closed_pnl holding_cost_adjusted_closed_pnl residual_holding_charge milestones baseline_predictions liquidation_mark mark_status')
        ident = row['position_id']; require(ident in positions and ident not in seen, 'position final lineage'); seen.add(ident); p = positions[ident]; role = p['role']
        require(row['role'] == role and row['attempt_id'] == p['attempt'] and row['target'] == list(p['target']) and row['holding_atoms'] == str(p['holding']) and row['opening_atoms'] == str(p['opening']) and row['opening_cost'] == r(p['cost']) and row['residual_basis'] == r(p['basis']) and row['closed_lot_pnl'] == r(p['pnl']) and row['holding_charge'] == r(p['charge']) and row['opened_ns'] == str(p['opened']) and row['horizon_ns'] == str(p['horizon']) and row['milestones'] == p['milestones'] and row['baseline_predictions'] == p['baseline_predictions'] and row['exit_reason'] == p['exit_reason'] and row['exit_time_ns'] == (None if p['exit_time'] is None else str(p['exit_time'])), 'final position conservation/lifecycle')
        require(row['state'] == ('CLOSED' if p['holding'] == 0 else 'CENSORED') and row['closed_ns'] == (None if p['closed'] is None else str(p['closed'])), 'censored residuals')
        require(row['hypothetical_closed_pnl'] == (r(p['pnl']) if not p['holding'] else None) and row['holding_cost_adjusted_closed_pnl'] == (r(p['pnl'] - p['charge']) if not p['holding'] else None), 'no forecast/open holding reported as closed profit')
        require(row['residual_holding_charge'] == r(p['basis'] * number(policy['holding_rate_per_ns']) * (int(v.snapshot['config']['end_ns']) - p['opened'])), 'residual analytical charge')
        if p['holding']:
            mark, mark_status = v.mark(p['target'], p['holding'], int(v.snapshot['config']['end_ns']), p['route'], debit[role], totals[role], status=True)
            require(row['liquidation_mark'] == (None if mark is None else r(mark)) and row['mark_status'] == mark_status, 'independent censored liquidation mark')
            residuals[role] += 1
        else:
            require(row['liquidation_mark'] == '0' and row['mark_status'] == 'CLOSED', 'closed liquidation mark')
            outcomes[role].append((p['pnl'] - p['charge']) * v.weight(p['target']))
    require(seen == set(positions), 'every position retained')
    require(all(a['state'] not in ('SIGNALLED', 'PENDING') for a in attempts.values()), 'every attempt has committed disposition')
    event = v.snapshot.get('outcomes', {}).get('document', {}).get('event_id')
    report = []
    for role in ROLES:
        stats = v.counts[role]; count = stats['forecast_observations']; values = sorted(outcomes[role])
        native = [{'venue': venue, 'asset': item['asset'], 'initial_cash': item['amount'], 'ending_cash': r(cash[role][venue]), 'native_cash_change': r(cash[role][venue] - number(item['amount']))} for item in policy['initial_cash'] for venue in (item['venue'],)]
        report.append({'role': role, 'event_id': event, **{k: value for k, value in stats.items() if k not in ('forecast_squared_error', 'counterfactual_sum')}, 'forecast_mse': None if count == 0 else r(stats['forecast_squared_error'] / count), 'counterfactual_nonadditive_sum': r(stats['counterfactual_sum']), 'censored_residuals': residuals[role], 'closed_position_net_total': r(sum(values, Fraction(0))), 'wins': sum(x > 0 for x in values), 'losses': sum(x < 0 for x in values), 'closed_position_distribution': [r(x) for x in values], 'tail_min': None if not values else r(values[0]), 'capital_used': r(spends[role]), 'native_cash': native, 'capacity_debits': [{'source': list(source), 'total': str(totals[role][source]), 'prices': [[str(p), str(q)] for (s, p), q in sorted(debit[role].items()) if s == source]} for source in sorted(totals[role])]})
    return {'version': 2, 'strategy': STRATEGY, 'experiment_sha256': v.experiment, 'rows': report, 'comparison': {'schedule': 'COMMON_UNION_OBSERVABLE_TRIGGERS', 'accounts': 'INDEPENDENT_NOT_ADDITIVE', 'prediction_counterfactuals': 'ISOLATED_NONADDITIVE', 'event_clusters': 1, 'event_clustered_uncertainty': 'UNAVAILABLE_SINGLE_EVENT', 'models_tried': policy['models_tried'], 'thresholds_tried': policy['thresholds_tried'], 'empirical_leadership_verdict': 'NO_CORPUS_INFERENCE'}, 'normal_resolution_only': True, 'membership_basis': v.snapshot['membership_basis'], 'history_complete': v.snapshot['history_complete']}


def verify_intervals(v, root, manifest):
    actual = {}
    for row in rows(root, 'denominators.ndjson', manifest['files']['denominators.ndjson']):
        obj(row, 'version role target status_ns'); key = row['role'], tuple(row['target']); require(key not in actual, 'denominator identity')
        require(type(row['status_ns']) is dict and all(int(uint(x)) > 0 for x in row['status_ns'].values()), 'positive denominator time')
        actual[key] = {k: int(x) for k, x in row['status_ns'].items()}
    require(actual == v.denoms, 'independent measurable/admission duration arithmetic')
    expected = {}; active = {}
    for time, payload in v.db.execute('SELECT time,payload FROM decisions ORDER BY time'):
        observations = {(o['role'], tuple(o['target'])): o for o in json.loads(payload)['observations']}
        for key in list(active):
            observation = observations.get(key)
            if observation is None or observation['signal'] is not True:
                start = active.pop(key)
                if time > start:
                    reason = 'UNAVAILABLE' if observation is None or observation['signal'] is None else 'PREDICATE_FALSE'
                    expected[key, start] = (time, reason)
        for key, observation in observations.items():
            if observation['signal'] is True and key not in active:
                active[key] = time
    end = int(v.snapshot['config']['end_ns'])
    for key, start in active.items():
        if end > start:
            expected[key, start] = (end, 'RUN_END')
    episodes = {}; actual_episodes = {}
    for row in rows(root, 'episodes.ndjson', manifest['files']['episodes.ndjson']):
        obj(row, 'version role target episode_id start_ns end_ns end_reason censored')
        start, end = int(uint(row['start_ns'])), int(uint(row['end_ns'])); require(start < end, 'positive episode duration')
        key = row['role'], tuple(row['target']); require(row['episode_id'] == digest([v.experiment, key[0], key[1], start]), 'detection episode identity')
        require(end <= int(v.snapshot['config']['end_ns']) and row['censored'] == (row['end_reason'] == 'RUN_END'), 'episode censoring')
        previous = episodes.get(key, -1); require(start >= previous, 'nonoverlapping detections'); episodes[key] = end
        require((key, start) not in actual_episodes, 'unique episode')
        actual_episodes[key, start] = (end, row['end_reason'])
        observations = v.db.execute('SELECT time,payload FROM observations WHERE role=? AND target=? AND time>=? AND time<? ORDER BY time', (key[0], json.dumps(list(key[1])), start, end))
        first = next(observations,None)
        require(first is not None and first[0] == start and json.loads(first[1])['signal'] is True and all(json.loads(payload)['signal'] is True for _, payload in observations), 'episode covers only actual positive economic signal')

    require(actual_episodes == expected, 'every maximal positive episode')

def read_provisional(directory, snapshot_directory, *, expected_sha256, fee_catalog_directory=None, game_state_path=None):
    root = Path(directory); snapshot = load_snapshot(snapshot_directory, expected_sha256=expected_sha256)
    manifest = read_json(root / 'manifest.json'); require(manifest['config']['snapshot_sha256'] == expected_sha256, 'snapshot binding')
    receipt = obj(read_json(root / 'content_receipt.json'), 'version semantic_sha256 run_id attempt_id group identity terminal')
    require(type(receipt['version']) is int and receipt['version'] == 2 and receipt['semantic_sha256'] == digest(manifest), 'content receipt identity')
    actual_manifest = json.loads(json.dumps(manifest))
    if fee_catalog_directory is not None:
        actual_manifest['config']['fees']['catalog_directory'] = str(fee_catalog_directory)
    if game_state_path is not None:
        actual_manifest['config']['policy']['game']['input']['path'] = str(game_state_path)
    summary = validate_content(root, snapshot, actual_manifest)
    require(read_json(root / 'summary.json') == summary and manifest['summary_sha256'] == digest(summary), 'summary identity')
    return {'receipt': receipt, 'manifest': manifest, 'summary': summary}


def read_completed(run_directory, group):
    root = Path(run_directory); success = read_success(root); run = read_run(root / 'run.json')
    require(group in success['outputs'], 'unknown completed group'); spec = run['strategies'][group]
    require(spec['factory'] == 'replay.strategies.milestone_leader_follower:build', 'factory binding')
    prepared = PreparedInput({k: spec['config'][k] for k in ('version', 'snapshot_directory', 'snapshot_sha256')}); prepared.bind(freeze(initial(run)))
    result = read_provisional(root / success['outputs'][group], spec['config']['snapshot_directory'], expected_sha256=spec['config']['snapshot_sha256'])
    require(result['manifest']['config'] == configuration(spec['config'], plain(prepared.snapshot)), 'configured scenario binding')
    receipt = result['receipt']; require({k: receipt[k] for k in ('run_id', 'attempt_id', 'group', 'identity', 'terminal')} == {'run_id': run['transport']['run_id'], 'attempt_id': success['attempt'], 'group': group, 'identity': success['identity'], 'terminal': success['terminal']}, 'supervisor content binding')
    return result


def check(*, run_directory, group, output_directory, context_directory):
    result = read_completed(run_directory, group)
    return {'passed': True, 'details': {'experiment_sha256': result['manifest']['experiment_sha256'], 'independent_model_fee_cash_capacity_read': True}}
