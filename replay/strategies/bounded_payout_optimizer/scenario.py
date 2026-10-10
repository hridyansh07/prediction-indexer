"""Strategy-local hypothetical accounts and exact fee/native-payout valuation.

The independent reader checks this output without importing this coordinator.
"""
from fractions import Fraction
from collections import OrderedDict

from replay.economic_fills import walk
from replay.economic_sdk.outcomes import outcome_scope
from replay.strategies.cross_venue_arbitrage.contract import assess_net, asset_row, source_key, UNIT
from replay.streams.protocol import require
from .contract import decimal, cash_key, wire
from .core import Capacity, solve

ECONOMIC_FIELDS=('key','quantity','cash','retained','gross_quantity','gross_cost','source','taken','asset','venue')


class PrimitiveCache:
    """Bounded pinned-fee amounts; never holds a carried assessment identity."""
    def __init__(self,maximum_bytes=16*1024*1024):
        self.maximum_bytes=maximum_bytes;self.retained_bytes=0;self.items=OrderedDict()
    def get(self,key):
        if key not in self.items:return None
        self.items.move_to_end(key);return self.items[key][0]
    def put(self,key,value):
        from replay.preparation import encoded
        size=4*len(encoded(wire([key,value])))+512
        if size>self.maximum_bytes:return
        if key in self.items:self.retained_bytes-=self.items.pop(key)[1]
        while self.items and self.retained_bytes+size>self.maximum_bytes:
            self.retained_bytes-=self.items.popitem(last=False)[1][1]
        self.items[key]=(value,size);self.retained_bytes+=size


def weights(config, assets):
    if config is None:
        if len(set(assets)) != 1: return None
        return [{a:Fraction(1) for a in assets}]
    result = [{cash_key('',r['asset'])[1]:decimal(r['weight']) for r in s['weights']} for s in config['scenarios']]
    return result if all(set(assets) <= set(s) for s in result) else None


def portfolio_value(books, orders, outcomes, valuation):
    assets = [cash_key('',o['asset'])[1] for o in orders]
    valuations = weights(valuation,assets)
    if valuations is None: return None
    native = []
    for venue,asset_id in sorted({cash_key(o['venue'],o['asset']) for o in orders}):
        matching = [(b,o) for b,o in zip(books,orders) if cash_key(o['venue'],o['asset']) == (venue,asset_id)]
        native.append({'venue':venue,'asset':matching[0][1]['asset'],
                       'outcomes':[[w,sum((o['cash']+(o['retained'] if w in b['keys'] else 0)
                                               for b,o in matching),Fraction(0))] for w in outcomes]})
    margins, costs, floors, gross_floors, gross_margins = [],[],[],[],[]
    for ws in valuations:
        cost = sum((-o['cash']*ws[a] for a,o in zip(assets,orders)),Fraction(0))
        payout = [sum((o['retained']*ws[a] for b,o,a in zip(books,orders,assets) if w in b['keys']),Fraction(0)) for w in outcomes]
        gross = [sum((o['gross_quantity']*ws[a] for b,o,a in zip(books,orders,assets) if w in b['keys']),Fraction(0)) for w in outcomes]
        gcost = sum((o['gross_cost']*ws[a] for o,a in zip(orders,assets)),Fraction(0))
        margins.append(min(payout)-cost); costs.append(cost); floors.append(min(payout))
        gross_floors.append(min(gross)); gross_margins.append(min(gross)-gcost)
    return min(margins),max(costs),min(floors),min(gross_floors),min(gross_margins),native


class PortfolioEvaluator:
    """Compile immutable mask/asset arithmetic once; materialize only retained proofs."""
    def __init__(self, books, outcomes, valuation, holding, refresh):
        self.valuation=valuation;self.outcomes=outcomes;self.holding=holding
        self.refresh=refresh
        self.scenarios=weights(valuation,[cash_key('',b['asset'])[1]for b in books])
        self.assets={b['key']:cash_key('',b['asset'])[1]for b in books}
        self.indices={b['key']:tuple(i for i,w in enumerate(outcomes)if w in b['keys'])for b in books}
        # All configured weights and fee/native amounts use finite decimal atoms.
        digits=max((len(r['weight'].partition('.')[2])for s in (valuation or {}).get('scenarios',[])for r in s['weights']),default=0)
        self.scale=10**(36+digits);self.cache={}

    def score(self, books, orders, outcomes):
        costs=[];floors=[];gfloors=[];gmargins=[];margins=[]
        for si,scenario in enumerate(self.scenarios):
            cost=gcost=0;payout=[0]*len(outcomes);gross=[0]*len(outcomes)
            for o in orders:
                identity=o['key'],o['quantity'],si
                if identity not in self.cache:
                    weight=scenario[self.assets[o['key']]]
                    amounts=[-o['cash']*weight,o['retained']*weight,o['gross_quantity']*weight,o['gross_cost']*weight]
                    scaled=[x*self.scale for x in amounts]
                    require(all(x.denominator==1 for x in scaled),'compiled decimal arithmetic')
                    self.cache[identity]=tuple(int(x)for x in scaled)
                c,h,q,gc=self.cache[identity];cost+=c;gcost+=gc
                for i in self.indices[o['key']]:payout[i]+=h;gross[i]+=q
            floor=min(payout);gf=min(gross)
            costs.append(cost);floors.append(floor);gfloors.append(gf);gmargins.append(gf-gcost);margins.append(floor-cost)
        return tuple(Fraction(x,self.scale)for x in (min(margins),max(costs),min(floors),min(gfloors),min(gmargins)))

    def __call__(self,books,orders,outcomes):
        # This remains a separate, easily fault-injected writer proof boundary.
        return portfolio_value(books,orders,outcomes,self.valuation)


def gross_dual_bound(books,outcomes,config,known_weights):
    if known_weights is None:return None
    bounds=[]
    for ws in known_weights:
        contributions=[]
        for b in books:
            first=b['source_levels'][0][0]
            ask=Fraction(10**b['price_scale']-first if b['kalshi']else first,10**b['price_scale'])
            expected=Fraction(sum(w in b['keys']for w in outcomes),len(outcomes))
            contributions.append(max(Fraction(0),expected-ask)*Fraction(b['cap'],10**b['quantity_scale'])*ws[cash_key('',b['asset'])[1]])
        bounds.append(sum(sorted(contributions,reverse=True)[:config['policy']['max_legs']],Fraction(0)))
    return min(bounds)


def gross_outcome_bounds(books,outcomes,known_weights):
    if known_weights is None:return None
    weights=known_weights[0]
    digits=max((len(str(w.denominator))for w in weights.values()),default=1)
    scale=10**(max(b['price_scale']+b['quantity_scale']for b in books)+digits)
    matrix={}
    for b in books:
        first=b['source_levels'][0][0];ask=Fraction(10**b['price_scale']-first if b['kalshi']else first,10**b['price_scale'])
        values=[(int(w in b['keys'])-ask)*weights[cash_key('',b['asset'])[1]]*scale/10**b['quantity_scale']for w in outcomes]
        require(all(v.denominator==1 for v in values),'exact compiled gross branch coefficients')
        matrix[b['key']]=tuple(int(v)for v in values)
    # Repeated outcome incidence vectors impose the same branch inequality.
    columns=sorted({tuple(matrix[b['key']][i]for b in books)for i in range(len(outcomes))})
    return scale,{b['key']:tuple(column[index]for column in columns)for index,b in enumerate(books)}


def price_order(bridge, model, q, time, sequence, scope, experiment, capacity=None, direction='BUY', diagnostics=None):
    """One order; one fee assessment per consumed native level, with no rounding carry across alternatives."""
    levels = model['source_levels'] if direction == 'BUY' else model['bid_levels']
    source = model['source'] if direction == 'BUY' else (*model['key'],'bid')
    eligible = levels if capacity is None else capacity.eligible(source,levels)
    projected = tuple((10**model['price_scale']-p,n) for p,n in eligible) if model['kalshi'] and direction == 'BUY' else eligible
    fill = walk(projected,(q,))[0]
    if fill.depth_limited: return None
    fee_leg = {'market_id':model['market_id'],'key':model['key'],'fill':fill,
               'price_scale':model['price_scale'],'quantity_scale':model['quantity_scale'],'side':direction}
    if direction == 'BUY':
        rows,unknown,assumptions,evidence = assess_net(bridge,experiment=experiment,scope=scope,
            basket={'native_book':list(model['key']),'quantity_atoms':str(q)},size=q,time=time,
            sequence=sequence,fee_legs=(fee_leg,),account='bounded_payout_optimizer_v1')
        row = rows[0]
        if unknown or row['cash'] is None:
            if diagnostics is not None and len(diagnostics)<16:
                diagnostics.append({'key':model['key'],'quantity_atoms':str(q),'reason':'FEE_UNKNOWN','unknowns':unknown,
                    'native_fee_rows':rows,'assumptions':sorted(assumptions),'evidence':sorted(evidence),
                    'gross_cost':Fraction(fill.cost,10**(model['price_scale']+model['quantity_scale']))})
            return None
        cash,retained = Fraction(row['cash'],UNIT),Fraction(row['received'],UNIT)
        ids,charges = row['assessment_ids'],row['charges']
    else:
        from replay.strategies._shared.fee_bridge import FeeEconomicsUnavailable
        try:
            prepared,unknown = bridge.assess_orders(experiment=experiment,scope=scope,
                basket={'native_book':list(model['key']),'quantity_atoms':str(q)},direction='SELL',
                size=q,time=time,sequence=sequence,legs=(fee_leg,),account='bounded_payout_optimizer_v1')
        except FeeEconomicsUnavailable: return None
        if unknown or prepared[0] is None: return None
        e,results=prepared[0]; cash=retained=Fraction(0); ids=[]; charges=[]; assumptions=set(); evidence=set()
        for r in results:
            if r.net_deltas is None or r.unknowns: return None
            ids.append(r.identity); assumptions.update(r.assumptions); evidence.add(r.evidence.value)
            for c in r.charges:
                charges.append({'asset':asset_row(c.amount.asset),'atoms':str(c.amount.amount.atoms),'scale':c.amount.amount.scale,
                    'component':c.component.value,'evidence':c.evidence.value})
            for d in r.net_deltas:
                amount=Fraction(d.atoms,10**d.scale)
                if d.asset == e.quote: cash += amount
                elif d.asset == e.outcome: retained += amount
                else: return None
        if -retained > Fraction(q,10**model['quantity_scale']): return None
    physical = tuple((10**model['price_scale']-p,n) for p,n in fill.taken) if model['kalshi'] and direction == 'BUY' else fill.taken
    gross_quantity=Fraction(q,10**model['quantity_scale'])
    gross_cost=Fraction(fill.cost,10**(model['price_scale']+model['quantity_scale']))
    return {'key':model['key'],'quantity':q,'gross_quantity':gross_quantity,'gross_cost':gross_cost,
            'fee_priced_ns':time,'fee_priced_sequence':sequence,'fee_priced_scope':scope,
            'cash':cash,'retained':retained,'asset':model['asset'],'venue':model['venue'],
            'outcome_asset':model['outcome_asset'],'source':source,'taken':physical,'ask_taken':fill.taken,
            'assessment_ids':ids,'charges':charges,'assumptions':sorted(assumptions),'evidence':sorted(evidence),
            'market_id':model['market_id'],'shape':model['shape'],'keys':sorted(model['keys']),
            'rule_identity':model['rule_identity'],'price_scale':model['price_scale'],
            'quantity_scale':model['quantity_scale'],'quantity_increment_atoms':model['increment']}


def models(snapshot, scope, config, bridge, book_rows, *, require_asks=True):
    masks=outcome_scope(snapshot,scope)
    plans={(p['instrument'],p['orientation']):p for p in snapshot['plans']}
    rows={tuple(r['key']):r for r in book_rows}
    sizes={ (r['instrument'],r['orientation']):r for r in config['policy']['quantities']}
    rules={ (r['instrument'],r['orientation']):r for r in config['rules']['books']}
    result=[]; rejected=[]
    for member in snapshot['scopes'][scope]['members']:
        if not member['capture_selected'] or not member['books']:
            rejected.append({'market_id':member['market_id'],'reason':'NOT_CAPTURED'})
        for book in member['books']:
            key=book['instrument'],book['orientation']; leg=masks.leg(key); source=source_key(key)
            reason=None
            if leg is None: reason=masks.status(key)[0]
            elif key not in rules: reason='RULE_UNSUPPORTED'
            elif key not in sizes: reason='QUANTITY_RULE_UNKNOWN'
            elif source not in rows: reason='NOT_CAPTURED'
            elif rows[source if require_asks else key]['validity'] != 'usable': reason='UNUSABLE'
            elif rows[key]['crossed'] and not key[0].startswith('kalshi:'): reason='SELF_CROSSED_LEG'
            elif bridge.economics(key) is None: reason='ECONOMICS_UNKNOWN'
            if reason:
                rejected.append({'key':list(key),'reason':reason}); continue
            e=bridge.economics(key); plan=plans[key]; qs=int(plan['quantity_scale']); ps=int(plan['price_scale'])
            inc=decimal(sizes[key]['increment'])*10**qs; cap=decimal(sizes[key]['cap'])*10**qs
            if inc.denominator != 1 or cap.denominator != 1:
                rejected.append({'key':list(key),'reason':'UNSUPPORTED_SCALE'}); continue
            source_levels=tuple((int(p),int(q)) for p,q in rows[source]['bid' if key[0].startswith('kalshi:') else 'ask'])
            if not source_levels and require_asks:
                rejected.append({'key':list(key),'reason':'ONE_SIDED'}); continue
            increment=int(inc); maximum=(min(int(cap),sum(q for _,q in source_levels)) if require_asks else int(cap))//increment*increment
            if not maximum:
                rejected.append({'key':list(key),'reason':'DEPTH_LIMITED'}); continue
            result.append({'key':key,'keys':leg.keys,'shape':leg.shape_id,'market_id':leg.market_id,
                           'source':(*source,'bid' if key[0].startswith('kalshi:') else 'ask'),
                           'source_levels':source_levels,'bid_levels':tuple((int(p),int(q)) for p,q in rows[key]['bid']),
                           'kalshi':key[0].startswith('kalshi:'),'price_scale':ps,'quantity_scale':qs,
                           'increment':increment,'cap':maximum,'configured_cap':int(cap),
                           'asset':asset_row(e.quote),'outcome_asset':asset_row(e.outcome),'venue':plan['venue'],
                           'rule_identity':rules[key]['rule_identity']})
    return result,rejected,masks


class Account:
    def __init__(self, config):
        self.config=config; self.capacity=Capacity()
        self.cash={cash_key(r['venue'],r['asset']):decimal(r['amount']) for r in config['account']['initial_cash']}
        self.assets={cash_key(r['venue'],r['asset']):r['asset'] for r in config['account']['initial_cash']}
        self.positions=[]; self.entries=0; self.spent=Fraction(0); self.closed_at=None
        self.signal=False; self.pending=None; self.nonpositive_since=None; self.armed=True; self.knowledge=None

    @property
    def open_positions(self): return [p for p in self.positions if p['state'] == 'OPEN']

    def record(self,time):
        return {'cash':[{'venue':v,'asset':self.assets[v,a],'amount':amount} for (v,a),amount in sorted(self.cash.items())],
                'capacity':self.capacity.record(),'entries':self.entries,'spent':self.spent,
                'positions':[self.position_record(p,time) for p in self.positions]}

    def position_record(self,p,time):
        return {'id':p['id'],'opened_ns':p['opened_ns'],'state':p['state'],'entry':p['entry'],
                'lots':[{k:v for k,v in o.items() if k != 'model'} for o in p['lots']],
                **position_metrics(p,time,self.config),'remaining_payout_vectors':remaining_payouts(p,self.knowledge)}

    def settle(self,time,knowledge):
        actions=[]; settlement=self.config['policy']['settlement']
        for p in self.open_positions:
            for lot in p['lots']:
                if lot['settled']: continue
                if knowledge.contradiction:
                    p['guarantee_valid']=False; continue
                rule=None
                if settlement and settlement['mode'] == 'PINNED_SETTLEMENT':
                    for row in settlement['payouts']:
                        if (row['instrument'],row['orientation']) == tuple(lot['key']):
                            rule=(Fraction(row['payout']),int(row['availability_ns'])); break
                elif settlement:
                    feasible=knowledge.feasible(lot['all_outcomes'])
                    if not feasible:
                        p['guarantee_valid']=False; continue
                    payouts={w in lot['keys'] for w in feasible}
                    if len(payouts) == 1:
                        if lot['determined_ns'] is None: lot['determined_ns']=knowledge.determined(lot['keys'],lot['all_outcomes'])
                        if lot['determined_ns'] is None: continue
                        rule=(Fraction(int(next(iter(payouts)))),lot['determined_ns']+int(settlement['delay_ns']))
                if rule is None: continue
                payout,due=rule; lot['due_ns']=max(p['opened_ns'],due)
                if lot['due_ns'] > time: continue
                key=cash_key(lot['venue'],lot['asset']); credit=lot['retained']*payout
                self.cash[key]=self.cash.get(key,Fraction(0))+credit; self.assets[key]=lot['asset']
                lot.update(settled=True,settled_at=time,credit=credit,remaining_quantity=Fraction(0)); p['pnl'] += credit+lot['cash']
                actions.append({'kind':'SETTLEMENT','position':p['id'],'key':lot['key'],'credit':credit,
                                'payout':payout,'due_ns':lot['due_ns'],'mode':settlement['mode']})
            if all(l['settled'] for l in p['lots']):
                p['state']='CLOSED'; self.closed_at=time
        return actions

    def permits(self,orders):
        if not orders: return True
        cfg=self.config['account']; ws=weights(self.config['valuation'],[cash_key('',o['asset'])[1] for o in orders])
        if ws is None: return False
        costs=[sum((-o['cash']*w[cash_key('',o['asset'])[1]] for o in orders),Fraction(0)) for w in ws]
        cost=max(costs)
        outstanding=outstanding_cost(self.open_positions,self.config)
        if cost > decimal(cfg['transaction_budget']) or cost+self.spent > decimal(cfg['event_budget']) or cost+outstanding > decimal(cfg['max_outstanding_cost']): return False
        if len(self.open_positions) >= cfg['max_open_positions']: return False
        flows={}
        for o in orders:
            key=cash_key(o['venue'],o['asset']); flows[key]=flows.get(key,Fraction(0))-o['cash']
        if any(amount > self.cash.get(k,Fraction(0)) for k,amount in flows.items()): return False
        return True

    def open(self,solution,time,scope,identity,model_by_key,all_outcomes):
        require(self.permits(solution['orders']), 'all-or-none account validation')
        p={'id':identity,'opened_ns':time,'state':'OPEN','entry':solution,'lots':[],'pnl':Fraction(0),'guarantee_valid':True}
        for o in solution['orders']:
            key=cash_key(o['venue'],o['asset']); self.cash[key]+=o['cash']; self.capacity.consume(o['source'],o['taken'])
            ws=weights(self.config['valuation'],[cash_key('',o['asset'])[1]])
            cost_value=max(-o['cash']*w[cash_key('',o['asset'])[1]] for w in ws)
            p['lots'].append({**o,'cost_value':cost_value,'all_outcomes':list(all_outcomes),'determined_ns':None,'due_ns':None,
                              'remaining_quantity':o['retained'],'settled':False,'settled_at':2**64-1,'credit':Fraction(0)})
        self.positions.append(p); self.entries+=1; self.spent+=solution['cost']; self.pending=None; self.armed=False
        return {'kind':'OPEN','position':identity,'portfolio':solution,
                'scenario':'SIMULTANEOUS_DISPLAYED_SCENARIO','trading_status_unknown':True}


def search(config, bridge, snapshot, scope, book_rows, knowledge, time, sequence, identity, account=None, frozen=None, conditioned=False, budget=None, allotment=None, pricing_cache=None):
    all_models,rejected,masks=models(snapshot,scope,config,bridge,book_rows)
    result=[]
    for shape,space in sorted(masks.spaces.items()):
        if space.coverage != 'EXHAUSTIVE':
            result.append({'shape':shape,'status':'INCOMPLETE_SPACE','rejected':rejected}); continue
        selected=[b for b in all_models if b['shape'] == shape]
        outcomes=knowledge.feasible(space.keys) if conditioned else tuple(space.keys)
        if not outcomes:
            result.append({'shape':shape,'status':'GAME_CONTRADICTION','rejected':rejected}); continue
        if len(selected) > config['policy']['max_books'] or len(outcomes) > config['policy']['max_outcomes']:
            result.append({'shape':shape,'status':'DOMAIN_EXCEEDED','rejected':rejected}); continue
        if frozen is not None:
            selected=[{**b,'cap':min(b['cap'],frozen[b['key']])} for b in selected if b['key'] in frozen]
            if {b['key'] for b in selected} != set(frozen):
                result.append({'shape':shape,'status':'PENDING_ADMISSION_CHANGED','rejected':rejected}); continue
        if not selected:
            result.append({'shape':shape,'status':'INPUT_UNAVAILABLE','rejected':rejected}); continue
        allowance=min(config['policy']['max_search_nodes'],budget.remaining if budget else config['policy']['max_search_nodes'])
        if allotment is not None: allowance=min(allowance,allotment)
        if allowance==0:
            result.append({'shape':shape,'status':'SEARCH_BUDGET_EXHAUSTED','rejected':rejected});continue
        if decimal(config['policy']['holding_cost']['rate_per_ns']) and config['policy']['holding_cost']['maximum_duration_ns'] is None:
            result.append({'shape':shape,'status':'HOLDING_COST_UNKNOWN','rejected':rejected}); continue
        pricing_failures={}; blocked={}; fee_diagnostics=[]
        asset_ids={b['key']:cash_key('',b['asset'])[1]for b in selected}
        known_weights=weights(config['valuation'],list(asset_ids.values()))
        duplicated_sources={b['source']for b in selected if sum(m['source']==b['source']for m in selected)>1}
        eligible_sources={b['source']:(account.capacity.eligible(b['source'],b['source_levels'])if account else b['source_levels'])for b in selected}
        from replay.preparation import digest
        source_identities={s:digest(levels)for s,levels in eligible_sources.items()}
        # Current-snapshot fees are immutable at reference_ns. Unpinned/historical
        # fee resolvers cannot reuse amounts across decision times.
        cached=pricing_cache if account is None and bridge._engine.resolver.reference_time is not None else None
        def pricing(b,q):
            cache_key=bridge.engine_identity,scope,b['key'],q,source_identities[b['source']]
            value=cached.get(cache_key)if cached is not None else None
            diagnostics=[]
            if value is None:
                value=price_order(bridge,b,q,time,sequence,scope,identity,account.capacity if account else None,diagnostics=diagnostics)
                if cached is not None and value is not None:
                    cached.put(cache_key,{field:value[field]for field in ECONOMIC_FIELDS})
            fee_diagnostics.extend(diagnostics[:max(0,16-len(fee_diagnostics))])
            if value is None:
                reason='CAPACITY_SHORTFALL' if account is not None and sum(n for _,n in account.capacity.eligible(b['source'],b['source_levels'])) < q else 'FEE_UNKNOWN'
                pricing_failures[reason]=pricing_failures.get(reason,0)+1
            return value
        fresh_orders={}
        def refresh(bs,orders):
            result=[]
            for b,order in zip(bs,orders):
                key=b['key'],order['quantity']
                if key not in fresh_orders:
                    fresh=price_order(bridge,b,order['quantity'],time,sequence,scope,identity,account.capacity if account else None)
                    require(fresh is not None and all(fresh[field]==order[field]for field in ECONOMIC_FIELDS),'cached economic fee/source amounts changed')
                    fresh_orders[key]=fresh
                result.append(fresh_orders[key])
            return result
        def feasible(orders):
            if not orders: return True
            if known_weights is None:
                blocked['VALUATION_UNKNOWN']=blocked.get('VALUATION_UNKNOWN',0)+1;return False
            # Check joint source consumption, including aliases within one transaction.
            for source in duplicated_sources & {o['source'] for o in orders}:
                model=next(b for b in selected if b['source'] == source)
                eligible=account.capacity.eligible(source,model['source_levels']) if account else model['source_levels']
                limits=dict(eligible); used={}
                for o in orders:
                    if o['source'] == source:
                        for p,q in o['taken']: used[p]=used.get(p,0)+q
                if any(q > limits.get(p,0) for p,q in used.items()) or sum(used.values()) > sum(limits.values()):
                    blocked['SHARED_DEPTH_CONFLICT']=blocked.get('SHARED_DEPTH_CONFLICT',0)+1;return False
            if account is None:return True
            allowed=account.permits(orders)
            if not allowed:
                for reason in capital_blocks(account,orders):blocked[reason]=blocked.get(reason,0)+1
            return allowed
        def valuation(bs,orders,keys):
            if not orders: return Fraction(0),Fraction(0),Fraction(0),Fraction(0),Fraction(0),[]
            return portfolio_value(bs,orders,keys,config['valuation'])
        rate=decimal(config['policy']['holding_cost']['rate_per_ns']); duration=int(config['policy']['holding_cost']['maximum_duration_ns'] or 0)
        def holding(orders):
            if not orders:return Fraction(0)
            vals=weights(config['valuation'],[cash_key('',o['asset'])[1] for o in orders])
            return max(sum((-o['cash']*v[cash_key('',o['asset'])[1]] for o in orders),Fraction(0)) for v in vals)*rate*duration
        evaluator=PortfolioEvaluator(selected,outcomes,config['valuation'],rate*duration,refresh)if known_weights is not None else valuation
        dual_bound=gross_dual_bound(selected,outcomes,config,known_weights)
        found=solve(selected,outcomes,pricing,max_legs=config['policy']['max_legs'],max_nodes=allowance,
                    alternative_limit=config['policy']['alternatives'],feasible=feasible,charge=holding,value=evaluator,margin_upper_bound=dual_bound,
                    outcome_bounds=gross_outcome_bounds(selected,outcomes,known_weights))
        if budget: budget.debit(found['visited'])
        qualification=(found['best']['margin'] > 0 and found['best']['margin'] >= decimal(config['policy']['minimum_net_margin']) and
                       10000*found['best']['margin'] >= int(config['policy']['minimum_return_bps'])*found['best']['cost'])
        valuation_unknown=weights(config['valuation'],[cash_key('',b['asset'])[1] for b in selected]) is None
        negative_certificate=None
        if not valuation_unknown:
            negative_certificate={'kind':'UNIFORM_OUTCOME_GROSS_DUAL_BOUND','margin_upper_bound':dual_bound}
        declared={(r['instrument'],r['orientation'])for r in config['policy']['quantities']}
        uncertain=valuation_unknown or bool(found['unknown_books']) or any(tuple(r.get('key',())) in declared and r['reason'] in ('UNUSABLE','ONE_SIDED','DEPTH_LIMITED','ECONOMICS_UNKNOWN','NOT_CAPTURED') for r in rejected)
        status='SEARCH_COMPLETE' if found['complete'] else 'SEARCH_LIMITED'
        certified_negative=negative_certificate is not None and negative_certificate['margin_upper_bound']<=0
        result.append({'shape':shape,'status':status,'economic_status':'POSITIVE' if qualification else 'FEASIBLE_BELOW_ENTRY_THRESHOLD'if found['best']['margin']>0 else 'INPUT_UNKNOWN' if uncertain else 'COMPLETE_NONPOSITIVE' if found['complete']and certified_negative else 'UNCERTIFIED_NONPOSITIVE'if found['complete']else 'LIMITED_NO_POSITIVE',
                       'outcomes':list(outcomes),'fee_diagnostics':fee_diagnostics,'pricing_failures':pricing_failures,'blocked_portfolios':blocked,'valuation_unknown':valuation_unknown,'rejected':rejected,'found':found,'qualifying':qualification,
                       'negative_certificate':negative_certificate,
                       'depth_caps':[{'key':list(b['key']),'quantity_atoms':str(b['cap']),'configured_atoms':str(b['configured_cap'])} for b in selected],
                       'upper_bound':None if valuation_unknown else sum(sorted((Fraction(b['cap'],10**b['quantity_scale'])*(weights(config['valuation'],[cash_key('',b['asset'])[1]]) or [{cash_key('',b['asset'])[1]:Fraction(1)}])[0][cash_key('',b['asset'])[1]] for b in selected),reverse=True)[:config['policy']['max_legs']])})
    if not result: result=[{'shape':None,'status':'OUTCOMES_UNAVAILABLE','rejected':rejected}]
    return result,all_models


def capital_blocks(account,orders):
    cfg=account.config['account'];scenarios=weights(account.config['valuation'],[cash_key('',o['asset'])[1] for o in orders])
    if scenarios is None:return ['VALUATION_UNKNOWN']
    cost=max(sum((-o['cash']*s[cash_key('',o['asset'])[1]] for o in orders),Fraction(0))for s in scenarios)
    reasons=[]
    if cost>decimal(cfg['transaction_budget']):reasons.append('TRANSACTION_BUDGET')
    if cost+account.spent>decimal(cfg['event_budget']):reasons.append('EVENT_BUDGET')
    outstanding=outstanding_cost(account.open_positions,account.config)
    if cost+outstanding>decimal(cfg['max_outstanding_cost']):reasons.append('OUTSTANDING_COST_LIMIT')
    if len(account.open_positions)>=cfg['max_open_positions']:reasons.append('OPEN_POSITION_LIMIT')
    for key in {cash_key(o['venue'],o['asset'])for o in orders}:
        if sum((-o['cash']for o in orders if cash_key(o['venue'],o['asset'])==key),Fraction(0))>account.cash.get(key,0):
            reasons.append('NATIVE_CASH_SHORTFALL');break
    return reasons


def remaining_payouts(position,knowledge):
    lots=position['lots'];outcomes=lots[0]['all_outcomes']
    possible=knowledge.feasible(outcomes)if knowledge else tuple(outcomes)
    if not possible:return {'status':'GAME_CONTRADICTION','vectors':[]}
    vectors=[]
    for venue,asset in sorted({cash_key(l['venue'],l['asset'])for l in lots}):
        owned=[l for l in lots if cash_key(l['venue'],l['asset'])==(venue,asset)]
        vectors.append({'venue':venue,'asset':owned[0]['asset'],'payouts':[[w,sum((l['remaining_quantity']for l in owned if w in l['keys']),Fraction(0))]for w in possible]})
    return {'status':'SUPPORTED_NORMAL_PAYOUTS','vectors':vectors}


def liquidation_marks(config,bridge,account,models,time,sequence,scope,identity):
    by_key={m['key']:m for m in models};marks=[]
    for p in account.positions:
        if p['state'] not in ('OPEN','CENSORED'):continue
        legs=[]
        for lot in p['lots']:
            if lot['settled']:continue
            model=by_key.get(tuple(lot['key']))
            if model is None:legs.append({'key':lot['key'],'status':'UNPRICED_BOOK','net_sale':None});continue
            amount=lot['remaining_quantity']*10**model['quantity_scale']
            if amount.denominator!=1 or int(amount)%model['increment']:
                legs.append({'key':lot['key'],'status':'UNREPRESENTABLE_HOLDINGS','net_sale':None});continue
            order=price_order(bridge,model,int(amount),time,sequence,scope,identity,account.capacity,'SELL')
            if order is None:legs.append({'key':lot['key'],'status':'NO_FULL_DEPTH_OR_UNKNOWN_SELL_FEES','net_sale':None})
            else:legs.append({'key':lot['key'],'status':'PRICED','net_sale':order['cash'],'order':order})
        scenarios=weights(config['valuation'],[cash_key('',l['asset'])[1]for l in p['lots']])
        known=all(l['status']=='PRICED'for l in legs)and scenarios is not None
        value=(min(sum((l['net_sale']*s[cash_key('',l['order']['asset'])[1]]for l in legs),Fraction(0))for s in scenarios)if known else None)
        marks.append({'position':p['id'],'kind':'UNPOSTED_CURRENT_BID_LIQUIDATION_MARK','legs':legs,'net_sale_value':value})
    return marks


def outstanding_cost(positions,config):
    lots=[l for p in positions for l in p['lots']if not l['settled']]
    if not lots:return Fraction(0)
    scenarios=weights(config['valuation'],[cash_key('',l['asset'])[1]for l in lots])
    if scenarios is None:return sum((l['cost_value']for l in lots),Fraction(0))
    return max(sum((-l['cash']*s[cash_key('',l['asset'])[1]]for l in lots),Fraction(0))for s in scenarios)


def position_metrics(p,time,config):
    lots=p['lots'];rate=decimal(config['policy']['holding_cost']['rate_per_ns'])
    ws=weights(config['valuation'],[cash_key('',l['asset'])[1]for l in lots])
    native=[]
    for venue,asset in sorted({cash_key(l['venue'],l['asset'])for l in lots}):
        selected=[l for l in lots if cash_key(l['venue'],l['asset'])==(venue,asset)]
        native.append({'venue':venue,'asset':selected[0]['asset'],
            'closed_lot_cash_pnl':sum((l['credit']+l['cash']for l in selected if l['settled']),Fraction(0)),
            'analytical_holding_charge':sum((-l['cash']*rate*(min(time,l['settled_at'])-p['opened_ns'])for l in selected),Fraction(0))})
    scenarios=[]
    for index,s in enumerate(ws or ()):
        pnl=sum(((l['credit']+l['cash'])*s[cash_key('',l['asset'])[1]]for l in lots if l['settled']),Fraction(0))
        charge=sum((-l['cash']*s[cash_key('',l['asset'])[1]]*rate*(min(time,l['settled_at'])-p['opened_ns'])for l in lots),Fraction(0))
        scenarios.append({'valuation':index,'closed_lot_cash_pnl':pnl,'holding_charge':charge,'adjusted_pnl':pnl-charge})
    closed=p['state']=='CLOSED'
    pnl=min((s['closed_lot_cash_pnl']for s in scenarios),default=None)
    maximum=config['policy']['holding_cost']['maximum_duration_ns']
    expired=bool(rate and maximum is not None and any(min(time,l['settled_at'])-p['opened_ns']>int(maximum)for l in lots))
    return {'cash_pnl':pnl if closed else None,'closed_lot_pnl':pnl,'native_pnl':native,'valuation_pnl':scenarios,
            'holding_charge':max((s['holding_charge']for s in scenarios),default=None),
            'adjusted_closed_pnl':min((s['adjusted_pnl']for s in scenarios),default=None)if closed else None,
            'guarantee_valid':p['guarantee_valid']and not expired,
            'guarantee_exclusions':['HOLDING_DURATION_ASSUMPTION_EXCEEDED']if expired else [],
            'capital_committed':outstanding_cost([p],config)}
