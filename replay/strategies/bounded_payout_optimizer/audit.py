"""Independent economic certificates. No writer/search/valuation imports.

Shared FeeBridge, native walking and pinned outcome masks are input primitives.
Only a separately checked dual bound certifies a nonpositive economic result.
"""
from fractions import Fraction
from itertools import groupby
from replay.economic_fills import walk
from replay.economic_sdk.outcomes import outcome_scope
from replay.strategies.cross_venue_arbitrage.contract import asset_row, source_key
from replay.strategies._shared.fee_bridge import FeeEconomicsUnavailable
from replay.streams.protocol import obj, require
from .contract import decimal, cash_key, wire


class Capacity:
    def __init__(self):self.levels={};self.totals={}
    def eligible(self,source,levels):
        available=max(0,sum(q for _,q in levels)-self.totals.get(source,0));result=[]
        for price,displayed in levels:
            q=min(available,max(0,displayed-self.levels.get((source,price),0)))
            if q:result.append((price,q));available-=q
        return tuple(result)
    def consume(self,source,taken):
        for p,q in taken:
            require(q>0,'independent positive capacity debit')
            self.levels[source,p]=self.levels.get((source,p),0)+q
            self.totals[source]=self.totals.get(source,0)+q
    def record(self):
        return [{'source':list(s),'total_atoms':str(self.totals[s]),'levels':[[str(p),str(q)]for(ss,p),q in sorted(self.levels.items())if ss==s]}for s in sorted(self.totals)]


class Knowledge:
    def __init__(self,participants):
        self.participants=participants;self.history=[];self.results={};self.phase='unavailable';self.final_score=None;self.contradiction=False
    def apply(self,facts):
        for f in facts:
            self.phase={'scheduled':'pre_match','segment_start':'in_segment','segment_end':'between_segments','match_end':'finished'}.get(f['kind'],self.phase)
            if f['kind']not in ('segment_end','match_end'):continue
            self.history.append(dict(f));require(len(self.history)<=4096,'independent released fact bound')
            if f['kind']=='segment_end':
                winner={0:'H',1:'A'}.get(self.participants.get(f['winner']))
                if f['index']in self.results and self.results[f['index']]!=winner:self.contradiction=True
                self.results[f['index']]=winner
            elif set(self.participants.values())=={0,1}:
                self.final_score={('H'if self.participants[s]==0 else'A'):f['score'][s]for s in ('home','away')}
    def feasible(self,outcomes):
        if self.contradiction:return ()
        if not all(k.startswith('seq:')for k in outcomes):return tuple(outcomes)
        final=next((f for f in reversed(self.history)if f['kind']=='match_end'),None)
        final_side=self.participants.get(final['winner'])if final else None
        possible=[]
        for key in outcomes:
            seq=key[4:]
            if any(w is not None and(i>len(seq)or seq[i-1]!=w)for i,w in self.results.items()):continue
            if self.final_score is not None and any(seq.count(w)!=n for w,n in self.final_score.items()):continue
            if final_side is not None and seq[-1]!={0:'H',1:'A'}[final_side]:continue
            possible.append(key)
        if not possible:self.contradiction=True
        return tuple(possible)
    def record(self):
        prefix=''
        while self.results.get(len(prefix)+1)is not None:prefix+=self.results[len(prefix)+1]
        return {'released_phase':self.phase,'released_results':[[i,w]for i,w in sorted(self.results.items())],
            'complete_prefix':prefix,'score_complete':not self.results or len(prefix)==max(self.results),
            'known_score':{'participant_0':sum(w=='H'for w in self.results.values()),'participant_1':sum(w=='A'for w in self.results.values())},
            'final_score':self.final_score,'contradiction':self.contradiction}
    def determined(self,claim,outcomes):
        prior=Knowledge(self.participants)
        for time,facts in groupby(self.history,key=lambda f:f['release_ns']):
            prior.apply(list(facts));possible=prior.feasible(outcomes)
            if possible and len({w in claim for w in possible})==1:return time
        return None


def weights(config,assets):
    assets=set(assets)
    if config is None:return [{a:Fraction(1)for a in assets}]if len(assets)<=1 else None
    result=[{cash_key('',r['asset'])[1]:decimal(r['weight'])for r in s['weights']}for s in config['scenarios']]
    return result if all(assets<=r.keys()for r in result)else None


def models(snapshot,scope,config,bridge,rows,*,require_asks=True):
    evidence={tuple(r['key']):r for r in rows};plans={(p['instrument'],p['orientation']):p for p in snapshot['plans']}
    quantity={(q['instrument'],q['orientation']):q for q in config['policy']['quantities']};rules={(r['instrument'],r['orientation']):r['rule_identity']for r in config['rules']['books']}
    masks=outcome_scope(snapshot,scope);result=[];rejected=[]
    for member in snapshot['scopes'][scope]['members']:
        if not member['capture_selected']or not member['books']:rejected.append({'market_id':member['market_id'],'reason':'NOT_CAPTURED'})
        for book in member['books']:
            key=book['instrument'],book['orientation'];source=source_key(key);leg=masks.leg(key);reason=None
            if leg is None:reason=masks.status(key)[0]
            elif key not in rules:reason='RULE_UNSUPPORTED'
            elif key not in quantity:reason='QUANTITY_RULE_UNKNOWN'
            elif source not in evidence:reason='NOT_CAPTURED'
            elif evidence[source if require_asks else key]['validity']!='usable':reason='UNUSABLE'
            elif not key[0].startswith('kalshi:')and evidence[key]['crossed']:reason='SELF_CROSSED_LEG'
            elif bridge.economics(key)is None:reason='ECONOMICS_UNKNOWN'
            if reason:rejected.append({'key':list(key),'reason':reason});continue
            plan=plans[key];qs=int(plan['quantity_scale']);ps=int(plan['price_scale']);kalshi=key[0].startswith('kalshi:')
            inc=decimal(quantity[key]['increment'])*10**qs;cap=decimal(quantity[key]['cap'])*10**qs
            if inc.denominator!=1 or cap.denominator!=1:rejected.append({'key':list(key),'reason':'UNSUPPORTED_SCALE'});continue
            levels=tuple((int(p),int(q))for p,q in evidence[source]['bid'if kalshi else'ask'])
            if not levels and require_asks:rejected.append({'key':list(key),'reason':'ONE_SIDED'});continue
            maximum=int(min(cap,sum(q for _,q in levels))if require_asks else cap)//int(inc)*int(inc)
            if not maximum:rejected.append({'key':list(key),'reason':'DEPTH_LIMITED'});continue
            economics=bridge.economics(key)
            result.append({'key':key,'keys':leg.keys,'shape':leg.shape_id,'market_id':leg.market_id,'venue':plan['venue'],
                'price_scale':ps,'quantity_scale':qs,'increment':int(inc),'cap':maximum,'configured_cap':int(cap),
                'source':(*source,'bid'if kalshi else'ask'),'source_levels':levels,'bid_levels':tuple((int(p),int(q))for p,q in evidence[key]['bid']),
                'kalshi':kalshi,'asset':asset_row(economics.quote),'outcome_asset':asset_row(economics.outcome),'rule_identity':rules[key]})
    return result,rejected,masks


def price_order(bridge,model,q,time,sequence,scope,identity,capacity=None,direction='BUY'):
    source=model['source']if direction=='BUY'else(*model['key'],'bid');levels=model['source_levels']if direction=='BUY'else model['bid_levels']
    levels=levels if capacity is None else capacity.eligible(source,levels);complement=model['kalshi']and direction=='BUY';unit=10**model['price_scale']
    fill=walk(tuple((unit-p if complement else p,n)for p,n in levels),(q,))[0]
    if fill.depth_limited:return None
    leg={'market_id':model['market_id'],'key':model['key'],'fill':fill,'price_scale':model['price_scale'],'quantity_scale':model['quantity_scale'],'side':direction}
    try:assessed,unknown=bridge.assess_orders(experiment=identity,scope=scope,basket={'native_book':list(model['key']),'quantity_atoms':str(q)},direction=direction,size=q,time=time,sequence=sequence,legs=(leg,),account='bounded_payout_optimizer_v1')
    except FeeEconomicsUnavailable:return None
    if unknown or assessed[0]is None:return None
    economics,results=assessed[0];cash=received=Fraction(0);ids=[];charges=[];assumptions=set();evidence=set()
    for r in results:
        if r.unknowns or r.net_deltas is None:return None
        ids.append(r.identity);assumptions.update(r.assumptions);evidence.add(r.evidence.value)
        for d in r.net_deltas:
            amount=Fraction(d.atoms,10**d.scale)
            if d.asset==economics.quote:cash+=amount
            elif d.asset==economics.outcome:received+=amount
            else:return None
        for c in r.charges:
            row={'asset':asset_row(c.amount.asset),'component':c.component.value}
            if direction=='BUY':row['amount_e36']=str(c.amount.amount.atoms*10**(36-c.amount.amount.scale))
            else:row.update(atoms=str(c.amount.amount.atoms),scale=c.amount.amount.scale,evidence=c.evidence.value)
            charges.append(row)
    if direction=='BUY':assumptions.add('PER_LEVEL_DECLARED_PARTITION_ESTIMATE')
    if direction=='SELL'and -received>Fraction(q,10**model['quantity_scale']):return None
    return {'key':model['key'],'quantity':q,'gross_quantity':Fraction(q,10**model['quantity_scale']),
        'fee_priced_ns':time,'fee_priced_sequence':sequence,'fee_priced_scope':scope,
        'gross_cost':Fraction(fill.cost,10**(model['price_scale']+model['quantity_scale'])),'cash':cash,'retained':received,
        'asset':model['asset'],'venue':model['venue'],'outcome_asset':model['outcome_asset'],'source':source,
        'taken':tuple((unit-p if complement else p,n)for p,n in fill.taken),'ask_taken':fill.taken,'assessment_ids':ids,'charges':charges,
        'assumptions':sorted(assumptions),'evidence':sorted(evidence),'market_id':model['market_id'],'shape':model['shape'],'keys':sorted(model['keys']),
        'rule_identity':model['rule_identity'],'price_scale':model['price_scale'],'quantity_scale':model['quantity_scale'],'quantity_increment_atoms':model['increment']}


def verify_portfolio(raw,lookup,outcomes,config,bridge,time,sequence,scope,identity,capacity=None,pricing_cache=None):
    obj(raw,'orders margin cost floor gross_floor gross_margin holding_bound native_vectors classification');orders=[]
    for evidence in raw['orders']:
        key=tuple(evidence['key']);require(key in lookup,'independent acquired mask/rule admission');model=lookup[key];q=evidence['quantity']
        require(type(q)is int and model['increment']<=q<=model['cap']and q%model['increment']==0,'independent quantity domain')
        priced_time=evidence['fee_priced_ns'];priced_sequence=evidence['fee_priced_sequence'];priced_scope=evidence['fee_priced_scope']
        require(type(priced_time)is int and priced_time==time and type(priced_sequence)is int and priced_sequence==sequence and priced_scope==scope,'independent transaction fee/order clock')
        if pricing_cache is None:pricing_cache={}
        levels=capacity.eligible(model['source'],model['source_levels'])if capacity else model['source_levels']
        cache_key=key,q,levels
        if cache_key not in pricing_cache:pricing_cache[cache_key]=price_order(bridge,model,q,time,sequence,scope,identity,capacity)
        order=pricing_cache[cache_key]
        require(order is not None and wire(order)==evidence,'independent native order/fee proof');orders.append(order)
        require(order['cash']<=-order['gross_cost']and 0<=order['retained']<=order['gross_quantity'],'independent nonnegative BUY fee assumptions')
    keys=[o['key']for o in orders];require(keys==sorted(set(keys))and len(keys)<=config['policy']['max_legs'],'independent distinct native claims')
    used={}
    for o in orders:
        for p,q in o['taken']:used[o['source'],p]=used.get((o['source'],p),0)+q
    for source in {o['source']for o in orders}:
        model=next(m for m in lookup.values()if m['source']==source);levels=model['source_levels']if capacity is None else capacity.eligible(source,model['source_levels'])
        require(all(q<=dict(levels).get(p,0)for(s,p),q in used.items()if s==source),'independent shared source capacity')
    if not orders:
        expected={**raw,**{k:'0'for k in ('margin','cost','floor','gross_floor','gross_margin','holding_bound')},'native_vectors':[],'classification':'zero'}
        require(raw==expected,'independent zero payoff');return {**raw,**{k:Fraction(0)for k in ('margin','cost','floor','gross_floor','gross_margin','holding_bound')}}
    valuations=weights(config['valuation'],[cash_key('',o['asset'])[1]for o in orders]);require(valuations is not None,'independent valuation availability')
    native=[]
    for venue,asset in sorted({cash_key(o['venue'],o['asset'])for o in orders}):
        matching=[o for o in orders if cash_key(o['venue'],o['asset'])==(venue,asset)]
        native.append({'venue':venue,'asset':matching[0]['asset'],'outcomes':[[w,sum((o['cash']+o['retained']*int(w in o['keys'])for o in matching),Fraction(0))]for w in outcomes]})
    costs=[];floors=[];margins=[];gross_floors=[];gross_margins=[]
    for v in valuations:
        cost=sum((-o['cash']*v[cash_key('',o['asset'])[1]]for o in orders),Fraction(0));gcost=sum((o['gross_cost']*v[cash_key('',o['asset'])[1]]for o in orders),Fraction(0))
        payout=[sum((o['retained']*v[cash_key('',o['asset'])[1]]for o in orders if w in o['keys']),Fraction(0))for w in outcomes]
        gross=[sum((o['gross_quantity']*v[cash_key('',o['asset'])[1]]for o in orders if w in o['keys']),Fraction(0))for w in outcomes]
        costs.append(cost);floors.append(min(payout));margins.append(min(payout)-cost);gross_floors.append(min(gross));gross_margins.append(min(gross)-gcost)
    holding=max(costs)*decimal(config['policy']['holding_cost']['rate_per_ns'])*int(config['policy']['holding_cost']['maximum_duration_ns']or 0)
    masks=[set(o['keys'])&set(outcomes)for o in orders]
    kind='known_payout'if len(masks)==1 and masks[0]==set(outcomes)else'partition'if all(sum(w in m for m in masks)==1 for w in outcomes)else'implication'if len(masks)==2 and masks[0]|masks[1]==set(outcomes)else'general_overlap'
    expected={'orders':orders,'margin':min(margins)-holding,'cost':max(costs),'floor':min(floors),'gross_floor':min(gross_floors),'gross_margin':min(gross_margins),'holding_bound':holding,'native_vectors':native,'classification':kind}
    require(wire(expected)==raw,'independent portfolio margin/payoff/valuation certificate');return expected


def negative_bound(books,outcomes,config):
    valuations=weights(config['valuation'],[cash_key('',b['asset'])[1]for b in books])
    if valuations is None:return None
    bounds=[]
    for v in valuations:
        contributions=[]
        for b in books:
            p=b['source_levels'][0][0];ask=Fraction(10**b['price_scale']-p if b['kalshi']else p,10**b['price_scale'])
            expected=Fraction(sum(w in b['keys']for w in outcomes),len(outcomes))
            contributions.append(max(Fraction(0),expected-ask)*Fraction(b['cap'],10**b['quantity_scale'])*v[cash_key('',b['asset'])[1]])
        bounds.append(sum(sorted(contributions,reverse=True)[:config['policy']['max_legs']],Fraction(0)))
    return min(bounds)


def outstanding_cost(positions,config):
    residual=[l for p in positions for l in p['lots']if not l['settled']]
    if not residual:return Fraction(0)
    valuation=weights(config['valuation'],[cash_key('',l['asset'])[1]for l in residual])
    return max(sum((-l['cash']*v[cash_key('',l['asset'])[1]]for l in residual),Fraction(0))for v in valuation)if valuation else sum(l['cost_value']for l in residual)


def position_metrics(p,time,config):
    valuation=weights(config['valuation'],[cash_key('',l['asset'])[1]for l in p['lots']]);rate=decimal(config['policy']['holding_cost']['rate_per_ns']);native={};scenarios=[]
    for lot in p['lots']:
        key=cash_key(lot['venue'],lot['asset']);row=native.setdefault(key,{'venue':lot['venue'],'asset':lot['asset'],'closed_lot_cash_pnl':Fraction(0),'analytical_holding_charge':Fraction(0)})
        if lot['settled']:row['closed_lot_cash_pnl']+=lot['cash']+lot['credit']
        row['analytical_holding_charge']-=lot['cash']*rate*(min(time,lot['settled_at'])-p['opened_ns'])
    for index,v in enumerate(valuation or ()):
        pnl=sum((r['closed_lot_cash_pnl']*v[k[1]]for k,r in native.items()),Fraction(0));charge=sum((r['analytical_holding_charge']*v[k[1]]for k,r in native.items()),Fraction(0))
        scenarios.append({'valuation':index,'closed_lot_cash_pnl':pnl,'holding_charge':charge,'adjusted_pnl':pnl-charge})
    closed=p['state']=='CLOSED';maximum=config['policy']['holding_cost']['maximum_duration_ns'];expired=bool(rate and maximum is not None and any(min(time,l['settled_at'])-p['opened_ns']>int(maximum)for l in p['lots']))
    pnl=min((s['closed_lot_cash_pnl']for s in scenarios),default=None)
    return {'cash_pnl':pnl if closed else None,'closed_lot_pnl':pnl,'native_pnl':[native[k]for k in sorted(native)],'valuation_pnl':scenarios,
        'holding_charge':max((s['holding_charge']for s in scenarios),default=None),'adjusted_closed_pnl':min((s['adjusted_pnl']for s in scenarios),default=None)if closed else None,
        'guarantee_valid':p['guarantee_valid']and not expired,'guarantee_exclusions':['HOLDING_DURATION_ASSUMPTION_EXCEEDED']if expired else [],'capital_committed':outstanding_cost([p],config)}


def remaining_payouts(p,knowledge):
    outcomes=p['lots'][0]['all_outcomes'];possible=knowledge.feasible(outcomes)if knowledge else tuple(outcomes)
    if not possible:return {'status':'GAME_CONTRADICTION','vectors':[]}
    vectors=[]
    for venue,asset in sorted({cash_key(l['venue'],l['asset'])for l in p['lots']}):
        lots=[l for l in p['lots']if cash_key(l['venue'],l['asset'])==(venue,asset)]
        vectors.append({'venue':venue,'asset':lots[0]['asset'],'payouts':[[w,sum((l['remaining_quantity']for l in lots if w in l['keys']),Fraction(0))]for w in possible]})
    return {'status':'SUPPORTED_NORMAL_PAYOUTS','vectors':vectors}


def liquidation_marks(config,bridge,account,models,time,sequence,scope,identity):
    lookup={m['key']:m for m in models};marks=[]
    for p in account.positions:
        if p['state']not in ('OPEN','CENSORED'):continue
        legs=[]
        for lot in p['lots']:
            if lot['settled']:continue
            model=lookup.get(tuple(lot['key']));status='UNPRICED_BOOK';order=None
            if model is not None:
                q=lot['remaining_quantity']*10**model['quantity_scale'];status='UNREPRESENTABLE_HOLDINGS'
                if q.denominator==1 and int(q)%model['increment']==0:
                    order=price_order(bridge,model,int(q),time,sequence,scope,identity,account.capacity,'SELL');status='PRICED'if order else'NO_FULL_DEPTH_OR_UNKNOWN_SELL_FEES'
            leg={'key':lot['key'],'status':status,'net_sale':order['cash']if order else None}
            if order:leg['order']=order
            legs.append(leg)
        valuations=weights(config['valuation'],[cash_key('',l['asset'])[1]for l in p['lots']]);value=None
        if all(l['status']=='PRICED'for l in legs)and valuations is not None:
            value=min(sum((l['net_sale']*v[cash_key('',l['order']['asset'])[1]]for l in legs),Fraction(0))for v in valuations)
        marks.append({'position':p['id'],'kind':'UNPOSTED_CURRENT_BID_LIQUIDATION_MARK','legs':legs,'net_sale_value':value})
    return marks
