"""Independent streaming reader: reprice proofs and reconstruct native accounts.

It never imports the callback runtime or Account coordinator. Tape authenticity
remains bound to the supervisor/pins; carried detached books are writer-attested.
"""
from fractions import Fraction
import hashlib
from pathlib import Path
from collections import OrderedDict

from replay.economic_sdk.game import Timeline, binding, experiment_policy
from replay.economic_sdk.bounds import MAX_STATE, json_cost
from replay.economic_sdk.outcomes import outcome_scope
from replay.game_state import load
from replay.preparation import digest, encoded, load_snapshot, sha
from replay.strategy_sdk import plain
from replay.streams.protocol import decode, obj, require, uint, freeze
from replay.strategies._shared.fee_bridge import FeeBridge
from .contract import NAME, MAX_LINE, MAX_BYTES, MAX_ROWS, Inputs, validate, decimal, wire, cash_key
from .audit import Capacity, Knowledge, models, price_order, verify_portfolio, negative_bound, weights, remaining_payouts, liquidation_marks, position_metrics, outstanding_cost
from .stream import expand,unpack


def read_json(path):
    path=Path(path); require(path.is_file() and not path.is_symlink(),'regular optimizer output')
    with path.open('rb') as stream: return decode(stream.read(MAX_LINE+1),MAX_LINE)


def rational(value):
    require(type(value) is str and len(value)<=160,'exact rational string')
    try: result=Fraction(value)
    except (ValueError,ZeroDivisionError): raise ValueError('invalid exact rational') from None
    require(wire(result) == value,'canonical exact rational')
    return result


class ReaderAccount:
    """Separate ledger reconstruction; no writer-state objects are accepted."""
    def __init__(self,config):
        self.config=config; self.cash={cash_key(r['venue'],r['asset']):decimal(r['amount']) for r in config['account']['initial_cash']}
        self.assets={cash_key(r['venue'],r['asset']):r['asset'] for r in config['account']['initial_cash']}
        self.capacity=Capacity(); self.positions=[]; self.entries=0; self.spent=Fraction(0); self.closed_at=None
        self.signal=False; self.armed=True; self.nonpositive_since=None; self.pending=None;self.knowledge=None

    @property
    def open_positions(self): return [p for p in self.positions if p['state']=='OPEN']

    def record(self,time):
        positions=[]
        for p in self.positions:
            positions.append({'id':p['id'],'opened_ns':p['opened_ns'],'state':p['state'],'entry':p['entry'],
                'lots':p['lots'],**position_metrics(p,time,self.config),
                'remaining_payout_vectors':remaining_payouts(p,self.knowledge)})
        return wire({'cash':[{'venue':v,'asset':self.assets[v,a],'amount':n} for (v,a),n in sorted(self.cash.items())],
                     'capacity':self.capacity.record(),'entries':self.entries,'spent':self.spent,'positions':positions})

    def permits(self,orders):
        if not orders: return True
        scenarios=weights(self.config['valuation'],[cash_key('',o['asset'])[1] for o in orders])
        if scenarios is None: return False
        cost=max(sum((-o['cash']*s[cash_key('',o['asset'])[1]] for o in orders),Fraction(0)) for s in scenarios)
        cfg=self.config['account']; outstanding=outstanding_cost(self.open_positions,self.config)
        if cost>decimal(cfg['transaction_budget']) or self.spent+cost>decimal(cfg['event_budget']) or cost+outstanding>decimal(cfg['max_outstanding_cost']): return False
        if len(self.open_positions)>=cfg['max_open_positions']:return False
        for key in {cash_key(o['venue'],o['asset']) for o in orders}:
            if sum((-o['cash'] for o in orders if cash_key(o['venue'],o['asset'])==key),Fraction(0))>self.cash.get(key,0): return False
        return True

    def settlements(self,time,knowledge):
        policy=self.config['policy']['settlement'];credits=[];positions=self.open_positions
        pin={(r['instrument'],r['orientation']):(Fraction(r['payout']),int(r['availability_ns']))for r in policy['payouts']}if policy and policy['mode']=='PINNED_SETTLEMENT'else {}
        for position in positions:
            unresolved=[lot for lot in position['lots']if not lot['settled']]
            if knowledge.contradiction:
                position['guarantee_valid']=False;continue
            for lot in unresolved:
                entitlement=pin.get(tuple(lot['key']))
                if policy and policy['mode']=='NORMAL_RESOLUTION_SCENARIO':
                    possible=knowledge.feasible(lot['all_outcomes'])
                    if not possible:position['guarantee_valid']=False;continue
                    paid=sum(w in lot['keys']for w in possible)
                    if paid not in (0,len(possible)):continue
                    first=knowledge.determined(lot['keys'],lot['all_outcomes'])
                    if first is None:continue
                    lot['determined_ns']=first
                    entitlement=Fraction(int(paid==len(possible))),first+int(policy['delay_ns'])
                if entitlement is None:continue
                payout,available=entitlement;lot['due_ns']=max(position['opened_ns'],available)
                if lot['due_ns']<=time:credits.append((position,lot,payout))
        result=[]
        for position,lot,payout in credits:
            amount=lot['retained']*payout;account=cash_key(lot['venue'],lot['asset'])
            self.cash[account]=self.cash.get(account,Fraction(0))+amount;self.assets[account]=lot['asset']
            lot.update(settled=True,settled_at=time,credit=amount,remaining_quantity=Fraction(0));position['pnl']+=amount+lot['cash']
            result.append({'kind':'SETTLEMENT','position':position['id'],'key':lot['key'],'credit':amount,'payout':payout,'due_ns':lot['due_ns'],'mode':policy['mode']})
        for position in positions:
            if all(lot['settled']for lot in position['lots']):position['state']='CLOSED';self.closed_at=time
        return result

    def opening(self,solution,time,identity,all_outcomes):
        require(self.permits(solution['orders']),'reader cash/budget/position bounds')
        p={'id':identity,'opened_ns':time,'state':'OPEN','entry':solution,'lots':[],'pnl':Fraction(0),'guarantee_valid':True}
        for order in solution['orders']:
            key=cash_key(order['venue'],order['asset']); self.cash[key]+=order['cash']
            require(self.cash[key]>=0,'negative native cash')
            self.capacity.consume(order['source'],order['taken'])
            scenarios=weights(self.config['valuation'],[cash_key('',order['asset'])[1]])
            cost=max(-order['cash']*s[cash_key('',order['asset'])[1]] for s in scenarios)
            p['lots'].append({**order,'cost_value':cost,'all_outcomes':list(all_outcomes),'determined_ns':None,
                'due_ns':None,'remaining_quantity':order['retained'],'settled':False,'settled_at':2**64-1,'credit':Fraction(0)})
        self.positions.append(p);self.entries+=1;self.spent+=solution['cost'];self.pending=None;self.armed=False
        return {'kind':'OPEN','position':identity,'portfolio':solution,
                'scenario':'SIMULTANEOUS_DISPLAYED_SCENARIO','trading_status_unknown':True}


def selected(results):
    rows=[r for r in results if r.get('qualifying')]
    return min(rows,key=lambda r:(-r['found']['best']['margin'],r['found']['best']['cost'],len(r['found']['best']['orders']),r['shape'])) if rows else None


def verify_search(rows,config,snapshot,bridge,books,knowledge,time,sequence,scope,identity,account=None,frozen=None,conditioned=False,remaining=None,proofs=None):
    current,rejected,masks=models(snapshot,scope,config,bridge,books)
    require(type(rows)is list and 1<=len(rows)<=len(masks.spaces)+1,'independent problem count')
    shapes=[r['shape']for r in rows];require(shapes==(sorted(masks.spaces)or[None]),'independent problem identity/coverage')
    if remaining is None:remaining=[config['policy']['max_search_nodes']]
    if proofs is None:proofs={}
    result=[]
    for row in rows:
        require(row['rejected']==rejected,'independent rejected input admission')
        expected=None;selected_models=[];outcomes=()
        if row['shape']is None:expected='OUTCOMES_UNAVAILABLE'
        else:
            space=masks.spaces[row['shape']]
            selected_models=[m for m in current if m['shape']==row['shape']]
            outcomes=knowledge.feasible(space.keys)if conditioned else tuple(space.keys)
            if space.coverage!='EXHAUSTIVE':expected='INCOMPLETE_SPACE'
            elif not outcomes:expected='GAME_CONTRADICTION'
            elif len(selected_models)>config['policy']['max_books']or len(outcomes)>config['policy']['max_outcomes']:expected='DOMAIN_EXCEEDED'
            elif frozen is not None:
                selected_models=[{**m,'cap':min(m['cap'],frozen[m['key']])}for m in selected_models if m['key']in frozen]
                if {m['key']for m in selected_models}!=set(frozen):expected='PENDING_ADMISSION_CHANGED'
            if expected is None:
                if not selected_models:expected='INPUT_UNAVAILABLE'
                elif remaining[0]==0:expected='SEARCH_BUDGET_EXHAUSTED'
                elif decimal(config['policy']['holding_cost']['rate_per_ns'])and config['policy']['holding_cost']['maximum_duration_ns']is None:expected='HOLDING_COST_UNKNOWN'
        if 'found'not in row:
            obj(row,'shape status rejected');require(expected is not None and row['status']==expected,'independent unavailable problem cause')
            result.append(row);continue
        require(expected is None,'independent search admission cause')
        obj(row,'shape status economic_status outcomes fee_diagnostics pricing_failures blocked_portfolios valuation_unknown rejected found qualifying depth_caps upper_bound negative_certificate')
        require(row['shape']in masks.spaces,'independent supported outcome shape')
        space=masks.spaces[row['shape']];require(space.coverage=='EXHAUSTIVE','independent exhaustive scope')
        outcomes=knowledge.feasible(space.keys)if conditioned else tuple(space.keys)
        require(outcomes and list(outcomes)==row['outcomes'],'independent released outcome constraints')
        lookup={m['key']:m for m in selected_models}
        require(row['depth_caps']==[{'key':list(m['key']),'quantity_atoms':str(m['cap']),'configured_atoms':str(m['configured_cap'])}for m in selected_models],'independent displayed grid caps')
        valuations=weights(config['valuation'],[cash_key('',m['asset'])[1]for m in selected_models])
        require(row['valuation_unknown']is(valuations is None),'independent valuation availability')
        gross_upper=None if valuations is None else sum(sorted((Fraction(m['cap'],10**m['quantity_scale'])*valuations[0][cash_key('',m['asset'])[1]]for m in selected_models),reverse=True)[:config['policy']['max_legs']],Fraction(0))
        require(row['upper_bound']==wire(gross_upper),'independent gross payout upper bound')
        found=obj(row['found'],'best alternatives capital_frontier frontier_status visited complete unknown_books')
        require(found['frontier_status']=='BOUNDED_FEASIBLE_SAMPLES','independent sampled capital frontier kind')
        require(type(found['visited'])is int and 0<=found['visited']<=config['policy']['max_search_nodes']and type(found['complete'])is bool,'independent search count bound')
        require(found['visited']<=remaining[0],'independent shared search allowance');remaining[0]-=found['visited']
        bound=negative_bound(selected_models,outcomes,config)
        # The count includes whole-suffix bounds as well as portfolio valuations;
        # enumeration/branch coverage is writer-attested, never an optimality proof.
        require(found['visited']<=config['policy']['max_search_nodes'],'independent finite domain evaluation budget')
        require(row['status']==('SEARCH_COMPLETE'if found['complete']else'SEARCH_LIMITED'),'independent search status')
        native={**found,'best':verify_portfolio(found['best'],lookup,outcomes,config,bridge,time,sequence,scope,identity,account.capacity if account else None,proofs)}
        for name in ('alternatives','capital_frontier'):
            require(type(found[name])is list and len(found[name])<=config['policy']['alternatives'],'independent retained proof bound')
            native[name]=[verify_portfolio(p,lookup,outcomes,config,bridge,time,sequence,scope,identity,account.capacity if account else None,proofs)for p in found[name]]
        best=native['best']
        require(all(best['margin']>=p['margin']for p in native['alternatives']+native['capital_frontier']),'independent retained winner objective')
        if account:require(all(account.permits(p['orders'])for p in [best]+native['alternatives']+native['capital_frontier']),'independent entry capital constraints')
        qualifying=best['margin']>0 and best['margin']>=decimal(config['policy']['minimum_net_margin'])and 10000*best['margin']>=int(config['policy']['minimum_return_bps'])*best['cost']
        require(row['qualifying']is qualifying,'independent entry margin gates')
        require(row['negative_certificate']==(wire({'kind':'UNIFORM_OUTCOME_GROSS_DUAL_BOUND','margin_upper_bound':bound})if bound is not None else None),'independent negative dual certificate')
        if row['economic_status']=='COMPLETE_NONPOSITIVE':
            require(found['complete']and best['margin']<=0 and bound is not None and bound<=0 and not found['unknown_books']and not row['valuation_unknown'],'independent certified economic negative')
            availability=proofs.setdefault('availability',OrderedDict())
            for model in selected_models:
                levels=account.capacity.eligible(model['source'],model['source_levels'])if account else model['source_levels']
                key=bridge.engine_identity,scope,model['key'],model['market_id'],model['increment'],digest(wire(levels))
                if bridge._engine.resolver.reference_time is None or key not in availability:
                    availability[key]=price_order(bridge,model,model['increment'],time,sequence,scope,identity,account.capacity if account else None)is not None
                    while len(availability)>4096:availability.popitem(last=False)
                require(availability[key],'independent certified negative fee availability')
        if qualifying:require(row['economic_status']=='POSITIVE','independent positive economic status')
        elif best['margin']>0:require(row['economic_status']=='FEASIBLE_BELOW_ENTRY_THRESHOLD','independent below-threshold status')
        else:require(row['economic_status']in ('COMPLETE_NONPOSITIVE','INPUT_UNKNOWN','UNCERTIFIED_NONPOSITIVE','LIMITED_NO_POSITIVE'),'independent nonpositive uncertainty')
        result.append({**row,'found':native})
    return result,current


def verify_scenario(row,account,config,snapshot,bridge,books,knowledge,time,sequence,scope,identity,remaining,proofs):
    obj(row,'scenario comparison_label before detection actions after liquidation_marks pending signal armed nonpositive_since entry_search')
    name=row['scenario']; conditioned=name=='conditioned'
    require(row['before']==account.record(time),'reader opening account conservation')
    actions=account.settlements(time,knowledge)
    detected,current_models=verify_search(row['detection'],config,snapshot,bridge,books,knowledge,time,sequence,scope,identity,conditioned=conditioned,remaining=remaining,proofs=proofs)
    positive=selected(detected)
    nonpositive=bool(detected) and all(r.get('economic_status')=='COMPLETE_NONPOSITIVE' and r['found']['best']['margin']<=0 for r in detected)
    if nonpositive:
        if account.nonpositive_since is None:account.nonpositive_since=time
        if not account.open_positions and account.nonpositive_since+int(config['policy']['rearm_nonpositive_ns'])<=time:account.armed=True
    else:account.nonpositive_since=None
    attempted=False;used_entry_search=False
    if account.pending and account.pending['scope']!=scope:
        pending=account.pending;account.pending=None;attempted=True
        actions.append({'kind':'CANCELLED','attempt':pending['id'],'reason':'SCOPE_CHANGED'})
    if account.pending and account.pending['due_ns']<=time:
        pending=account.pending;account.pending=None;attempted=True
        require(row['entry_search']is not None,'missing required delayed entry proof');used_entry_search=True
        found,current=verify_search(row['entry_search'],config,snapshot,bridge,books,knowledge,time,sequence,scope,identity,account=account,
                    frozen={tuple(k):q for k,q in pending['caps']},conditioned=conditioned,remaining=remaining,proofs=proofs)
        best=selected([r for r in found if r['shape']==pending['shape']]); modelmap={b['key']:b for b in current}
        if best and all(modelmap.get(tuple(k),{}).get('rule_identity')==rule for k,rule in pending['rules']):
            actions.append(account.opening(best['found']['best'],time,pending['id'],outcome_scope(snapshot,scope).spaces[pending['shape']].keys))
            actions.extend(account.settlements(time,knowledge))
        else:actions.append({'kind':'CANCELLED','attempt':pending['id'],'reason':'DELAY_REVALIDATION_FAILED','search':found})
    if positive and not account.signal and account.armed and not attempted:
        attempt=digest([identity,name,scope,str(time),sequence,wire(positive['found']['best']['orders'])]);account.armed=False
        if account.entries>=config['policy']['max_entries_per_event']:
            actions.append({'kind':'SKIPPED','attempt':attempt,'reason':'EVENT_ENTRY_LIMIT'})
        elif account.open_positions or account.closed_at==time:
            actions.append({'kind':'SKIPPED','attempt':attempt,'reason':'OPEN_OR_SAME_TIME_CLOSED_POSITION'})
        elif int(config['policy']['decision_delay_ns']):
            best=positive['found']['best'];due=time+int(config['policy']['decision_delay_ns'])
            account.pending={'id':attempt,'due_ns':due,'shape':positive['shape'],'scope':scope,'caps':[(o['key'],o['quantity']) for o in best['orders']],
                'rules':[(o['key'],o['rule_identity']) for o in best['orders']]}
            actions.append({'kind':'PENDING','attempt':attempt,'due_ns':due,'caps':account.pending['caps']})
        else:
            require(row['entry_search']is not None,'missing required entry proof');used_entry_search=True
            found,_=verify_search(row['entry_search'],config,snapshot,bridge,books,knowledge,time,sequence,scope,identity,account=account,conditioned=conditioned,remaining=remaining,proofs=proofs)
            best=selected(found)
            if best:
                actions.append(account.opening(best['found']['best'],time,attempt,outcome_scope(snapshot,scope).spaces[best['shape']].keys))
                actions.extend(account.settlements(time,knowledge))
            else:actions.append({'kind':'SKIPPED','attempt':attempt,'reason':'CASH_BUDGET_OR_CAPACITY','search':found})
    account.signal=positive is not None
    require(used_entry_search or row['entry_search']is None,'unrequested entry proof')
    require(row['actions']==wire(actions),'independent position lifecycle/actions')
    require(row['liquidation_marks']==wire(liquidation_marks(config,bridge,account,models(snapshot,scope,config,bridge,books,require_asks=False)[0],time,sequence,scope,identity)),'independent unposted liquidation mark')
    require(row['after']==account.record(time),'reader closing account/depth/holdings conservation')
    require(row['pending']==wire(account.pending) and row['signal']==account.signal and row['armed']==account.armed and row['nonpositive_since']==account.nonpositive_since,'independent signal/rearm state')
    return detected


def check_books(rows,plans,allowed,time):
    require(type(rows)is list and len(rows)<=256,'detached book bound')
    keys=[]
    for row in rows:
        obj(row,'key validity reason crossed last_change_ns bid ask');require(type(row['key'])is list and len(row['key'])==2,'book key')
        key=tuple(row['key']);keys.append(key);require(key in plans and key in allowed,'book planned identity')
        require(row['validity'] in ('usable','not_initialized','unusable'),'book validity')
        require(type(row['crossed'])is bool and uint(row['last_change_ns'])<=time,'book observation time')
        ps=int(plans[key]['price_scale'])
        for side in ('bid','ask'):
            require(type(row[side])is list and len(row[side])<=100000,'ladder level bound')
            previous=None
            for level in row[side]:
                require(type(level)is list and len(level)==2,'native level')
                p,q=uint(level[0]),uint(level[1]);require(p<=10**ps and q>0,'native level price/quantity')
                require(previous is None or (p<previous if side=='bid' else p>previous),'native best-first ladder');previous=p
        crossed=bool(row['bid'] and row['ask'] and int(row['bid'][0][0])>int(row['ask'][0][0]))
        require(row['crossed']==crossed,'book crossing proof')
    require(keys==sorted(set(keys)) and set(keys)==allowed,'detached book identities')


def validate_content(directory,snapshot,manifest,bridge=None):
    obj(manifest,'version strategy snapshot_sha256 experiment_sha256 config fee_engine_identity files run_identity source_revision labels'+
        (' summary_sha256' if 'summary_sha256'in manifest else '')+(' game_state' if 'game_state'in manifest else ''))
    require(type(manifest['version'])is int and manifest['version']==2 and manifest['strategy']==NAME,'optimizer output version')
    config=validate(manifest['config']);require(manifest['source_revision']==config['source_revision'],'source revision binding');require(config['snapshot_sha256']==manifest['snapshot_sha256'],'snapshot manifest pin')
    for field in ('snapshot_sha256','experiment_sha256','fee_engine_identity','run_identity'):sha(manifest[field])
    bridge=bridge or FeeBridge(config['fees'],plain(snapshot['plans']))
    require(not bridge._engine.policy.include_account_rebates and not bridge._engine.policy.include_rounding_refunds,'independent nonnegative fee assumptions for dual bound')
    require(bridge.engine_identity==manifest['fee_engine_identity'],'fee engine identity')
    identity=digest({'strategy':NAME,'snapshot_sha256':manifest['snapshot_sha256'],'source_revision':config['source_revision'],'policy':experiment_policy(config['policy']),
                     'fees':bridge.semantic_config,'valuation':config['valuation'],'account':config['account'],'rules':config['rules']})
    require(identity==manifest['experiment_sha256'],'optimizer experiment identity')
    policy=config['policy'];timeline=None
    if 'game'in policy:
        require(manifest.get('game_state')==binding(policy['game']),'game binding')
        timeline=Timeline(load(policy['game']['input']['path'],policy['game']['input']['sha256'],snapshot),policy['game'],int(snapshot['config']['start_ns']))
    else:require('game_state'not in manifest,'unexpected game binding')
    labels=['SIMULTANEOUS_DISPLAYED_SCENARIO','CUMULATIVE_DISPLAY_CAP','TRADABLE_IF_USABLE_SCENARIO','normal_resolution_only',
            'ZERO_FINANCING_COST_SCENARIO' if policy['holding_cost']['rate_per_ns']=='0' else 'ANALYTICAL_HOLDING_COST_SCENARIO']
    if policy['settlement'] and policy['settlement']['mode']=='NORMAL_RESOLUTION_SCENARIO' and policy['settlement']['delay_ns']=='0':labels.append('IMMEDIATE_NORMAL_PAYOUT_AVAILABILITY_SCENARIO')
    require(manifest['labels']==labels,'scenario labels')
    knowledge=Knowledge({s:timeline.view.competitors[s]['participant'] for s in ('home','away')} if timeline else {})
    accounts={'static':ReaderAccount(config)}
    if timeline and timeline.view.phase!='unavailable':accounts['conditioned']=ReaderAccount(config)
    for account in accounts.values():account.knowledge=knowledge
    plans={(p['instrument'],p['orientation']):p for p in snapshot['plans']}
    from replay.strategies.cross_venue_arbitrage.contract import source_key
    allowed={(r['instrument'],r['orientation'])for r in policy['quantities']}
    allowed|={source_key(k)for k in allowed};allowed&=set(plans)
    obj(manifest['files'],'decisions.ndjson episodes.ndjson');file=obj(manifest['files']['decisions.ndjson'],'sha256 byte_length records');sha(file['sha256'])
    path=Path(directory)/'decisions.ndjson';require(path.is_file()and not path.is_symlink(),'regular decision file')
    hasher=hashlib.sha256();size=count=0;previous=None;sequence=-1;terminal=None;latest=None;stats={};episodes={};transport_state=None
    availability=OrderedDict()
    episode_path=Path(directory)/'episodes.ndjson'
    require(episode_path.is_file()and not episode_path.is_symlink(),'regular episode file')
    episode_hasher=hashlib.sha256();episode_size=episode_count=0;episode_transport_state=None
    episode_file=obj(manifest['files']['episodes.ndjson'],'sha256 byte_length records');sha(episode_file['sha256'])
    def close_episode(name,end,chosen,censored,stream):
        nonlocal episode_size,episode_count,episode_transport_state
        prior=episodes.pop(name)
        if end==prior['start_ns']:return
        expected=wire({**prior,'end_ns':end,'duration_ns':end-prior['start_ns'],'censored':censored,
            'reason':'RUN_END'if censored else 'SIGNAL_CHANGED'if chosen else 'PREDICATE_FALSE'})
        raw=stream.readline(MAX_LINE+1)
        require(raw and len(raw)<=MAX_LINE and raw.endswith(b'\n'),'bounded episode line/LF')
        episode_count+=1;episode_size+=len(raw);episode_hasher.update(raw)
        require(episode_count<=MAX_ROWS and episode_size<=MAX_BYTES,'episode file budget')
        stored=decode(raw[:-1],MAX_LINE);require(encoded(stored)+b'\n'==raw,'canonical episode bytes')
        episode_transport_state=expand(stored,episode_transport_state,episode_count-1)
        require(unpack(episode_transport_state)==expected,'independent detection episode')
        stats[name]['episodes']+=1
    def update_episodes(row,stream):
        end=int(row['t_ns']);censored=row['type']=='terminal'
        for name in accounts:
            scenario=next((r for r in row.get('scenarios',[])if r['scenario']==name),None)
            chosen=selected_native(scenario['detection'])if scenario and any(r.get('qualifying')for r in scenario['detection'])else None
            semantic=None
            if chosen:
                best=chosen['found']['best']
                semantic=digest([row['scope'],chosen['shape'],chosen['outcomes'],
                    [[o['key'],o['quantity'],o['retained'],o['rule_identity']]for o in best['orders']]])
            if name in episodes and episodes[name]['semantics']!=semantic:close_episode(name,end,chosen,censored,stream)
            if chosen:
                if name not in episodes:
                    episodes[name]={'id':digest([identity,name,semantic,str(end)]),'scenario':name,'semantics':semantic,
                        'start_ns':end,'scope':row['scope'],'shape':chosen['shape'],'outcomes':chosen['outcomes'],
                        'opening_portfolio':best,'maximum_margin':rational(best['margin'])}
                else:episodes[name]['maximum_margin']=max(episodes[name]['maximum_margin'],rational(best['margin']))
    def add_duration(end):
        if latest is None:return
        duration=end-int(latest['t_ns'])
        require(duration>=0,'decision duration')
        for row in latest['scenarios']:
            name=row['scenario'];stat=stats[name];d=row['detection']
            measurable=any(r.get('economic_status')in('POSITIVE','COMPLETE_NONPOSITIVE')for r in d)
            positive=any(r.get('qualifying')for r in d)
            status='positive' if positive else 'below_entry_threshold'if any(r.get('economic_status')=='FEASIBLE_BELOW_ENTRY_THRESHOLD'for r in d)else 'complete_nonpositive' if measurable and all(r.get('economic_status')=='COMPLETE_NONPOSITIVE'for r in d) else 'unknown'
            stat[status+'_ns']+=duration
    with path.open('rb')as stream,episode_path.open('rb')as episode_stream:
        while raw := stream.readline(MAX_LINE+1):
            size+=len(raw);count+=1;hasher.update(raw)
            require(size<=MAX_BYTES and count<=MAX_ROWS and len(raw)<=MAX_LINE and raw.endswith(b'\n'),'decision file budget/LF')
            stored=decode(raw[:-1],MAX_LINE);require(encoded(stored)+b'\n'==raw,'canonical decision bytes')
            transport_state=expand(stored,transport_state,count-1);row=unpack(transport_state)
            require(terminal is None,'rows after terminal')
            time=uint(row['t_ns']);require(previous is None or time>previous,'strict committed decision time')
            require(type(row['sequence'])is int and row['sequence']>=sequence,'decision sequence');sequence=row['sequence']
            start,end=int(snapshot['config']['start_ns']),int(snapshot['config']['end_ns'])
            require(start<=time<=end,'decision time window')
            if previous is not None:
                deadlines=[]
                if timeline and timeline.next_time is not None:deadlines.append(max(start,timeline.next_time))
                deadlines.extend(int(s['end_ns'])for s in snapshot['scopes'][:-1])
                for account in accounts.values():
                    if account.pending:deadlines.append(account.pending['due_ns'])
                    if account.nonpositive_since is not None and not account.armed:
                        deadlines.append(account.nonpositive_since+int(policy['rearm_nonpositive_ns']))
                    for position in account.open_positions:
                        deadlines.extend(lot['due_ns']for lot in position['lots']if not lot['settled']and lot['due_ns']is not None)
                        holding=policy['holding_cost']
                        if holding['rate_per_ns']!='0'and holding['maximum_duration_ns']is not None:
                            deadlines.append(position['opened_ns']+int(holding['maximum_duration_ns'])+1)
                require(not any(previous<deadline<time for deadline in deadlines),'missing required decision deadline')
            facts=[]
            if timeline:
                for f in timeline.advance(time):
                    value={'kind':f.kind,'release_ns':f.release_ns,'source_ns':f.source_ns,'index':f.index}
                    if f.kind=='segment_end':value['winner']=f.value[0]
                    elif f.kind=='match_end':value.update(winner=f.value['winner'],score=dict(f.value['score']))
                    facts.append(value)
            require(row['facts']==facts,'only every released game fact')
            knowledge.apply(facts)
            if 'conditioned'in accounts:
                for space in outcome_scope(snapshot,row['scope']).spaces.values():knowledge.feasible(space.keys)
            require(row['knowledge']==knowledge.record(),'released knowledge/prefix alignment')
            add_duration(time)
            if row['type']=='terminal':
                obj(row,'type t_ns sequence facts knowledge books scope accounts');require(type(row['scope'])is int and row['scope']==len(snapshot['scopes'])-1,'terminal scope');require(time==end,'terminal at run end')
                require([r['scenario']for r in row['accounts']]==list(accounts),'terminal accounts')
                for r in row['accounts']:
                    obj(r,'scenario actions after liquidation_marks');account=accounts[r['scenario']];actions=account.settlements(time,knowledge)
                    if account.pending:
                        actions.append({'kind':'CANCELLED','attempt':account.pending['id'],'reason':'RUN_END'});account.pending=None
                    for p in account.open_positions:p['state']='CENSORED'
                    require(r['actions']==wire(actions)and r['after']==account.record(time),'terminal settlement/censoring conservation')
                    check_books(row['books'],plans,allowed,time)
                    current,_,_=models(snapshot,row['scope'],config,bridge,row['books'],require_asks=False)
                    require(r['liquidation_marks']==wire(liquidation_marks(config,bridge,account,current,time,sequence,row['scope'],identity)),'terminal unposted liquidation mark')
                    for action in actions:stats[r['scenario']]['actions'][action['kind']]=stats[r['scenario']]['actions'].get(action['kind'],0)+1
                terminal=row;latest=None
            else:
                obj(row,'type t_ns sequence scope facts knowledge books scenarios search_budget');require(row['type']=='decision'and time<end,'decision type/end')
                search_budget=obj(row['search_budget'],'limit evaluations')
                evaluations=sum(r['found']['visited']for scenario in row['scenarios']for r in scenario['detection']+(scenario['entry_search']or [])if 'found'in r)
                require(search_budget=={'limit':policy['max_search_nodes'],'evaluations':evaluations}and evaluations<=policy['max_search_nodes'],'independent per-decision search budget')
                require(previous is not None or time==start,'decision start coverage')
                scope=next(i for i,s in enumerate(snapshot['scopes'])if int(s['start_ns'])<=time<int(s['end_ns']))
                require(type(row['scope'])is int and row['scope']==scope,'current scope at decision')
                check_books(row['books'],plans,allowed,time)
                require([r['scenario']for r in row['scenarios']]==list(accounts),'decision scenarios')
                remaining=[policy['max_search_nodes']];proofs={'availability':availability}
                for r in row['scenarios']:
                    name=r['scenario'];label='STATE_CONDITIONED'if name=='conditioned'else 'STATIC_GAME_UNAVAILABLE'if timeline is None or timeline.view.phase=='unavailable'else 'STATIC_REFERENCE'
                    require(r['comparison_label']==label,'game/static comparison label')
                    verify_scenario(r,accounts[name],config,snapshot,bridge,row['books'],knowledge,time,sequence,scope,identity,remaining,proofs)
                    stats.setdefault(name,dict(positive_ns=0,below_entry_threshold_ns=0,complete_nonpositive_ns=0,unknown_ns=0,episodes=0,actions={},skipped_entries_by_reason={}))
                    for action in r['actions']:
                        stats[name]['actions'][action['kind']]=stats[name]['actions'].get(action['kind'],0)+1
                        if action['kind']=='SKIPPED':
                            reasons=stats[name]['skipped_entries_by_reason'];reasons[action['reason']]=reasons.get(action['reason'],0)+1
                latest=row
            update_episodes(row,episode_stream)
            require(json_cost({'accounts':[a.record(time)for a in accounts.values()],'history':knowledge.history,'episodes':wire(episodes)})<=MAX_STATE,'reader retained state budget')
            previous=time
        require(not episode_stream.readline(MAX_LINE+1),'extra episode rows')
    require(terminal is not None,'missing optimizer terminal')
    require({'sha256':hasher.hexdigest(),'byte_length':size,'records':count}==file,'decision stored identity')
    require({'sha256':episode_hasher.hexdigest(),'byte_length':episode_size,'records':episode_count}==episode_file,'episode stored identity')
    rows=[]
    for name,account in accounts.items():
        stat=stats[name];require(sum(stat[k]for k in('positive_ns','below_entry_threshold_ns','complete_nonpositive_ns','unknown_ns'))==int(snapshot['config']['end_ns'])-int(snapshot['config']['start_ns']),'event-time denominator')
        rows.append({'scenario':name,**{k:str(v)if k.endswith('_ns')else v for k,v in stat.items()},'final_account':account.record(int(snapshot['config']['end_ns']))})
    summary={'version':2,'strategy':NAME,'event_id':config['rules']['event_id'],'snapshot_sha256':manifest['snapshot_sha256'],
        'experiment_sha256':identity,'terminal_sequence':sequence+1,'time_unit':'event_nanoseconds','history_complete':snapshot['history_complete'],
        'labels':labels,'rows':rows,'verification':'REPRICED_FEES_PAYOFFS_AND_NATIVE_LEDGER',
        'search_coverage_verification':'WRITER_ATTESTED_GRID_COVERAGE_OR_INDEPENDENT_NEGATIVE_DUAL_BOUND',
        'optimality_verification':'FEASIBLE_CERTIFICATE_ONLY_UNLESS_DUAL_BOUND_ATTAINED',
        'book_evidence':'WRITER_ATTESTED_DETACHED_BOOKS_BOUND_TO_SUPERVISOR_PINS'}
    require(len(encoded(summary))<=MAX_LINE,'optimizer summary bound')
    return summary


def selected_native(rows):
    positive=[r for r in rows if r.get('qualifying')]
    return min(positive,key=lambda r:(-rational(r['found']['best']['margin']),rational(r['found']['best']['cost']),len(r['found']['best']['orders']),r['shape']))


def read_provisional(directory,snapshot_directory,*,expected_sha256,bridge=None):
    root=Path(directory);snapshot=load_snapshot(snapshot_directory,expected_sha256=expected_sha256)
    manifest=read_json(root/'manifest.json');require(manifest['snapshot_sha256']==expected_sha256,'snapshot binding')
    receipt=obj(read_json(root/'content_receipt.json'),'version semantic_sha256 run_id attempt_id group identity terminal')
    require(type(receipt['version'])is int and receipt['version']==2,'receipt version');sha(receipt['identity']);sha(receipt['semantic_sha256'])
    require(receipt['semantic_sha256']==digest(manifest),'semantic receipt identity')
    for field in('run_id','attempt_id','group'):require(type(receipt[field])is str and 0<len(receipt[field])<=128,'receipt identifiers')
    require(type(receipt['terminal'])is int and receipt['terminal']>=2,'terminal sequence')
    require(manifest['run_identity']==receipt['identity'],'manifest run binding')
    summary=validate_content(root,snapshot,manifest,bridge)
    require(receipt['terminal']==summary['terminal_sequence'],'receipt terminal binding')
    require(read_json(root/'summary.json')==summary and manifest['summary_sha256']==digest(summary),'summary identity/schema')
    return {'receipt':receipt,'manifest':manifest,'summary':summary}


def read_completed(run_directory,group):
    from replay.supervisor import read,read_success,initial
    from replay.strategies import canonical_reference
    root=Path(run_directory);success=read_success(root);run=read(root/'run.json')
    require(group in success['outputs'],'known strategy group');spec=run['strategies'][group]
    require(canonical_reference(spec['factory'])=='replay.strategies.bounded_payout_optimizer:build','optimizer factory binding')
    inputs=Inputs(spec['config']);inputs.prepared.bind(freeze(initial(run)))
    result=read_provisional(root/success['outputs'][group],spec['config']['snapshot_directory'],expected_sha256=spec['config']['snapshot_sha256'],bridge=inputs.bridge)
    require(result['manifest']['experiment_sha256']==inputs.identity,'configured optimizer identity')
    require({k:result['receipt'][k]for k in('identity','attempt_id','group','run_id','terminal')}==
            {'identity':success['identity'],'attempt_id':success['attempt'],'group':group,'run_id':run['transport']['run_id'],'terminal':success['terminal']},'supervisor/content binding')
    return result


def check(*,run_directory,group,output_directory,context_directory):
    result=read_completed(run_directory,group)
    return {'passed':True,'details':{'semantic_sha256':result['receipt']['semantic_sha256'],'verification':result['summary']['verification']}}
