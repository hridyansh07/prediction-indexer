"""Observer-time staging for the bounded payout optimizer's isolated scenarios."""
from pathlib import Path

from replay.economic_intervals import CutClock
from replay.economic_sdk.game import Timeline, binding
from replay.economic_sdk.bounds import MAX_STATE, json_cost, view_cost
from replay.economic_sdk.types import BookRequirement
from replay.economic_sdk.views import ViewBuilder, touched_sides
from replay.game_state import load
from replay.preparation import digest
from replay.strategy_sdk import LineWriter, plain
from replay.streams.protocol import require
from replay.supervisor import write_json_durable
from .contract import Inputs, NAME, MAX_LINE, MAX_BYTES, MAX_ROWS, wire
from .core import Knowledge
from .scenario import Account, search, liquidation_marks, models as current_models


def fact_row(fact):
    row={'kind':fact.kind,'release_ns':fact.release_ns,'source_ns':fact.source_ns,'index':fact.index}
    if fact.kind == 'segment_end': row['winner']=fact.value[0]
    elif fact.kind == 'match_end': row.update(winner=fact.value['winner'],score=dict(fact.value['score']))
    return row


def choose(results):
    positive=[r for r in results if r.get('qualifying')]
    return min(positive,key=lambda r:(-r['found']['best']['margin'],r['found']['best']['cost'],
                    len(r['found']['best']['orders']),r['shape'])) if positive else None


class Runtime:
    """One committed decision per observer timestamp; quiet timers use detached books."""
    def __init__(self,context):
        self.inputs=Inputs(context['config']); self.config=self.inputs.config; self.snapshot=self.inputs.snapshot
        self.bridge=self.inputs.bridge; self.identity=self.inputs.identity; self.clock=CutClock(self.snapshot)
        self.root=Path(context['output_directory']); require(self.root.is_dir() and not any(self.root.iterdir()),'empty optimizer output')
        self.binding={k:context[k] for k in ('run_id','attempt_id','group','identity')}
        self.game=None
        if 'game' in self.config['policy']:
            policy=self.config['policy']['game']; self.game=Timeline(load(policy['input']['path'],policy['input']['sha256'],self.snapshot),policy,self.clock.start)
        participants=({s:self.game.view.competitors[s]['participant'] for s in ('home','away')} if self.game else {})
        self.knowledge=Knowledge(participants)
        self.accounts={'static':Account(self.config)}
        if self.game and self.game.view.phase != 'unavailable': self.accounts['conditioned']=Account(self.config)
        for account in self.accounts.values():account.knowledge=self.knowledge
        plans={(p['instrument'],p['orientation']):p for p in self.snapshot['plans']}
        needed={(r['instrument'],r['orientation']) for r in self.config['policy']['quantities']}
        from replay.strategies.cross_venue_arbitrage.contract import source_key
        needed |= {source_key(k) for k in needed}
        self.builders={k:ViewBuilder(BookRequirement(('bid','ask'),(1,),ladders=('bid','ask')),plans[k]) for k in sorted(needed) if k in plans}
        self.views={}; self.staged=None; self.sequence=-1; self.terminal=False; self.poisoned=False; self.last_inputs=None
        self.writer=LineWriter(self.root/'decisions.ndjson',max_bytes=MAX_BYTES,max_records=MAX_ROWS,max_line_bytes=MAX_LINE)
        self.episode_writer=LineWriter(self.root/'episodes.ndjson',max_bytes=MAX_BYTES,max_records=MAX_ROWS,max_line_bytes=MAX_LINE)
        self.episodes={}; self.facts=[]; self.decisions=0; self.processed_time=self.clock.start-1

    def __call__(self,cut):
        try:
            require(not self.terminal and not self.poisoned,'closed optimizer runtime')
            require(cut.sequence == self.sequence+1,'optimizer sequence'); self.sequence=cut.sequence
            if cut.kind == 'initial':
                self.inputs.prepared.bind(cut.body)
                for k,b in self.builders.items(): self.views[k]=b.build(cut.books[k],self.clock.start)
                self.staged=self.clock.start; self.guard_state(self.clock.start); return
            require(self.inputs.prepared.bound,'missing optimizer initial')
            if cut.kind == 'terminal':
                end=self.clock.terminal(); self.flush(); self.advance(end,terminal=True)
                self.terminal=True; return
            require(cut.kind == 'cut','optimizer cut kind')
            raw,time=self.clock.observe(cut)
            if self.staged is not None and time > self.staged: self.flush()
            self.advance(time)
            changed=touched_sides(cut)
            for k,sides in changed.items():
                if k in self.builders:
                    self.views[k]=self.builders[k].build(cut.books[k],time,self.views.get(k),sides)
            # Pre-start cuts only initialize the final staged start state.
            self.staged=time
            self.guard_state(time)
        except Exception:
            self.poisoned=True; raise

    def next_time(self,limit):
        values=[]
        if self.game and self.game.next_time is not None: values.append(max(self.clock.start,self.game.next_time))
        if self.clock.scope+1 < len(self.clock.scopes): values.append(int(self.clock.scopes[self.clock.scope]['end_ns']))
        for account in self.accounts.values():
            if account.pending is not None: values.append(account.pending['due_ns'])
            if account.nonpositive_since is not None and not account.armed:
                values.append(account.nonpositive_since+int(self.config['policy']['rearm_nonpositive_ns']))
            for p in account.open_positions:
                holding=self.config['policy']['holding_cost']
                if holding['rate_per_ns']!='0'and holding['maximum_duration_ns']is not None:
                    values.append(p['opened_ns']+int(holding['maximum_duration_ns'])+1)
                for lot in p['lots']:
                    if not lot['settled'] and lot['due_ns'] is not None: values.append(lot['due_ns'])
        values=[t for t in values if self.processed_time < t < limit and t < self.clock.end]
        return min(values) if values else None

    def release(self,time):
        for _old,_boundary,_new in self.clock.advance(time): pass
        facts=[fact_row(f) for f in self.game.advance(time)] if self.game else []
        self.knowledge.apply(facts); self.facts.extend(facts)

    def advance(self,time,terminal=False):
        while (due:=self.next_time(time)) is not None:
            self.release(due); self.decide(due,force=True)
        # A timer at a book time joins that same final stage; terminal has no
        # economic decision at the half-open end, but due credits are processed.
        if not terminal:
            self.release(time)
        else:
            self.release(time)
            rows=[]
            for name,account in self.accounts.items():
                actions=account.settle(time,self.knowledge)
                if account.pending:
                    actions.append({'kind':'CANCELLED','attempt':account.pending['id'],'reason':'RUN_END'})
                    account.pending=None
                for p in account.open_positions: p['state']='CENSORED'
                current,_,_=current_models(self.snapshot,self.clock.scope,self.config,self.bridge,self.books(),require_asks=False)
                marks=liquidation_marks(self.config,self.bridge,account,current,time,self.sequence,self.clock.scope,self.identity)
                rows.append({'scenario':name,'actions':actions,'after':wire(account.record(time)),'liquidation_marks':wire(marks)})
            self.update_episodes(time,None)
            self.guard_state(time)
            self.writer.append({'type':'terminal','t_ns':str(time),'sequence':self.sequence,
                                'facts':self.facts,'knowledge':self.knowledge.record(),'books':self.books(),'scope':self.clock.scope,'accounts':rows})
            self.facts=[]

    def books(self):
        rows=[]
        for k,v in sorted(self.views.items()):
            rows.append({'key':list(k),'validity':v.validity,'reason':v.reason,'crossed':v.crossed,
                         'last_change_ns':str(v.last_change),
                         'bid':[[str(p),str(q)] for p,q in v.ladders.get('bid',())],
                         'ask':[[str(p),str(q)] for p,q in v.ladders.get('ask',())]})
        return rows

    def flush(self):
        if self.staged is not None:
            self.release(self.staged); self.decide(self.staged); self.staged=None

    def decide(self,time,force=False):
        books=self.books(); scope=self.clock.scope
        fingerprint=digest({'books':[{k:v for k,v in b.items() if k != 'last_change_ns' and (k != 'bid' or b['key'][0].startswith('kalshi:'))} for b in books],
                            'scope':scope,'knowledge':self.knowledge.record()})
        timers=any(a.pending and a.pending['due_ns'] <= time or
                   a.nonpositive_since is not None and not a.armed and
                   a.nonpositive_since+int(self.config['policy']['rearm_nonpositive_ns']) <= time or
                   any(l['due_ns'] is not None and l['due_ns'] <= time for p in a.open_positions for l in p['lots'] if not l['settled'])
                   for a in self.accounts.values())
        if not force and not timers and not self.facts and fingerprint == self.last_inputs: return
        self.last_inputs=fingerprint; scenarios=[]; episode_candidates={}
        for name,account in self.accounts.items():
            before=wire(account.record(time)); actions=account.settle(time,self.knowledge)
            conditioned=name == 'conditioned'
            detected,models=search(self.config,self.bridge,self.snapshot,scope,books,self.knowledge,time,self.sequence,self.identity,conditioned=conditioned)
            signal=choose(detected)
            episode_candidates[name]=(scope,signal)
            known_nonpositive=bool(detected) and all(r.get('economic_status') == 'COMPLETE_NONPOSITIVE' and r['found']['best']['margin'] <= 0 for r in detected)
            if known_nonpositive:
                if account.nonpositive_since is None: account.nonpositive_since=time
                if not account.open_positions and account.nonpositive_since+int(self.config['policy']['rearm_nonpositive_ns']) <= time:
                    account.armed=True
            else: account.nonpositive_since=None
            by_key={b['key']:b for b in models}
            pending=account.pending
            attempted=False
            if pending is not None and pending['due_ns'] <= time:
                account.pending=None; attempted=True
                results,_=search(self.config,self.bridge,self.snapshot,scope,books,self.knowledge,time,self.sequence,self.identity,
                    account=account,frozen={tuple(k):q for k,q in pending['caps']},conditioned=conditioned)
                solution=choose([r for r in results if r['shape'] == pending['shape']])
                if solution and all(by_key.get(tuple(k),{}).get('rule_identity') == rule for k,rule in pending['rules']):
                    action=account.open(solution['found']['best'],time,scope,pending['id'],by_key,
                            next(s.keys for sid,s in outcome_scope(self.snapshot,scope).spaces.items() if sid == pending['shape']))
                    actions.append(action); actions.extend(account.settle(time,self.knowledge))
                else: actions.append({'kind':'CANCELLED','attempt':pending['id'],'reason':'DELAY_REVALIDATION_FAILED','search':results})
            can_attempt=signal is not None and not account.signal and account.armed and not attempted
            if can_attempt:
                attempt=digest([self.identity,name,scope,str(time),self.sequence,wire(signal['found']['best']['orders'])])
                account.armed=False
                if account.entries >= self.config['policy']['max_entries_per_event']:
                    actions.append({'kind':'SKIPPED','attempt':attempt,'reason':'EVENT_ENTRY_LIMIT'})
                elif account.open_positions or account.closed_at == time:
                    actions.append({'kind':'SKIPPED','attempt':attempt,'reason':'OPEN_OR_SAME_TIME_CLOSED_POSITION'})
                elif int(self.config['policy']['decision_delay_ns']):
                    best=signal['found']['best']; due=time+int(self.config['policy']['decision_delay_ns'])
                    account.pending={'id':attempt,'due_ns':due,'shape':signal['shape'],
                        'caps':[(o['key'],o['quantity']) for o in best['orders']],
                        'rules':[(o['key'],o['rule_identity']) for o in best['orders']]}
                    actions.append({'kind':'PENDING','attempt':attempt,'due_ns':due,'caps':account.pending['caps']})
                else:
                    results,_=search(self.config,self.bridge,self.snapshot,scope,books,self.knowledge,time,self.sequence,self.identity,account=account,conditioned=conditioned)
                    solution=choose(results)
                    if solution:
                        actions.append(account.open(solution['found']['best'],time,scope,attempt,by_key,
                            outcome_scope(self.snapshot,scope).spaces[solution['shape']].keys))
                        actions.extend(account.settle(time,self.knowledge))
                    else: actions.append({'kind':'SKIPPED','attempt':attempt,'reason':'CASH_BUDGET_OR_CAPACITY','search':results})
            account.signal=signal is not None
            scenarios.append({'scenario':name,'comparison_label':('STATE_CONDITIONED' if conditioned else 'STATIC_GAME_UNAVAILABLE' if not self.game or self.game.view.phase == 'unavailable' else 'STATIC_REFERENCE'),
                              'before':before,'detection':wire(detected),'actions':wire(actions),'after':wire(account.record(time)),
                              'liquidation_marks':wire(liquidation_marks(self.config,self.bridge,account,current_models(self.snapshot,scope,self.config,self.bridge,books,require_asks=False)[0],time,self.sequence,scope,self.identity)),
                              'pending':wire(account.pending),'signal':account.signal,'armed':account.armed,'nonpositive_since':account.nonpositive_since})
        self.update_episodes(time,episode_candidates)
        self.guard_state(time)
        self.writer.append({'type':'decision','t_ns':str(time),'sequence':self.sequence,'scope':scope,
                            'facts':self.facts,'knowledge':self.knowledge.record(),'books':books,'scenarios':scenarios})
        self.facts=[]; self.decisions+=1; self.processed_time=time

    def guard_state(self,time):
        detached=sum(view_cost(view,1)for view in self.views.values())
        retained=json_cost(wire({'accounts':[a.record(time)for a in self.accounts.values()],
                                 'history':self.knowledge.history,'episodes':self.episodes}))
        require(detached+retained<=MAX_STATE,'optimizer retained decision state budget')

    def update_episodes(self,time,candidates):
        for name in self.accounts:
            scope,chosen=candidates[name]if candidates is not None else (None,None)
            semantic=None
            if chosen:
                best=chosen['found']['best']
                semantic=digest(wire([scope,chosen['shape'],chosen['outcomes'],
                    [[o['key'],o['quantity'],o['retained'],o['rule_identity']]for o in best['orders']]]))
            prior=self.episodes.get(name)
            if prior and prior['semantics']!=semantic:
                if time>prior['start_ns']:
                    self.episode_writer.append(wire({**prior,'end_ns':time,'duration_ns':time-prior['start_ns'],
                        'censored':candidates is None,'reason':'RUN_END'if candidates is None else 'SIGNAL_CHANGED'if chosen else 'PREDICATE_FALSE'}))
                del self.episodes[name]
            if chosen:
                if name not in self.episodes:
                    self.episodes[name]={'id':digest([self.identity,name,semantic,str(time)]),'scenario':name,
                        'semantics':semantic,'start_ns':time,'scope':scope,'shape':chosen['shape'],
                        'outcomes':chosen['outcomes'],'opening_portfolio':best,'maximum_margin':best['margin']}
                else:self.episodes[name]['maximum_margin']=max(self.episodes[name]['maximum_margin'],best['margin'])

    def finish(self):
        require(self.terminal and not self.poisoned,'optimizer requires terminal')
        files={'decisions.ndjson':self.writer.finish(),'episodes.ndjson':self.episode_writer.finish()}
        manifest={'version':1,'strategy':NAME,'snapshot_sha256':self.inputs.prepared.sha256,
                  'experiment_sha256':self.identity,'config':self.config,'fee_engine_identity':self.bridge.engine_identity,
                  'files':files,'run_identity':self.binding['identity'],'source_revision':self.config['source_revision'],
                  'labels':['SIMULTANEOUS_DISPLAYED_SCENARIO','CUMULATIVE_DISPLAY_CAP','TRADABLE_IF_USABLE_SCENARIO',
                            'normal_resolution_only','ZERO_FINANCING_COST_SCENARIO' if self.config['policy']['holding_cost']['rate_per_ns'] == '0' else 'ANALYTICAL_HOLDING_COST_SCENARIO']}
        if self.config['policy']['settlement'] and self.config['policy']['settlement']['mode'] == 'NORMAL_RESOLUTION_SCENARIO' and self.config['policy']['settlement']['delay_ns'] == '0':
            manifest['labels'].append('IMMEDIATE_NORMAL_PAYOUT_AVAILABILITY_SCENARIO')
        if self.game: manifest['game_state']=binding(self.config['policy']['game'])
        from .output import validate_content
        summary=validate_content(self.root,self.snapshot,manifest,self.bridge)
        manifest['summary_sha256']=digest(summary)
        write_json_durable(self.root/'summary.json',summary); write_json_durable(self.root/'manifest.json',manifest)
        receipt={'version':1,'semantic_sha256':digest(manifest),**self.binding,'terminal':self.sequence+1}
        write_json_durable(self.root/'content_receipt.json',receipt)
        return receipt


def build(context): return Runtime(context)

# Deliberately imported only for current-scope proof lookup, never live networking.
from replay.economic_sdk.outcomes import outcome_scope
