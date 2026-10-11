"""Bounded optimizer: exact synthetic portfolios and stateful offline contracts."""
from fractions import Fraction
import unittest

from replay.strategies.bounded_payout_optimizer.core import Capacity, Knowledge, solve


class OptimizerCoreTests(unittest.TestCase):
    def test_overlap_and_unequal_quantity_search(self):
        # All pair-only margins are nonpositive; the three overlapping claims win.
        books = [dict(key=(str(i), 'outcome'), keys=frozenset(k), increment=1, cap=2)
                 for i, k in enumerate(('ab', 'bc', 'ac'))]
        def price(book, q):
            return dict(key=book['key'], quantity=q, retained=Fraction(q), cash=-Fraction(3*q, 5), source=book['key'], taken=((1,q),))
        found = solve(books, tuple('abc'), price, max_legs=3, max_nodes=1000)
        self.assertEqual(found['best']['margin'], Fraction(2,5))
        self.assertEqual([o['quantity'] for o in found['best']['orders']], [2,2,2])
        books = [dict(key=('a','outcome'), keys=frozenset('a'), increment=1, cap=104),
                 dict(key=('b','outcome'), keys=frozenset('b'), increment=1, cap=100)]
        def token_fee(book, q):
            a = book['key'][0] == 'a'
            return dict(key=book['key'], quantity=q, retained=Fraction(q*97,100) if a else Fraction(q),
                        cash=-Fraction(q*(40 if a else 58),100), source=book['key'], taken=((1,q),))
        found = solve(books, ('a','b'), token_fee, max_legs=2, max_nodes=20000)
        self.assertEqual(found['best']['margin'], Fraction(37,50))
        self.assertEqual([o['quantity'] for o in found['best']['orders']], [100,97])

    def test_limit_has_feasible_seed_and_never_claims_complete_negative(self):
        books = [dict(key=('a','outcome'), keys=frozenset('a'), increment=1, cap=100)]
        def price(book,q):
            return dict(key=book['key'], quantity=q, retained=Fraction(q), cash=-Fraction(q,2), source=book['key'], taken=((1,q),))
        found = solve(books, ('a',), price, max_legs=1, max_nodes=2)
        self.assertFalse(found['complete'])
        self.assertEqual(found['best']['margin'], 50)

    def test_capacity_keeps_price_and_total_debits_across_repricing(self):
        cap = Capacity()
        source = ('market','no','bid')
        cap.consume(source, ((60,40),))
        self.assertEqual(cap.eligible(source, ((60,100),)), ((60,60),))
        self.assertEqual(cap.eligible(source, ((55,100),)), ((55,60),))
        self.assertEqual(cap.eligible(source, ((55,30),)), ())
        self.assertEqual(cap.eligible(source, ((55,150),)), ((55,110),))

    def test_all_same_time_releases_preserve_unknown_prefix(self):
        k = Knowledge({'home':0,'away':1})
        k.apply([{'kind':'segment_end','index':1,'winner':None,'release_ns':20,'source_ns':20},
                 {'kind':'segment_end','index':2,'winner':'away','release_ns':20,'source_ns':20}])
        self.assertEqual(k.prefix, ())
        self.assertFalse(k.complete)
        self.assertEqual(k.feasible(('seq:HH','seq:HAH','seq:HAA','seq:AHH','seq:AHA','seq:AA')), ('seq:HAH','seq:HAA','seq:AA'))
        # Phase/end with no winner cannot shrink the domain.
        self.assertEqual(Knowledge({'home':0,'away':1}).feasible(('seq:HH','seq:AA')), ('seq:HH','seq:AA'))


class KnowledgeAlignmentTests(unittest.TestCase):
    def test_reversed_source_sides_are_explicit_participant_counts(self):
        k=Knowledge({'home':1,'away':0})
        k.apply([{'kind':'segment_end','index':1,'winner':'home','release_ns':1,'source_ns':1}])
        self.assertEqual(k.record()['known_score'], {'participant_0':0,'participant_1':1})
        self.assertEqual(k.feasible(('seq:HH','seq:AA')),('seq:AA',))

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from unittest.mock import patch

from replay.preparation import encoded
from replay.streams.protocol import ProtocolError
from replay.tests.test_same_venue_multi_market import Harness as MultiHarness
from replay.tests.test_game_state_sdk import game_file, policy as game_policy
from replay.strategies.bounded_payout_optimizer import build, read_provisional
from replay.strategies.bounded_payout_optimizer.contract import cash_key
from replay.strategies.bounded_payout_optimizer.output import validate_content


def optimizer_policy(**changes):
    result={'version':1,'max_legs':4,'max_books':128,'max_outcomes':1024,'max_search_nodes':100000,
            'alternatives':4,'quantities':[],'minimum_net_margin':'0.01','minimum_return_bps':'0',
            'decision_delay_ns':'0','max_entries_per_event':1,'rearm_nonpositive_ns':'1',
            'holding_cost':{'rate_per_ns':'0','maximum_duration_ns':None},'settlement':None}
    result.update(changes);return result


def decision_payload(rows):
    from replay.strategies.bounded_payout_optimizer.stream import pack
    return b''.join(encoded({'version':2,'index':i,'kind':'checkpoint','state':pack(r)})+b'\n'for i,r in enumerate(rows))


class OptimizerHarness(MultiHarness):
    def __init__(self,root,*,p=None,game=None,cash='100',cap='2',books=None,configure=None,**kw):
        def factory(context):
            cfg=deepcopy(dict(context['config']));cfg['source_revision']='d'*40;cfg['policy']=optimizer_policy(**(p or {}))
            plans=cfg['fees']['instrument_bindings']
            if books is not None:plans=[r for r in plans if (r['instrument'],r['orientation'])in books]
            cfg['policy']['quantities']=[{'instrument':r['instrument'],'orientation':r['orientation'],'increment':'1','cap':cap}for r in plans]
            accounts=[{'venue':v,'asset':a,'amount':cash}for v,a in cfg['fees']['assets'].items()]
            accounts.sort(key=lambda r:cash_key(r['venue'],r['asset']))
            assets=sorted({encoded(r['asset']):r['asset']for r in accounts}.values(),key=encoded)
            cfg['valuation']={'kind':'PARITY_SCENARIO','unit':'research_dollar','scenarios':[{'name':'parity','weights':[{'asset':a,'weight':'1'}for a in assets]}]}
            cfg['account']={'initial_cash':accounts,'transaction_budget':'1000','event_budget':'1000','max_outstanding_cost':'1000','max_open_positions':1}
            cfg['rules']={'event_id':'event:d1:'+'a'*64,'compatibility_id':'c'*64,'evidence_sha256':'d'*64,
                'normal_resolution_only':True,'assumptions':['SYNTHETIC_REVIEWED_NORMAL_RULES'],
                'books':[{'instrument':r['instrument'],'orientation':r['orientation'],'rule_identity':'e'*64}for r in plans]}
            if game is not None:
                raw=encoded(game)+b'\n';path=root/'game_state.json';path.write_bytes(raw)
                cfg['policy']['game']=game_policy(path,hashlib.sha256(raw).hexdigest(),segment_start='at_segment_end')
            if configure is not None:configure(cfg)
            self.optimizer_config=cfg
            return build({**context,'config':cfg})
        with patch('replay.tests.test_same_venue_multi_market.build',side_effect=factory):
            super().__init__(root,**kw)

    def finish(self):
        self.terminal();self.decoder.finish();self.strategy.finish()
        return read_provisional(self.output,self.root/'context',expected_sha256=self.sha,bridge=self.strategy.bridge)

    def close(self):
        self.strategy.writer.stream.close();self.strategy.episode_writer.stream.close()

    def records(self,name):
        rows=super().records(name)
        if name not in('decisions.ndjson','episodes.ndjson'):return rows
        from replay.strategies.bounded_payout_optimizer.stream import expand,unpack
        result=[];previous=None
        for index,stored in enumerate(rows):
            previous=expand(stored,previous,index);result.append(unpack(previous))
        return result


class OptimizerReplayTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)

    def harness(self,**kw):
        h=OptimizerHarness(self.root,**kw);self.addCleanup(h.close);h.window();return h

    def test_same_time_last_books_only_open_and_run_end_censors(self):
        h=self.harness()
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2))
        h.pm_ask(12,'987',(700,2));h.group(13)
        # Superseded positive at 12 is not an entry or positive-length episode.
        result=h.finish();s=result['summary']['rows'][0]
        self.assertEqual(s['final_account']['entries'],0)
        self.assertEqual(s['positive_ns'],'0')

    def test_general_overlap_open_and_censored_holdings(self):
        h=self.harness()
        h.kalshi_ask(12,'series','outcome',(60,2))
        h.kalshi_ask(12,'h20','complement',(60,2))
        h.kalshi_ask(12,'h21','complement',(60,2))
        result=h.finish();account=result['summary']['rows'][0]['final_account']
        self.assertEqual(account['entries'],1)
        self.assertEqual(account['positions'][0]['entry']['classification'],'general_overlap')
        self.assertEqual(account['positions'][0]['entry']['margin'],'2/5')
        self.assertEqual(account['positions'][0]['state'],'CENSORED')
        self.assertIsNone(account['positions'][0]['cash_pnl'])

    def test_delay_reprices_smaller_support_and_unknown_fee_is_visible(self):
        h=self.harness(p={'decision_delay_ns':'3'})
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));h.group(13)
        h.pm_ask(14,'987',(500,1));h.group(16)
        result=h.finish();position=result['summary']['rows'][0]['final_account']['positions'][0]
        self.assertEqual(position['opened_ns'],15)
        self.assertEqual([o['gross_quantity']for o in position['entry']['orders']],['1','1'])
        self.assertEqual(position['entry']['margin'],'1/10')

    def test_partial_and_complete_normal_settlement_separate_accounts(self):
        g=game_file();g['segments']=[{'index':1,'start_ns':12,'start_estimated':True,'end_ns':20,'settled_ns':None,'winner':'home','details':{}},
            {'index':2,'start_ns':21,'start_estimated':True,'end_ns':30,'settled_ns':None,'winner':'home','details':{}}]
        g['match']={'end_ns':30,'winner':'home','score':{'home':2,'away':0}}
        h=self.harness(game=g,p={'settlement':{'mode':'NORMAL_RESOLUTION_SCENARIO','delay_ns':'2'}})
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));h.group(35)
        result=h.finish();self.assertEqual([r['scenario']for r in result['summary']['rows']],['static','conditioned'])
        for row in result['summary']['rows']:
            position=row['final_account']['positions'][0]
            self.assertEqual(position['state'],'CLOSED');self.assertEqual(position['cash_pnl'],'1/5')
            self.assertEqual([o['settled_at']for o in position['lots']],[32,32])

    def test_only_released_winners_create_known_payout_and_no_lookahead(self):
        g=game_file();g['segments']=[{'index':1,'start_ns':12,'start_estimated':True,'end_ns':20,'settled_ns':None,'winner':'home','details':{}},
            {'index':2,'start_ns':21,'start_estimated':True,'end_ns':25,'settled_ns':None,'winner':'home','details':{}}]
        g['match']=None
        h=self.harness(game=g)
        h.pm_ask(12,'123',(900,2));h.group(30)
        result=h.finish();static,conditioned=result['summary']['rows']
        self.assertEqual(static['final_account']['entries'],0)
        pos=conditioned['final_account']['positions'][0]
        self.assertEqual(pos['opened_ns'],25);self.assertEqual(pos['entry']['classification'],'known_payout')
        self.assertEqual(pos['entry']['margin'],'1/5')

    def test_reader_rejects_rehashed_fee_payoff_cash_and_schema_tampering(self):
        h=self.harness();h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));result=h.finish()
        original=h.records('decisions.ndjson')
        for mutation in ('cash','margin','unknown'):
            rows=deepcopy(original);row=next(r for r in rows if r['type']=='decision'and r['scenarios'][0]['actions'])
            if mutation=='cash':row['scenarios'][0]['after']['cash'][0]['amount']='999'
            elif mutation=='margin':row['scenarios'][0]['detection'][0]['found']['best']['margin']='999'
            else:row['unexpected']=True
            payload=decision_payload(rows);(h.output/'decisions.ndjson').write_bytes(payload)
            manifest=deepcopy(result['manifest']);manifest.pop('summary_sha256')
            manifest['files']['decisions.ndjson']={'sha256':hashlib.sha256(payload).hexdigest(),'byte_length':len(payload),'records':len(rows)}
            with self.subTest(mutation=mutation),self.assertRaises(ProtocolError):
                validate_content(h.output,h.snapshot,manifest,h.strategy.bridge)

class OptimizerAdditionalReplayTests(unittest.TestCase):
    setUp = OptimizerReplayTests.setUp
    harness = OptimizerReplayTests.harness
    def test_later_known_outcome_purchase_cannot_delay_an_already_due_credit(self):
        g=game_file();g['segments']=[{'index':1,'start_ns':12,'start_estimated':True,'end_ns':20,'settled_ns':None,'winner':'home','details':{}},
            {'index':2,'start_ns':21,'start_estimated':True,'end_ns':25,'settled_ns':None,'winner':'home','details':{}}];g['match']=None
        h=self.harness(game=g,p={'settlement':{'mode':'NORMAL_RESOLUTION_SCENARIO','delay_ns':'2'}})
        h.group(29);h.pm_ask(30,'123',(900,2));h.group(31)
        result=h.finish();position=result['summary']['rows'][1]['final_account']['positions'][0]
        self.assertEqual(position['opened_ns'],30);self.assertEqual(position['lots'][0]['settled_at'],30)

    def test_frozen_delayed_support_unavailable_cancels_visibly(self):
        h=self.harness(p={'decision_delay_ns':'3'})
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));h.group(13)
        h.group(14,(h.initial['plans'][h.plan_index('polymarket:987')],),why={'kind':'connection_closed'})
        result=h.finish();account=result['summary']['rows'][0]
        self.assertEqual(account['final_account']['entries'],0)
        self.assertEqual(account['actions']['CANCELLED'],1)

class OptimizerCapitalTests(unittest.TestCase):
    setUp = OptimizerReplayTests.setUp
    harness = OptimizerReplayTests.harness
    def test_zero_cash_keeps_the_economic_signal_and_records_one_skip(self):
        h=self.harness(cash='0');h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));h.group(13)
        h.pm_ask(14,'123',(400,2));h.group(15)
        result=h.finish();row=result['summary']['rows'][0]
        self.assertGreater(int(row['positive_ns']),0)
        self.assertEqual(row['final_account']['entries'],0)
        self.assertEqual(row['actions']['SKIPPED'],1)

class OptimizerBenchTests(unittest.TestCase):
    def test_bench_real_completed_reader_and_entrypoint(self):
        from replay import supervisor
        from replay.bench.inside import execute, import_callable
        from replay.bench.specs import supervisor_config
        from replay.tests.test_bench import resolved, run_spec
        from replay.tests.test_supervisor import config as supervisor_fixture, metadata_pin
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);pin=metadata_pin();h=OptimizerHarness(root,pin=pin)
            self.addCleanup(h.close)
            spec=run_spec(root,root/'context')
            spec['groups']=[{'name':'coverage','factory':'replay.strategies.bounded_payout_optimizer:build',
                'revision':'synthetic-test','config':h.optimizer_config,
                'reader':'replay.strategies.bounded_payout_optimizer:read_completed',
                'checks':['replay.strategies.bounded_payout_optimizer:check'],
                'compare':{'rows':'rows','key':['scenario']}}]
            spec=resolved(spec);spec['runtime']['run_id']=h.context['run_id']
            base=supervisor_fixture();base['transport'].update(plans=h.initial['plans'],start_ns='10',end_ns='40',inputs=[pin]);spec['runtime']['base_run_config']=base
            def complete(config,run_directory,redis_url):
                identity=supervisor.identity(config);h.strategy.binding['identity']=identity;h.window();h.finish()
                attempt=h.context['attempt_id'];participant=run_directory/attempt/'coverage';participant.mkdir(parents=True)
                h.output.rename(participant/'output');terminal=h.seq
                supervisor.write_json_durable(run_directory/'run.json',config)
                supervisor.write_json_durable(participant/'complete.json',{'version':1,'identity':identity,'attempt':attempt,'group':'coverage','terminal':terminal})
                supervisor.write_json_durable(participant.parent/'result.json',{'version':1,'identity':identity,'attempt':attempt,'outcome':'success','fatal':False,'progress':terminal,'terminal':terminal,'participants':{'publisher':0,'coverage':0}})
                supervisor.write_json_durable(run_directory/'SUCCESS.json',{'version':1,'identity':identity,'attempt':attempt,'terminal':terminal,'outputs':{'coverage':attempt+'/coverage/output'}})
                return supervisor.read_success(run_directory)
            with patch.object(supervisor,'_strict_metadata_preflight'):
                code,result=execute(spec,root,redis_url='redis://unused',supervisor_run=complete)
            self.assertEqual((code,result['status']),(0,'SUCCESS'),result['error'])
            self.assertTrue(all(result['groups'][0]['checks'].values()))
            self.assertIs(import_callable(spec['groups'][0]['factory']),build)


class OptimizerLiquidityTests(unittest.TestCase):
    setUp = OptimizerReplayTests.setUp
    harness = OptimizerReplayTests.harness

    def test_current_sell_mark_uses_bids_when_acquisition_asks_are_absent(self):
        from replay.tests.economic_scenarios import ladder
        h=self.harness();h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));h.group(13)
        ladder(h,14,'polymarket:123','outcome',bids=((400,2*10**6),),asks=())
        ladder(h,14,'polymarket:987','outcome',bids=((500,2*10**6),),asks=())
        h.finish();terminal=h.records('decisions.ndjson')[-1]
        mark=terminal['accounts'][0]['liquidation_marks'][0]
        self.assertEqual(mark['net_sale_value'],'9/5')
        self.assertTrue(all(l['status']=='PRICED'for l in mark['legs']))

    def test_fee_rounding_cannot_prune_a_larger_positive_size(self):
        h=self.harness(fees='collateral',cap='5')
        h.kalshi_ask(12,'series','outcome',(48,5));h.kalshi_ask(12,'series','complement',(48,5))
        result=h.finish();pos=result['summary']['rows'][0]['final_account']['positions'][0]
        self.assertEqual(pos['entry']['margin'],'1/50')
        self.assertEqual([o['gross_quantity']for o in pos['entry']['orders']],['4','4'])

    def test_partial_resolution_retains_censored_native_inventory_and_basis(self):
        g=game_file();g['segments'][0]['winner']='away';g['match']=None
        h=self.harness(game=g,p={'settlement':{'mode':'NORMAL_RESOLUTION_SCENARIO','delay_ns':'2'}})
        h.kalshi_ask(12,'series','complement',(46,2));h.kalshi_ask(12,'h20','outcome',(20,2));h.kalshi_ask(12,'h21','outcome',(25,2))
        result=h.finish();position=result['summary']['rows'][0]['final_account']['positions'][0]
        self.assertEqual(position['state'],'CENSORED');self.assertIsNone(position['cash_pnl'])
        settled=[l for l in position['lots']if l['settled']]
        self.assertEqual(len(settled),1);self.assertEqual(settled[0]['remaining_quantity'],'0')
        self.assertEqual(position['capital_committed'],'71/50')

    def test_reentry_requires_known_nonpositive_duration_and_keeps_event_capacity(self):
        books={('polymarket:123','outcome'),('polymarket:987','outcome')}
        settlement={'mode':'PINNED_SETTLEMENT','payouts':[
            {'instrument':'polymarket:123','orientation':'outcome','payout':'1','availability_ns':'15','evidence_sha256':'a'*64},
            {'instrument':'polymarket:987','orientation':'outcome','payout':'0','availability_ns':'15','evidence_sha256':'a'*64}]}
        h=self.harness(books=books,p={'max_entries_per_event':2,'rearm_nonpositive_ns':'2','settlement':settlement})
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));h.group(13)
        h.pm_ask(16,'123',(600,2));h.pm_ask(16,'987',(600,2));h.group(19)
        h.pm_ask(20,'123',(400,2));h.pm_ask(20,'987',(500,2));h.group(21)
        result=h.finish();row=result['summary']['rows'][0]
        self.assertEqual(row['final_account']['entries'],1)
        self.assertEqual(row['actions']['SKIPPED'],1)
        skips=[a for r in h.records('decisions.ndjson')if r['type']=='decision'for a in r['scenarios'][0]['actions']if a['kind']=='SKIPPED']
        self.assertTrue(any(x.get('pricing_failures',{}).get('CAPACITY_SHORTFALL')for x in skips[0]['search']))


class OptimizerFeeAndDomainTests(unittest.TestCase):
    setUp = OptimizerReplayTests.setUp
    harness = OptimizerReplayTests.harness

    def test_native_limitless_token_charge_reduces_holdings_not_cash(self):
        from replay.fees.artifacts import build_catalog
        from replay.fees.domain import Asset,AssetKind,AssetAmount,Fixed,InstrumentEconomics,canonical
        from replay.fees.schedules import Catalog
        from replay.strategies._shared.fee_bridge import FeeBridge
        from replay.strategies.bounded_payout_optimizer.scenario import price_order
        quote=Asset(AssetKind.USDC,'synthetic','collateral');token=Asset(AssetKind.OUTCOME,'synthetic','yes')
        e=InstrumentEconomics(quote,token,AssetAmount(quote,Fixed(1,0)),6,6,2)
        catalog=Catalog.build(());directory=build_catalog(self.root/'empty-fees',catalog,{})
        plan={'instrument':'limitless:yes','orientation':'outcome','venue':'limitless','lane':'offline','price_scale':'2','quantity_scale':'6'}
        fees={'catalog_directory':str(directory),'catalog_identity':catalog.identity,'reference_ns':'20','limitless_buy_bps':300,'limitless_sell_bps':150,
              'kalshi_member_class':'NON_DIRECT','assets':{'limitless':{'kind':AssetKind.USDC.value,'ledger':'synthetic','token':'collateral'}},
              'instrument_bindings':[{'instrument':'limitless:yes','orientation':'outcome','economics':json.loads(canonical(e))}]}
        bridge=FeeBridge(fees,[plan])
        model={'key':('limitless:yes','outcome'),'market_id':'limitless:yes','shape':'x','keys':frozenset('a'),'source':('limitless:yes','outcome','ask'),
            'source_levels':((40,104*10**6),),'bid_levels':((39,104*10**6),),'kalshi':False,'price_scale':2,'quantity_scale':6,'increment':10**6,
            'asset':fees['assets']['limitless'],'outcome_asset':{'kind':AssetKind.OUTCOME.value,'ledger':'synthetic','token':'yes'},'venue':'limitless','rule_identity':'e'*64}
        order=price_order(bridge,model,104*10**6,12,1,0,'f'*64)
        self.assertEqual(order['cash'],Fraction(-208,5));self.assertEqual(order['retained'],Fraction(2522,25))
        self.assertTrue(order['charges']);self.assertEqual(order['charges'][0]['asset']['kind'],AssetKind.OUTCOME.value)

    def test_limited_positive_opens_but_limited_absence_is_unknown(self):
        h=self.harness(p={'max_search_nodes':2},cap='100')
        h.pm_ask(12,'123',(400,100));h.pm_ask(12,'987',(500,100))
        result=h.finish();row=result['summary']['rows'][0]
        self.assertEqual(row['final_account']['entries'],1)
        detections=[r for r in h.records('decisions.ndjson')if r['type']=='decision'][-1]['scenarios'][0]['detection']
        self.assertEqual(detections[0]['status'],'SEARCH_LIMITED')
        self.assertEqual(detections[0]['found']['best']['margin'],'10')

    def test_unknown_holding_bound_and_closed_config_are_rejected_visibly(self):
        h=self.harness(p={'holding_cost':{'rate_per_ns':'0.001','maximum_duration_ns':None}})
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2))
        result=h.finish();self.assertEqual(result['summary']['rows'][0]['final_account']['entries'],0)
        rows=h.records('decisions.ndjson')
        self.assertTrue(any(d['status']=='HOLDING_COST_UNKNOWN'for r in rows if r['type']=='decision'for d in r['scenarios'][0]['detection']))
        from replay.strategies.bounded_payout_optimizer.contract import validate
        for changes in ({'source_revision':'not-a-pin'},{'unknown':True}):
            bad=deepcopy(h.optimizer_config);bad.update(changes)
            with self.assertRaises(ProtocolError):validate(bad)

class OptimizerBoundedReaderTests(unittest.TestCase):
    setUp = OptimizerReplayTests.setUp
    harness = OptimizerReplayTests.harness

    def test_oversized_line_is_read_with_a_limit_before_rejection(self):
        h=self.harness();h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));result=h.finish()
        original=Path.open;limits=[];test=self
        class BoundedStream:
            def __init__(self,stream):self.stream=stream
            def __enter__(self):return self
            def __exit__(self,*args):self.stream.close()
            def __iter__(self):test.fail('unbounded line iteration')
            def readline(self,limit):limits.append(limit);return self.stream.readline(limit)
        def opening(path,*args,**kwargs):
            stream=original(path,*args,**kwargs)
            return BoundedStream(stream)if path.name=='decisions.ndjson'else stream
        with patch('replay.strategies.bounded_payout_optimizer.output.MAX_LINE',32),patch.object(Path,'open',opening):
            with self.assertRaises(ProtocolError):validate_content(h.output,h.strategy.snapshot,result['manifest'],h.strategy.bridge)
        self.assertEqual(limits,[33])

class OptimizerFinalContractTests(unittest.TestCase):
    setUp = OptimizerReplayTests.setUp
    harness = OptimizerReplayTests.harness

    def test_exact_search_matches_an_independent_small_domain_reference(self):
        from itertools import product
        import random
        rng=random.Random(47)
        for case in range(20):
            outcomes=tuple('abc')
            books=[{'key':(str(i),'outcome'),'keys':frozenset(w for w in outcomes if rng.randrange(2)),
                    'increment':1,'cap':2,'price':Fraction(rng.randrange(1,10),10)}for i in range(4)]
            def price(b,q):return {'key':b['key'],'quantity':q,'cash':-b['price']*q,'retained':Fraction(q),'source':b['key'],'taken':((1,q),)}
            def allowed(orders):return -sum((o['cash']for o in orders),Fraction(0))<=3
            found=solve(books,outcomes,price,max_legs=3,max_nodes=1000,feasible=allowed)
            reference=[]
            for vector in product(range(3),repeat=4):
                orders=[price(b,q)for b,q in zip(books,vector)if q]
                if len(orders)>3 or not allowed(orders):continue
                cost=-sum((o['cash']for o in orders),Fraction(0))
                floor=min(sum(q for b,q in zip(books,vector)if w in b['keys'])for w in outcomes)
                reference.append((-(floor-cost),cost,len(orders),tuple((o['key'],o['quantity'])for o in orders)))
            best=found['best'];key=(-best['margin'],best['cost'],len(best['orders']),tuple((o['key'],o['quantity'])for o in best['orders']))
            with self.subTest(case=case):self.assertTrue(found['complete']);self.assertEqual(key,min(reference))

    def test_episodes_are_positive_intervals_and_bid_only_changes_are_marks(self):
        from replay.tests.economic_scenarios import ladder
        h=self.harness();h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));h.group(13)
        ladder(h,14,'polymarket:123','outcome',bids=((390,3*10**6),),asks=((400,2*10**6),))
        h.group(15);result=h.finish()
        self.assertNotIn('14',[r['t_ns']for r in h.records('decisions.ndjson')if r['type']=='decision'])
        episodes=h.records('episodes.ndjson');self.assertEqual(len(episodes),1)
        self.assertEqual((episodes[0]['start_ns'],episodes[0]['end_ns'],episodes[0]['duration_ns']),(12,40,28))
        self.assertTrue(episodes[0]['censored']);self.assertEqual(result['summary']['rows'][0]['episodes'],1)
        bad=deepcopy(episodes);bad[0]['duration_ns']=29
        payload=decision_payload(bad);(h.output/'episodes.ndjson').write_bytes(payload)
        manifest=deepcopy(result['manifest']);manifest.pop('summary_sha256');manifest['files']['episodes.ndjson']={'sha256':hashlib.sha256(payload).hexdigest(),'byte_length':len(payload),'records':1}
        with self.assertRaisesRegex(ProtocolError,'independent detection episode'):validate_content(h.output,h.snapshot,manifest,h.strategy.bridge)

    def test_unknown_fee_keeps_exact_known_evidence_and_cannot_rearm(self):
        def configure(cfg):
            from replay.fees.artifacts import build_catalog
            from replay.fees.schedules import Catalog
            catalog=Catalog.build(());directory=build_catalog(self.root/'missing-fees',catalog,{})
            cfg['fees'].update(catalog_directory=str(directory),catalog_identity=catalog.identity)
        h=self.harness(configure=configure);h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2))
        result=h.finish();row=next(r for r in h.records('decisions.ndjson')if r['type']=='decision'and r['t_ns']=='12')['scenarios'][0]
        self.assertTrue(any(r.get('economic_status')=='INPUT_UNKNOWN'for r in row['detection']))
        diagnostics=[d for r in row['detection']for d in r.get('fee_diagnostics',[])]
        self.assertTrue(diagnostics);self.assertTrue(diagnostics[0]['unknowns']);self.assertEqual(diagnostics[0]['gross_cost'],'4/5')
        self.assertIsNone(row['nonpositive_since']);self.assertEqual(result['summary']['rows'][0]['final_account']['entries'],0)

    def test_quiet_holding_assumption_expiry_preserves_native_cash_and_inventory(self):
        h=self.harness(p={'holding_cost':{'rate_per_ns':'0.001','maximum_duration_ns':'3'}})
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));result=h.finish()
        row=next(r for r in h.records('decisions.ndjson')if r['t_ns']=='16')
        position=row['scenarios'][0]['after']['positions'][0]
        self.assertFalse(position['guarantee_valid']);self.assertEqual(position['holding_charge'],'9/1250')
        final=result['summary']['rows'][0]['final_account']['positions'][0]
        self.assertEqual([o['remaining_quantity']for o in final['lots']],['2','2']);self.assertIsNone(final['cash_pnl'])
        self.assertEqual(final['holding_charge'],'63/1250')

    def test_robust_native_valuation_keeps_correlated_cost_and_pnl(self):
        from replay.fees.domain import AssetKind
        from replay.strategies.bounded_payout_optimizer.scenario import portfolio_value,position_metrics,outstanding_cost
        assets=[{'kind':AssetKind.USDC.value,'ledger':'synthetic','token':t}for t in ('a','b')]
        valuations={'kind':'STRESS_SCENARIO','unit':'scenario','scenarios':[{'name':name,'weights':sorted([{'asset':a,'weight':weight}for a,weight in zip(assets,weights)],key=lambda r:encoded(r['asset']))}for name,weights in [('a_high',('2','1')),('b_high',('1','2'))]]}
        orders=[{'venue':'polymarket','asset':a,'cash':Fraction(-1),'gross_quantity':Fraction(3),'gross_cost':Fraction(1),'retained':Fraction(3)}for a in assets]
        books=[{'keys':frozenset('a')},{'keys':frozenset('b')}]
        value=portfolio_value(books,orders,('a','b'),valuations)
        self.assertEqual(value[:2],(0,3));self.assertEqual({v['asset']['token']:v['outcomes']for v in value[-1]},{'a':[['a',2],['b',-1]],'b':[['a',-1],['b',2]]})
        lots=[{**o,'credit':Fraction(3 if i==0 else 0),'settled':True,'settled_at':20,'cost_value':Fraction(2)}for i,o in enumerate(orders)]
        p={'state':'CLOSED','opened_ns':10,'lots':lots,'guarantee_valid':True}
        cfg={'valuation':valuations,'policy':{'holding_cost':{'rate_per_ns':'0.01','maximum_duration_ns':'20'}}}
        metrics=position_metrics(p,30,cfg);self.assertEqual(metrics['cash_pnl'],0);self.assertEqual(metrics['adjusted_closed_pnl'],Fraction(-3,10))
        for lot in lots:lot['settled']=False
        self.assertEqual(outstanding_cost([p],cfg),3)

    def test_optional_unavailable_game_runs_static_and_required_fails_before_output(self):
        from replay.economic_sdk.game import GameStateUnavailable
        g=game_file();g.update(state='unavailable',reason='no_fetch',source=None,scheduled_start_ns=None,segment_kind=None,segments=[],match=None)
        h=self.harness(game=g);h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));result=h.finish()
        self.assertEqual([r['scenario']for r in result['summary']['rows']],['static'])
        self.assertTrue(all(r['scenarios'][0]['comparison_label']=='STATIC_GAME_UNAVAILABLE'for r in h.records('decisions.ndjson')if r['type']=='decision'))
        required=self.root/'required';required.mkdir()
        with self.assertRaises(GameStateUnavailable):OptimizerHarness(required,game=g,configure=lambda cfg:cfg['policy']['game'].update(required=True))
        self.assertEqual(list((required/'output').iterdir()),[])

    def test_source_and_terminal_receipt_bindings_are_independently_checked(self):
        h=self.harness();result=h.finish()
        manifest=deepcopy(result['manifest']);manifest['source_revision']='e'*40
        with self.assertRaisesRegex(ProtocolError,'source revision binding'):validate_content(h.output,h.snapshot,manifest,h.strategy.bridge)
        receipt=deepcopy(result['receipt']);receipt['terminal']+=1;(h.output/'content_receipt.json').write_bytes(encoded(receipt)+b'\n')
        with self.assertRaisesRegex(ProtocolError,'receipt terminal binding'):read_provisional(h.output,h.root/'context',expected_sha256=h.sha,bridge=h.strategy.bridge)

class OptimizerAdmissionAndExamplesTests(unittest.TestCase):
    setUp = OptimizerReplayTests.setUp
    harness = OptimizerReplayTests.harness

    def test_below_threshold_positive_is_not_a_complete_economic_negative(self):
        books={('polymarket:123','outcome'),('polymarket:987','outcome')}
        h=self.harness(books=books,cap='1');h.pm_ask(12,'123',(494,1));h.pm_ask(12,'987',(500,1));result=h.finish()
        decision=next(r for r in h.records('decisions.ndjson')if r['type']=='decision'and r['t_ns']=='12')
        detection=decision['scenarios'][0]['detection'][0]
        self.assertEqual(detection['found']['best']['margin'],'3/500');self.assertFalse(detection['qualifying'])
        self.assertEqual(detection['economic_status'],'FEASIBLE_BELOW_ENTRY_THRESHOLD')
        self.assertEqual(result['summary']['rows'][0]['complete_nonpositive_ns'],'0')

    def test_remaining_frozen_subset_positive_still_cancels_missing_original_support(self):
        g=game_file();g['segments']=[{'index':i,'start_ns':None,'start_estimated':False,'end_ns':15,'settled_ns':None,'winner':'home','details':{}}for i in (1,2)];g['match']=None
        h=self.harness(game=g,p={'decision_delay_ns':'4'})
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));h.group(13)
        h.group(14,(h.initial['plans'][h.plan_index('polymarket:987')],),why={'kind':'connection_closed'})
        result=h.finish();row=next(r for r in h.records('decisions.ndjson')if r['t_ns']=='16')['scenarios'][1]
        self.assertTrue(any(d.get('qualifying')and d['found']['best']['classification']=='known_payout'for d in row['detection']))
        self.assertEqual(row['actions'][0]['kind'],'CANCELLED');self.assertEqual(row['after']['entries'],0)
        self.assertEqual(result['summary']['rows'][1]['final_account']['entries'],0)

    def test_reversed_source_sides_remain_aligned_through_output_and_reader(self):
        g=game_file();g['competitors']={'home':{'label':'Beta','participant':1},'away':{'label':'Alpha','participant':0}}
        g['segments']=[{'index':i,'start_ns':None,'start_estimated':False,'end_ns':20+i,'settled_ns':None,'winner':'home','details':{}}for i in (1,2)];g['match']=None
        h=self.harness(game=g);h.pm_ask(12,'987',(900,2));result=h.finish()
        position=result['summary']['rows'][1]['final_account']['positions'][0]
        self.assertEqual(position['entry']['orders'][0]['key'],['polymarket:987','outcome'])
        knowledge=h.records('decisions.ndjson')[-1]['knowledge'];self.assertEqual(knowledge['complete_prefix'],'AA')
        self.assertEqual(knowledge['known_score'],{'participant_0':0,'participant_1':2})

    def test_examples_and_all_entrypoints_use_the_current_closed_contract(self):
        from replay.strategies.bounded_payout_optimizer.contract import validate
        from replay.bench.specs import validate_run_spec
        from replay.strategies import load_entrypoint
        package=Path(__file__).parents[1]/'strategies/bounded_payout_optimizer'
        validate(json.loads((package/'config.example.json').read_text()))
        validate_run_spec(json.loads((package/'bench.example.json').read_text()),check_paths=False)
        self.assertIs(load_entrypoint('replay.strategies.bounded_payout_optimizer:build'),build)
        for name in ('read_provisional','read_completed','check'):self.assertTrue(callable(load_entrypoint('replay.strategies.bounded_payout_optimizer:'+name)))

class OptimizerDeadlineReaderTests(unittest.TestCase):
    setUp = OptimizerReplayTests.setUp
    harness = OptimizerReplayTests.harness

    def test_rehashed_omitted_credit_timer_cannot_post_at_a_later_decision(self):
        g=game_file();g['segments']=[{'index':1,'start_ns':12,'start_estimated':True,'end_ns':20,'settled_ns':None,'winner':'home','details':{}},
            {'index':2,'start_ns':21,'start_estimated':True,'end_ns':30,'settled_ns':None,'winner':'home','details':{}}]
        g['match']={'end_ns':30,'winner':'home','score':{'home':2,'away':0}}
        h=self.harness(game=g,p={'settlement':{'mode':'NORMAL_RESOLUTION_SCENARIO','delay_ns':'2'}})
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));result=h.finish()
        rows=h.records('decisions.ndjson');credit=next(r for r in rows if r['t_ns']=='32');rows.remove(credit)
        terminal=rows[-1]
        for row,prior in zip(terminal['accounts'],credit['scenarios']):
            row['actions']=prior['actions']
            for p in row['after']['positions']:
                for lot in p['lots']:lot['settled_at']=40
        payload=decision_payload(rows);(h.output/'decisions.ndjson').write_bytes(payload)
        manifest=deepcopy(result['manifest']);manifest.pop('summary_sha256');manifest['files']['decisions.ndjson']={'sha256':hashlib.sha256(payload).hexdigest(),'byte_length':len(payload),'records':len(rows)}
        with self.assertRaisesRegex(ProtocolError,'missing required decision deadline'):validate_content(h.output,h.snapshot,manifest,h.strategy.bridge)


class OptimizerReviewRegressions(unittest.TestCase):
    setUp = OptimizerReplayTests.setUp
    harness = OptimizerReplayTests.harness

    def test_reader_rejects_negative_with_declared_rejected_inputs(self):
        from replay.strategies.bounded_payout_optimizer import strategy
        original=strategy.search
        def falsely_certified(*args,**kwargs):
            results,models=original(*args,**kwargs)
            for result in results:
                if 'found'in result and result['negative_certificate']['margin_upper_bound']==0:
                    self.assertEqual(result['economic_status'],'INPUT_UNKNOWN')
                    result['economic_status']='COMPLETE_NONPOSITIVE'
            return results,models
        h=self.harness();h.pm_ask(12,'123',(900,2))
        # The faulty writer produces coherent durations, rearm state and hashes.
        with patch.object(strategy,'search',side_effect=falsely_certified),self.assertRaisesRegex(ProtocolError,'independent.*negative.*input'):
            h.finish()

    def test_honest_rejected_declared_inputs_stay_unknown(self):
        for reason in ('UNUSABLE','ONE_SIDED','DEPTH_LIMITED','ECONOMICS_UNKNOWN','SELF_CROSSED_LEG','UNSUPPORTED_SCALE'):
            with self.subTest(reason=reason),tempfile.TemporaryDirectory()as tmp:
                root=Path(tmp)
                def configure(cfg):
                    if reason=='ECONOMICS_UNKNOWN':
                        cfg['fees']['instrument_bindings']=[r for r in cfg['fees']['instrument_bindings']if r['instrument']!='polymarket:987']
                    elif reason=='UNSUPPORTED_SCALE':
                        next(q for q in cfg['policy']['quantities']if q['instrument']=='polymarket:987')['increment']='0.0000003'
                h=OptimizerHarness(root,books={('polymarket:123','outcome'),('polymarket:987','outcome')},configure=configure)
                try:
                    h.window();h.pm_ask(12,'123',(900,2))
                    if reason=='ONE_SIDED':h.pm_ask(12,'987')
                    elif reason=='DEPTH_LIMITED':h.pm_ask(12,'987',(900,Fraction(1,2)))
                    elif reason=='ECONOMICS_UNKNOWN':h.pm_ask(12,'987',(900,2))
                    elif reason=='UNSUPPORTED_SCALE':h.pm_ask(12,'987',(950,2))
                    elif reason=='SELF_CROSSED_LEG':
                        from replay.tests.economic_scenarios import ladder
                        ladder(h,12,'polymarket:987',bids=((960,2000000),),asks=((950,2000000),))
                    result=h.finish();stat=result['summary']['rows'][0]
                    self.assertEqual(stat['unknown_ns'],'30');self.assertEqual(stat['complete_nonpositive_ns'],'0')
                    rejected=[r for row in h.records('decisions.ndjson')if row['type']=='decision'for d in row['scenarios'][0]['detection']for r in d['rejected']]
                    self.assertTrue(any(r.get('key')==['polymarket:987','outcome']and r['reason']==reason for r in rejected),rejected)
                finally:h.close()

    def test_reader_rejects_crossed_and_unrepresentable_declared_negative(self):
        from replay.strategies.bounded_payout_optimizer import strategy
        from replay.tests.economic_scenarios import ladder
        original=strategy.search
        def falsely_certified(*args,**kwargs):
            results,models=original(*args,**kwargs)
            for result in results:
                if 'found'in result and result['negative_certificate']['margin_upper_bound']==0:
                    result['economic_status']='COMPLETE_NONPOSITIVE'
            return results,models
        for reason in ('SELF_CROSSED_LEG','UNSUPPORTED_SCALE'):
            with self.subTest(reason=reason),tempfile.TemporaryDirectory()as tmp:
                def configure(cfg):
                    if reason=='UNSUPPORTED_SCALE':
                        next(q for q in cfg['policy']['quantities']if q['instrument']=='polymarket:987')['increment']='0.0000003'
                h=OptimizerHarness(Path(tmp),books={('polymarket:123','outcome'),('polymarket:987','outcome')},configure=configure)
                try:
                    h.window();h.pm_ask(12,'123',(900,2))
                    if reason=='SELF_CROSSED_LEG':ladder(h,12,'polymarket:987',bids=((960,2000000),),asks=((950,2000000),))
                    else:h.pm_ask(12,'987',(950,2))
                    with patch.object(strategy,'search',side_effect=falsely_certified),self.assertRaisesRegex(ProtocolError,'independent.*negative.*input'):
                        h.finish()
                finally:h.close()

    def test_reader_rejects_rule_event_pin_even_if_writer_guard_is_faulty(self):
        from replay.strategies.bounded_payout_optimizer import contract
        original=contract.require
        def skip_writer_event_guard(condition,message):
            if message!='rule event alignment':return original(condition,message)
        with patch.object(contract,'require',side_effect=skip_writer_event_guard):
            h=self.harness(configure=lambda cfg:cfg['rules'].update(event_id='event:d1:'+'b'*64))
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2))
        with self.assertRaisesRegex(ProtocolError,'independent rule event alignment'):h.finish()

    def test_uncaptured_declared_market_stays_unknown_after_scope_change(self):
        h=self.harness(scopes='uncaptured',books={('kalshi:series','outcome'),('polymarket:123','outcome')})
        h.kalshi_ask(12,'series','outcome',(90,2));h.pm_ask(12,'123',(900,2));h.group(25)
        stat=h.finish()['summary']['rows'][0]
        self.assertEqual(stat['unknown_ns'],'19');self.assertEqual(stat['complete_nonpositive_ns'],'11')

    def test_uncaptured_undeclared_sibling_does_not_veto_negative(self):
        h=self.harness(scopes='uncaptured',books={('kalshi:series','outcome')})
        h.kalshi_ask(12,'series','outcome',(90,2));h.group(25)
        stat=h.finish()['summary']['rows'][0]
        self.assertEqual(stat['unknown_ns'],'2');self.assertEqual(stat['complete_nonpositive_ns'],'28')

    def test_reader_rejects_false_negative_from_market_only_uncaptured_row(self):
        from replay.strategies.bounded_payout_optimizer import strategy
        original=strategy.search
        def falsely_certified(*args,**kwargs):
            results,models=original(*args,**kwargs)
            for result in results:
                if 'found'in result and any(r.get('market_id')=='polymarket:series'and r['reason']=='NOT_CAPTURED'for r in result['rejected']):
                    result['economic_status']='COMPLETE_NONPOSITIVE'
            return results,models
        h=self.harness(scopes='uncaptured',books={('kalshi:series','outcome'),('polymarket:123','outcome')})
        with patch.object(strategy,'search',side_effect=falsely_certified),self.assertRaisesRegex(ProtocolError,'independent.*negative.*input'):
            h.kalshi_ask(12,'series','outcome',(90,2));h.pm_ask(12,'123',(900,2));h.group(25)
            h.finish()

    def test_reader_rejects_false_advertised_bound_and_frontier_kind(self):
        h=self.harness(books={('polymarket:123','outcome'),('polymarket:987','outcome')})
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));result=h.finish()
        original=h.records('decisions.ndjson')
        for mutation in ('upper_bound','frontier_status'):
            rows=deepcopy(original)
            problem=next(r['scenarios'][0]['detection'][0]for r in rows if r['type']=='decision'and r['scenarios'][0]['actions'])
            if mutation=='upper_bound':problem['upper_bound']='0'
            else:problem['found']['frontier_status']='EXHAUSTIVE_OPTIMAL_FRONTIER'
            payload=decision_payload(rows);(h.output/'decisions.ndjson').write_bytes(payload)
            manifest=deepcopy(result['manifest']);manifest.pop('summary_sha256')
            manifest['files']['decisions.ndjson']={'sha256':hashlib.sha256(payload).hexdigest(),'byte_length':len(payload),'records':len(rows)}
            with self.subTest(mutation=mutation),self.assertRaisesRegex(ProtocolError,'independent.*(upper bound|frontier)'):
                validate_content(h.output,h.snapshot,manifest,h.strategy.bridge)

    def test_reader_rejects_writer_valuation_bug_even_while_patch_is_active(self):
        from replay.strategies.bounded_payout_optimizer import scenario
        original=scenario.portfolio_value
        def broken(*args):
            margin,*rest=original(*args)
            return (margin+1,*rest)
        h=self.harness(books={('polymarket:123','outcome'),('polymarket:987','outcome')},cap='1')
        with patch.object(scenario,'portfolio_value',side_effect=broken):
            h.pm_ask(12,'123',(200,1));h.pm_ask(12,'987',(1000,1))
            with self.assertRaisesRegex(ProtocolError,'independent.*(margin|payoff|valuation)'):
                h.finish()

    def test_entire_decision_has_one_shared_search_budget(self):
        from replay.strategies.bounded_payout_optimizer import scenario
        original=scenario.solve;visits=[]
        def counting(*args,**kwargs):
            result=original(*args,**kwargs);visits.append(result['visited']);return result
        h=self.harness(game=game_file(),p={'max_search_nodes':8})
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2))
        with patch.object(scenario,'solve',side_effect=counting):
            h.strategy.flush()
        self.assertLessEqual(sum(visits),8,visits)

    def test_contradictory_results_are_flagged_in_knowledge(self):
        k=Knowledge({'home':0,'away':1})
        k.apply([{'kind':'segment_end','index':i,'winner':'home','release_ns':i,'source_ns':i}for i in (1,2,3)])
        self.assertEqual(k.feasible(('seq:HH','seq:HAH','seq:HAA','seq:AHH','seq:AHA','seq:AA')),())
        self.assertTrue(k.record()['contradiction'])

    def test_malformed_settlement_uses_closed_config_error(self):
        from replay.strategies.bounded_payout_optimizer.contract import validate
        h=self.harness()
        for bad in ({},[],{'mode':'NORMAL_RESOLUTION_SCENARIO'},'not a block'):
            cfg=deepcopy(h.optimizer_config);cfg['policy']['settlement']=bad
            with self.subTest(bad=bad),self.assertRaises(ProtocolError):validate(cfg)

    def test_skipped_entries_summary_counts_reasons(self):
        h=self.harness(cash='0');h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2))
        result=h.finish()
        self.assertEqual(result['summary']['rows'][0]['skipped_entries_by_reason'],{'CASH_BUDGET_OR_CAPACITY':1})

    def test_scope_change_cancels_pending_even_if_native_books_survive(self):
        h=self.harness(scopes=True,p={'decision_delay_ns':'10'})
        h.pm_ask(20,'123',(400,2));h.pm_ask(20,'987',(500,2));h.group(21)
        h.group(26)
        h.finish()
        actions=[a for r in h.records('decisions.ndjson')if r['type']=='decision'for a in r['scenarios'][0]['actions']]
        self.assertTrue(any(a['kind']=='CANCELLED'and a['reason']=='SCOPE_CHANGED'for a in actions),actions)
        self.assertFalse(any(a['kind']=='OPEN'for a in actions))

    def test_unchanged_book_gets_current_decision_fee_proofs(self):
        h=self.harness(books={('polymarket:123','outcome'),('polymarket:987','outcome')})
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));h.group(13)
        h.strategy.flush()
        h.strategy.decide(14,force=True)
        h.finish()
        row=next(r for r in h.records('decisions.ndjson')if r['t_ns']=='14')
        proofs=[o for r in row['scenarios'][0]['detection'] for o in r['found']['best']['orders']]
        self.assertTrue(proofs)
        self.assertEqual({o['fee_priced_ns']for o in proofs},{14})

    def test_cache_cannot_hide_changed_economics_at_materialization(self):
        from replay.strategies.bounded_payout_optimizer import scenario
        h=self.harness(books={('polymarket:123','outcome'),('polymarket:987','outcome')})
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2));h.group(13)
        h.strategy.flush()
        original=scenario.price_order
        def changed(*args,**kwargs):
            order=original(*args,**kwargs)
            if order is not None and args[3]==14:order={**order,'cash':order['cash']-Fraction(1,100)}
            return order
        with patch.object(scenario,'price_order',side_effect=changed),self.assertRaises(ProtocolError):
            h.strategy.decide(14,force=True)

    def test_reader_rejects_false_unavailable_search_that_hides_positive(self):
        from replay.strategies.bounded_payout_optimizer import strategy
        original=strategy.search
        def missing(*args,**kwargs):
            before=kwargs['budget'].used
            results,models=original(*args,**kwargs)
            kwargs['budget'].used=before
            return ([{'shape':r['shape'],'status':'INPUT_UNAVAILABLE','rejected':r['rejected']}if 'found'in r else r for r in results],models)
        h=self.harness(books={('polymarket:123','outcome'),('polymarket:987','outcome')})
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2))
        with patch.object(strategy,'search',side_effect=missing),self.assertRaises(ProtocolError):h.finish()

    def test_reader_rejects_writer_settlement_fault_with_consistent_writer_ledger(self):
        from replay.strategies.bounded_payout_optimizer.scenario import Account
        g=game_file();g['segments']=[{'index':1,'start_ns':12,'start_estimated':True,'end_ns':20,'settled_ns':None,'winner':'home','details':{}},
            {'index':2,'start_ns':21,'start_estimated':True,'end_ns':30,'settled_ns':None,'winner':'home','details':{}}]
        g['match']={'end_ns':30,'winner':'home','score':{'home':2,'away':0}}
        h=self.harness(game=g,p={'settlement':{'mode':'NORMAL_RESOLUTION_SCENARIO','delay_ns':'2'}})
        h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2))
        original=Account.settle
        def faulty(account,*args):
            actions=original(account,*args)
            for action in actions:
                if action['kind']!='SETTLEMENT':continue
                position=next(p for p in account.positions if p['id']==action['position'])
                lot=next(l for l in position['lots']if l['key']==action['key'])
                action['credit']+=1;lot['credit']+=1;position['pnl']+=1
                account.cash[cash_key(lot['venue'],lot['asset'])]+=1
            return actions
        with patch.object(Account,'settle',faulty),self.assertRaises(ProtocolError):h.finish()

    def test_repeated_fee_certificates_fit_real_event_output_budget(self):
        h=self.harness(books={('kalshi:series','outcome'),('kalshi:beta','outcome')},cap='100',fees='collateral')
        h.kalshi_ask(12,'series','outcome',(48,100));h.kalshi_ask(12,'beta','outcome',(48,100));h.group(13)
        h.kalshi_ask(14,'series','outcome',(47,100));h.group(15)
        h.kalshi_ask(16,'series','outcome',(48,100));h.group(17)
        h.kalshi_ask(18,'series','outcome',(47,100));h.group(19);h.finish()
        raw=[json.loads(line)for line in (h.output/'decisions.ndjson').read_bytes().splitlines()]
        delta=next(row for row in reversed(raw)if row['kind']=='delta')
        from replay.strategies.bounded_payout_optimizer.contract import MAX_BYTES
        self.assertLess(100000*(len(encoded(delta))+1),MAX_BYTES)

    def test_supervised_finish_does_not_run_the_long_independent_audit(self):
        h=self.harness();h.pm_ask(12,'123',(400,2));h.pm_ask(12,'987',(500,2))
        h.terminal();h.decoder.finish()
        with patch('replay.strategies.bounded_payout_optimizer.output.validate_content',side_effect=AssertionError('audit stalls terminal ACK')):
            h.strategy.finish()
        read_provisional(h.output,self.root/'context',expected_sha256=h.sha,bridge=h.strategy.bridge)

    def test_gross_branch_bounds_match_cartesian_with_nonlinear_nonnegative_fees(self):
        from itertools import product
        import random
        rng=random.Random(83)
        for case in range(30):
            books=[dict(key=(str(i),'outcome'),keys=frozenset(w for w in 'abc'if rng.randrange(2)),increment=1,cap=3,ask=rng.randrange(1,10),fee=rng.randrange(4))for i in range(4)]
            def pricing(b,q):return dict(key=b['key'],quantity=q,cash=-Fraction(b['ask']*q+b['fee']*(q%2),10),retained=Fraction(q),source=b['key'],taken=((1,q),))
            coefficients={b['key']:tuple(10*int(w in b['keys'])-b['ask']for w in 'abc')for b in books}
            found=solve(books,tuple('abc'),pricing,max_legs=3,max_nodes=10000,outcome_bounds=(10,coefficients))
            reference=[]
            for vector in product(range(4),repeat=4):
                orders=[pricing(b,q)for b,q in zip(books,vector)if q]
                if len(orders)>3:continue
                cost=-sum((o['cash']for o in orders),Fraction(0));floor=min(sum(q for b,q in zip(books,vector)if w in b['keys'])for w in 'abc')
                reference.append((-(floor-cost),cost,len(orders),tuple((o['key'],o['quantity'])for o in orders)))
            best=found['best']
            with self.subTest(case=case):
                self.assertTrue(found['complete'])
                self.assertEqual((-best['margin'],best['cost'],len(best['orders']),tuple((o['key'],o['quantity'])for o in best['orders'])),min(reference))

    def test_transport_is_bounded_canonical_and_copy_on_write(self):
        from replay.strategies.bounded_payout_optimizer.stream import pack,unpack,expand,changes
        old=pack({'type':'decision','value':{'items':['a','b'],'time':1}})
        new=pack({'type':'decision','value':{'items':['a','changed','c'],'time':2}})
        stored={'version':2,'index':1,'kind':'delta','patch':changes(old,new)}
        result=expand(stored,old,1)
        self.assertEqual(unpack(result)['value']['items'],['a','changed','c'])
        self.assertEqual(unpack(old)['value']['items'],['a','b'])
        for bad in ({**stored,'unexpected':1},{**stored,'index':True},{**stored,'patch':[1,999,[0,'bad']]}):
            with self.subTest(bad=bad),self.assertRaises(ProtocolError):expand(bad,old,1)
        with patch('replay.strategies.bounded_payout_optimizer.stream.MAX_LINE',8),self.assertRaises(ProtocolError):expand(stored,old,1)

    def test_primitive_cache_enforces_byte_bound_and_contains_no_proof_ids(self):
        from replay.strategies.bounded_payout_optimizer.scenario import PrimitiveCache
        cache=PrimitiveCache(4096)
        for i in range(100):cache.put((str(i),),{'quantity':i,'cash':Fraction(-i,10)})
        self.assertLessEqual(cache.retained_bytes,4096)
        self.assertLess(len(cache.items),100)

    def test_reader_rejects_certified_negative_that_hides_missing_fees(self):
        from replay.strategies.bounded_payout_optimizer import strategy
        def configure(cfg):
            from replay.fees.artifacts import build_catalog
            from replay.fees.schedules import Catalog
            catalog=Catalog.build(());directory=build_catalog(self.root/'no-fees',catalog,{})
            cfg['fees'].update(catalog_directory=str(directory),catalog_identity=catalog.identity)
        h=self.harness(configure=configure,books={('polymarket:123','outcome'),('polymarket:987','outcome')})
        h.pm_ask(12,'123',(900,2));h.pm_ask(12,'987',(900,2))
        original=strategy.search
        def hide_unknown(*args,**kwargs):
            results,models=original(*args,**kwargs)
            for result in results:
                if 'found'in result:
                    result['found']['unknown_books']=[];result['fee_diagnostics']=[];result['pricing_failures']={};result['economic_status']='COMPLETE_NONPOSITIVE'
            return results,models
        with patch.object(strategy,'search',side_effect=hide_unknown),self.assertRaises(ProtocolError):h.finish()

if __name__ == '__main__':
    unittest.main()
