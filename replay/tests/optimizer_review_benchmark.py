"""Offline optimizer timing and transport volume rehearsal; no user evidence.

Run with the project Python: python -m replay.tests.optimizer_review_benchmark
--negative-updates 100000 --positive-updates 1000 --volume-rows 100000.
The volume rehearsal substitutes synthetic assessment hashes; it verifies the
transport, not the economics of 100000 independently reassessed transactions.
"""
import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from time import perf_counter
from unittest.mock import patch

from replay.preparation import encoded
from replay.strategies.bounded_payout_optimizer import read_provisional
from replay.strategies.bounded_payout_optimizer.contract import MAX_BYTES,MAX_LINE,MAX_ROWS
from replay.strategies.bounded_payout_optimizer.stream import DecisionWriter,expand,unpack
from replay.tests.test_bounded_payout_optimizer import OptimizerHarness
from replay.tests import test_same_venue_multi_market as multi


def event(kind,count):
    original=multi._PREPARE;end=count+100
    def extended(config,directory,**kwargs):
        config=deepcopy(config);config['end_ns']=str(end);config['occurrences'][-1]['end_ns']=str(end)
        return original(config,directory,**kwargs)
    with tempfile.TemporaryDirectory()as tmp:
        root=Path(tmp);positive=kind=='fee_positive';hard=kind=='hard_overlap'
        with patch.object(multi,'_PREPARE',side_effect=extended):
            h=OptimizerHarness(root,cap='100',books={('kalshi:series','outcome'),('kalshi:beta','outcome')}if positive else None,fees='collateral'if positive else'zero')
        try:
            h.window(end=end)
            if positive:
                h.kalshi_ask(12,'series','outcome',(48,100));h.kalshi_ask(12,'beta','outcome',(48,100))
            else:
                for book in h.optimizer_config['policy']['quantities']:
                    instrument,orientation=book['instrument'],book['orientation']
                    if instrument.startswith('kalshi:'):h.kalshi_ask(12,instrument.split(':')[1],orientation,(60 if hard else 90,100))
                    else:h.pm_ask(12,instrument.split(':')[1],(600 if hard else 900,100))
            start=perf_counter();h.group(13);initial_seconds=perf_counter()-start
            worst_callback_seconds=initial_seconds;start=perf_counter()
            for index in range(count):
                callback_start=perf_counter()
                if positive:h.kalshi_ask(14+index,'series','outcome',(47+index%2,100))
                else:h.pm_ask(14+index,'123',((600 if hard else 900)+index%2,100))
                worst_callback_seconds=max(worst_callback_seconds,perf_counter()-callback_start)
            prior=unpack(h.strategy.writer.previous);callback_start=perf_counter();h.strategy.flush()
            worst_callback_seconds=max(worst_callback_seconds,perf_counter()-callback_start);last=unpack(h.strategy.writer.previous)
            write_seconds=perf_counter()-start;start=perf_counter()
            h.terminal();h.decoder.finish();h.strategy.finish();finish_seconds=perf_counter()-start
            start=perf_counter();read_provisional(h.output,root/'context',expected_sha256=h.sha,bridge=h.strategy.bridge)
            audit_seconds=perf_counter()-start
            return dict(kind=kind,updates=count,write_seconds=write_seconds,initial_decision_seconds=initial_seconds,worst_callback_seconds=worst_callback_seconds,
                finish_seconds=finish_seconds,independent_audit_seconds=audit_seconds,
                output_bytes={p.name:p.stat().st_size for p in h.output.iterdir()},last_search=last['search_budget'],cache_bytes=h.strategy.pricing_cache.retained_bytes),[prior,last]
        finally:h.close()


def volume(pair,count):
    """Real row shapes, changed hashes/clocks, two alternating fee-positive books."""
    def at_time(node,time,identities):
        if type(node)is dict:
            if 'fee_priced_ns'in node and node['fee_priced_ns']>12:
                result={key:at_time(value,time,identities)for key,value in node.items()}
                result['fee_priced_ns']=time;result['fee_priced_sequence']=time
                result['assessment_ids']=[identities.setdefault(old,hashlib.sha256(encoded([old,time])).hexdigest())for old in node['assessment_ids']]
                return result
            return {key:at_time(value,time,identities)for key,value in node.items()}
        if type(node)is list:return [at_time(value,time,identities)for value in node]
        return node
    with tempfile.TemporaryDirectory()as tmp:
        root=Path(tmp);limits=dict(max_bytes=MAX_BYTES,max_records=MAX_ROWS,max_line_bytes=MAX_LINE)
        decisions=DecisionWriter(root/'decisions.ndjson',**limits);episodes=DecisionWriter(root/'episodes.ndjson',**limits)
        start=perf_counter()
        for index in range(count):
            time=100+index;row=at_time(pair[index%2],time,{})
            row['t_ns']=str(time);row['sequence']=time
            for book in row['books']:book['last_change_ns']=str(time)
            decisions.append(row)
            best=row['scenarios'][0]['detection'][0]['found']['best']
            episodes.append(dict(id=hashlib.sha256(encoded(['episode',time])).hexdigest(),scenario='static',semantics=hashlib.sha256(encoded(['semantics',index%2])).hexdigest(),
                start_ns=time,end_ns=time+1,duration_ns=1,censored=False,reason='SIGNAL_CHANGED',scope=row['scope'],shape=row['scenarios'][0]['detection'][0]['shape'],
                outcomes=row['scenarios'][0]['detection'][0]['outcomes'],opening_portfolio=best,maximum_margin=best['margin']))
        identities={name:writer.finish()for name,writer in [('decisions.ndjson',decisions),('episodes.ndjson',episodes)]}
        write_seconds=perf_counter()-start;start=perf_counter()
        for name,identity in identities.items():
            prior=None;hasher=hashlib.sha256();size=records=0
            with (root/name).open('rb')as stream:
                while raw:=stream.readline(MAX_LINE+1):
                    assert raw.endswith(b'\n')and len(raw)<=MAX_LINE
                    stored=json.loads(raw);assert encoded(stored)+b'\n'==raw
                    prior=expand(stored,prior,records);unpack(prior)
                    hasher.update(raw);size+=len(raw);records+=1
            assert dict(sha256=hasher.hexdigest(),byte_length=size,records=records)==identity
            assert records==count and size<MAX_BYTES
        return dict(kind='transport_volume_only_synthetic_assessment_hashes',rows_per_file=count,write_seconds=write_seconds,
            transport_audit_seconds=perf_counter()-start,files=identities,total_ndjson_bytes=sum(v['byte_length']for v in identities.values()))


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--negative-updates',type=int,default=1000);parser.add_argument('--positive-updates',type=int,default=1000);parser.add_argument('--hard-updates',type=int,default=100);parser.add_argument('--volume-rows',type=int,default=0)
    options=parser.parse_args()
    for kind,count in [('negative',options.negative_updates),('fee_positive',options.positive_updates)]:
        assert 2<=count<=100000
        report,pair=event(kind,count);print(json.dumps(report),flush=True)
    if options.volume_rows:
        assert 2<=options.volume_rows<=100000
        print(json.dumps(volume(pair,options.volume_rows)),flush=True)
    if options.hard_updates:
        assert 2<=options.hard_updates<=100000
        report,_=event('hard_overlap',options.hard_updates);print(json.dumps(report),flush=True)


if __name__=='__main__':main()
