"""Opt-in synthetic Decoder volume check; no live feeds or retained data.

Run: python -m replay.tests.benchmark_leader_follower --changes 100000
"""
import argparse
import json
from pathlib import Path
import tempfile
import time
import sqlite3
from unittest.mock import patch

from replay.tests.test_milestone_leader_follower import Harness
from replay.tests.economic_scenarios import ladder, M
from replay.strategies.milestone_leader_follower import read_provisional


def run(changes):
    with tempfile.TemporaryDirectory(prefix='leader-volume-') as temporary:
        root = Path(temporary); end = changes + 100
        h = Harness(root,end_ns=end)
        h.window(end=end); h.leaders(11); h.target(11)
        bids = [(520-i,100*M) for i in range(100)]
        asks = [(530+i,100*M) for i in range(100)]
        timings = []; start = time.perf_counter()
        for i in range(changes):
            bids[-1] = (421, (100+(i%2))*M)
            then = time.perf_counter()
            ladder(h,12+i,'polymarket:123',bids=tuple(bids),asks=tuple(asks))
            timings.append(time.perf_counter()-then)
            if (i+1) % 10000 == 0:
                print(json.dumps({'checkpoint_changes':i+1,'runtime_seconds':time.perf_counter()-start,'bytes':{p.name:p.stat().st_size for p in h.output.glob('*.ndjson')}}),flush=True)
        h.terminal(); h.decoder.finish()
        runtime = time.perf_counter()-start
        start = time.perf_counter(); h.strategy.finish(); finish = time.perf_counter()-start
        manifest = json.loads((h.output/'manifest.json').read_bytes())
        print(json.dumps({'runtime_seconds':runtime,'finish_seconds':finish,'book_changes':changes,'files':manifest['files'],'stage':'writer_complete'}),flush=True)
        indexes = []; connect = sqlite3.connect
        class MeasuredConnection(sqlite3.Connection):
            def close(self):
                indexes.append(Path(self.execute('PRAGMA database_list').fetchone()[2]).stat().st_size)
                return super().close()
        def measured_connect(*args,**kwargs):
            return connect(*args,**kwargs,factory=MeasuredConnection)
        start = time.perf_counter()
        with patch('replay.strategies.milestone_leader_follower.output.sqlite3.connect',measured_connect):
            read_provisional(h.output,root/'context',expected_sha256=h.sha)
        audit = time.perf_counter()-start
        result = {'book_changes':changes,'runtime_seconds':runtime,'finish_seconds':finish,'independent_audit_seconds':audit,'independent_index_bytes':max(indexes),'maximum_callback_seconds':max(timings),'files':manifest['files'],'total_bytes':sum(x['byte_length'] for x in manifest['files'].values())}
        print(json.dumps(result,sort_keys=True),flush=True)
        return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--changes',type=int,default=100000)
    run(parser.parse_args().changes)
