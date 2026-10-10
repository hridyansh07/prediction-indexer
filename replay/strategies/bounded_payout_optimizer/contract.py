"""Closed pinned configuration for hypothetical bounded payout portfolios."""
from fractions import Fraction
import re

from replay.economic_sdk.game import game_policy, experiment_policy
from replay.preparation import digest, encoded, sha
from replay.strategy_sdk import PreparedInput, plain
from replay.streams.protocol import obj, require, uint
from replay.strategies._shared.fee_bridge import FeeBridge
from replay.strategies.cross_venue_arbitrage.contract import asset_value

NAME = 'bounded_payout_optimizer_v1'
MAX_LINE = 8*1024*1024
MAX_BYTES = 512*1024*1024
MAX_ROWS = 1000000


def decimal(value, *, positive=False):
    require(type(value) is str and re.fullmatch(r'(0|[1-9][0-9]*)(\.[0-9]*[1-9])?', value) is not None,
            'canonical unsigned decimal')
    require(len(value) <= 80, 'decimal bound')
    result = Fraction(value)
    require(result >= 0 and (not positive or result > 0), 'positive decimal')
    return result


def exact(value):
    return str(value.numerator) if value.denominator == 1 else str(value.numerator)+'/'+str(value.denominator)


def wire(value):
    if isinstance(value, Fraction): return exact(value)
    if isinstance(value, dict): return {k:wire(v) for k,v in value.items()}
    if isinstance(value, (list,tuple)): return [wire(v) for v in value]
    if isinstance(value, (set,frozenset)): return sorted(value)
    return value


def cash_key(venue, asset):
    return venue, digest(asset)


def validate(config):
    config = obj(plain(config), 'version source_revision snapshot_directory snapshot_sha256 fees policy valuation account rules')
    require(type(config['source_revision']) is str and re.fullmatch(r'[0-9a-f]{40}|[0-9a-f]{64}',config['source_revision']) is not None, 'pinned source revision')
    require(type(config['version']) is int and config['version'] == 1, 'optimizer config version')
    require(len(encoded(config)) <= MAX_LINE, 'optimizer configuration budget')
    p = config['policy']
    obj(p, 'version max_legs max_books max_outcomes max_search_nodes alternatives quantities minimum_net_margin minimum_return_bps decision_delay_ns max_entries_per_event rearm_nonpositive_ns holding_cost settlement'+(' game' if 'game' in p else ''))
    require(type(p['version']) is int and p['version'] == 1, 'optimizer policy version')
    for name,lower,upper in [('max_legs',1,8),('max_books',1,128),('max_outcomes',1,1024),
                             ('max_search_nodes',1,10000000),('alternatives',0,16),('max_entries_per_event',1,1000)]:
        require(type(p[name]) is int and lower <= p[name] <= upper, name)
    for name in ('minimum_net_margin',): decimal(p[name])
    for name in ('minimum_return_bps','decision_delay_ns','rearm_nonpositive_ns'): uint(p[name])
    require(p['max_entries_per_event'] == 1 or int(p['rearm_nonpositive_ns']) > 0, 'positive rearm duration')
    rows = p['quantities']; require(type(rows) is list and len(rows) <= p['max_books'], 'quantity books bound')
    keys = []
    for row in rows:
        obj(row,'instrument orientation increment cap'); keys.append((row['instrument'],row['orientation']))
        inc,cap = decimal(row['increment'],positive=True),decimal(row['cap'],positive=True)
        require(cap >= inc and cap/inc <= 10000000, 'quantity domain bound')
    require(keys == sorted(set(keys)), 'quantity rules sorted unique')
    h = p['holding_cost']; obj(h,'rate_per_ns maximum_duration_ns')
    rate = decimal(h['rate_per_ns'])
    require(h['maximum_duration_ns'] is None or uint(h['maximum_duration_ns']) > 0, 'holding duration')
    s = p['settlement']
    if s is not None:
        require(s['mode'] in ('NORMAL_RESOLUTION_SCENARIO','PINNED_SETTLEMENT'), 'settlement mode')
        if s['mode'] == 'NORMAL_RESOLUTION_SCENARIO':
            obj(s,'mode delay_ns'); uint(s['delay_ns'])
        else:
            obj(s,'mode payouts'); require(type(s['payouts']) is list and len(s['payouts']) <= 128, 'pinned settlements bound')
            skeys = []
            for row in s['payouts']:
                obj(row,'instrument orientation payout availability_ns evidence_sha256')
                require(row['payout'] in ('0','1'), 'unit normal payout'); uint(row['availability_ns']); sha(row['evidence_sha256'])
                skeys.append((row['instrument'],row['orientation']))
            require(skeys == sorted(set(skeys)), 'settlements sorted unique')
    if 'game' in p: game_policy(p['game'])
    v = config['valuation']
    if v is not None:
        obj(v,'kind unit scenarios'); require(v['kind'] in ('PARITY_SCENARIO','STRESS_SCENARIO'), 'valuation kind')
        require(type(v['unit']) is str and bool(v['unit']), 'valuation unit')
        require(type(v['scenarios']) is list and 1 <= len(v['scenarios']) <= 16, 'valuation scenario bound')
        names = []
        for scenario in v['scenarios']:
            obj(scenario,'name weights'); names.append(scenario['name'])
            require(type(scenario['name']) is str and bool(scenario['name']), 'valuation name')
            require(type(scenario['weights']) is list and 1 <= len(scenario['weights']) <= 16, 'valuation weight bound')
            assets = []
            for row in scenario['weights']:
                obj(row,'asset weight'); asset_value(row['asset']); assets.append(encoded(row['asset']))
                weight = decimal(row['weight'],positive=True)
                require(v['kind'] != 'PARITY_SCENARIO' or weight == 1, 'parity weights')
            require(assets == sorted(set(assets)), 'valuation assets sorted unique')
        require(names == sorted(set(names)), 'valuation names sorted unique')
    account = obj(config['account'], 'initial_cash transaction_budget event_budget max_outstanding_cost max_open_positions')
    for field in ('transaction_budget','event_budget','max_outstanding_cost'): decimal(account[field],positive=True)
    require(type(account['max_open_positions']) is int and 1 <= account['max_open_positions'] <= 1000, 'position count bound')
    require(type(account['initial_cash']) is list and 1 <= len(account['initial_cash']) <= 48, 'cash account bound')
    accounts = []
    for row in account['initial_cash']:
        obj(row,'venue asset amount'); asset_value(row['asset']); decimal(row['amount']); accounts.append(cash_key(row['venue'],row['asset']))
        require(row['venue'] in ('kalshi','polymarket','limitless'), 'native account venue')
    require(accounts == sorted(set(accounts)), 'accounts sorted unique')
    rules = obj(config['rules'], 'event_id compatibility_id evidence_sha256 normal_resolution_only assumptions books')
    require(type(rules['normal_resolution_only']) is bool and rules['normal_resolution_only'], 'normal resolution boundary')
    sha(rules['evidence_sha256']); sha(rules['compatibility_id'])
    require(type(rules['assumptions']) is list and all(type(a) is str and a for a in rules['assumptions']) and
            rules['assumptions'] == sorted(set(rules['assumptions'])), 'reviewed rule assumptions')
    require(type(rules['books']) is list and len(rules['books']) <= p['max_books'], 'rule books bound')
    rkeys=[]
    for row in rules['books']:
        obj(row,'instrument orientation rule_identity'); sha(row['rule_identity']); rkeys.append((row['instrument'],row['orientation']))
    require(rkeys == sorted(set(rkeys)), 'rule bindings sorted unique')
    return config


class Inputs:
    def __init__(self, config):
        self.config = validate(config)
        self.prepared = PreparedInput({k:self.config[k] for k in ('version','snapshot_directory','snapshot_sha256')})
        self.snapshot = plain(self.prepared.snapshot)
        doc = self.snapshot.get('outcomes',{}).get('document')
        require(doc is None or self.config['rules']['event_id'] == doc['event_id'], 'rule event alignment')
        self.bridge = FeeBridge(self.config['fees'],self.snapshot['plans'])
        p = self.config['policy']
        self.identity = digest({'strategy':NAME,'snapshot_sha256':self.prepared.sha256,
            'source_revision':self.config['source_revision'],'policy':experiment_policy(p),'fees':self.bridge.semantic_config,
            'valuation':self.config['valuation'],'account':self.config['account'],'rules':self.config['rules']})
