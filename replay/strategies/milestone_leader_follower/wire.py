"""Version 2 compact NDJSON transport; no economic decisions live here.

Native ladders are sparse exact replacements by price. Repeated relationship
objects are content-bound references. Valuation orders are reconstructed by the
independent reader; actual hypothetical cash-flow orders remain explicit.
"""
from copy import deepcopy
from replay.preparation import digest, encoded
from replay.streams.protocol import obj, require

# Fixed, versioned key symbols shorten repeated closed schemas. Unknown keys
# survive decoding and are rejected by the owning closed reader schema.
FIELDS = tuple('version role time_ns sequence scope knowledge released book_updates history_resets observations admission game phase score score_quality prefix prefix_quality released_results milestones elapsed_ns contradiction kind release_ns source_ns index winner key validity revision bids asks last_change_ns reason target route model_id cohort target_endpoint leader_endpoint displacement status sizes selected signal innovation prediction_id alternates previous_ns previous current_ns current delta price_revision quantity buy forecast_sale initial_sale forecast_charge forecast_net initial_net qualifies cash retained_atoms quantity_atoms taken leader market_id leader_market_id shape_id leader_keys target_keys feasible_keys relation proof rule_ids candidate_id books captured_books unresolved_members routes unsupported_routes attempt_id position_id maximum_quantity_atoms due_ns baseline_predictions order ledger revalidation horizon_ns side price_scale quantity_scale consumed cost_atoms source source_taken outcome asset assessment_ids assumptions evidence lineage forecast_displacement cash_before cash_after holdings_before holdings_after allocated_basis lot_pnl holding_charge lateness_ns payout payout_per_contract settlement_identity episode_id start_ns end_ns end_reason censored decision_ns desired_ns observed_ns forecast_error counterfactual_net capacity_mode sale claim_keys state opened_ns exit_reason exit_time_ns closed_ns holding_atoms opening_atoms opening_cost residual_basis closed_lot_pnl hypothetical_closed_pnl holding_cost_adjusted_closed_pnl residual_holding_charge liquidation_mark mark_status status_ns'.split())
SYMBOLS = {field: format(i, 'x') for i, field in enumerate(FIELDS)}
KEYS = {symbol:field for field,symbol in SYMBOLS.items()}


def compact_observation(row):
    result = deepcopy(row)
    for size in result['sizes']:
        if size['buy'] is not None:
            size['buy'] = {k:size['buy'][k] for k in ('cash','retained_atoms','quantity_atoms')}
        if size['forecast_sale'] is not None:
            size['forecast_sale'] = {'cash':size['forecast_sale']['cash']}
        if size['initial_sale'] is not None:
            size['initial_sale'] = {'cash':size['initial_sale']['cash'],'taken':[size['initial_sale']['taken'][0]]}
    return result


class Codec:
    def __init__(self, objects):
        self.objects = objects
        self.identities = {}
        self.object_bytes = 0
        self.books = {}

    def encode(self, row, *, new=True, full_books=False):
        def reference(value):
            identity = digest(value)
            if identity not in self.identities and new and self.object_bytes < 32*1024*1024:
                ident = len(self.identities)
                self.identities[identity] = ident
                self.objects.append({'version':2,'id':ident,'sha256':identity,'value':value})
                self.object_bytes += len(encoded(value)) + 128
            return self.identities.get(identity)
        def pack(value):
            if type(value) is list or type(value) is tuple:
                return [pack(x) for x in value]
            if type(value) is not dict:
                return value
            if 'model_id' in value and 'sizes' in value:
                value = compact_observation(value)
                changing = {k:value[k] for k in ('target_endpoint','leader_endpoint','innovation','prediction_id')}
                template = {k:v for k,v in value.items() if k not in changing}
                ident = reference(template)
                if ident is not None:
                    return {'!':ident,'+':pack(changing)}
            if 'candidate_id' in value and 'target_keys' in value:
                ident = reference(value)
                if ident is not None:
                    return {'@':ident}
            if 'validity' in value and 'bids' in value:
                key = tuple(value['key']); prior = self.books.get(key)
                self.books[key] = value
                if prior is not None and not full_books:
                    changes = {}
                    for side in ('bids','asks'):
                        old, current = dict(prior[side]), dict(value[side])
                        changes[side] = [[p,current.get(p,0)] for p in sorted(set(old)|set(current)) if old.get(p,0) != current.get(p,0)]
                    value = {**value,**changes,'delta_book':True}
                else:
                    value = {**value,'delta_book':False}
            return {SYMBOLS.get(k, ':'+k):pack(v) for k,v in value.items()}
        return {'version':2,'payload':pack(row)}


class Unpacker:
    def __init__(self, objects):
        self.objects = objects; self.books = {}

    def decode(self, row):
        obj(row,'version payload'); require(type(row['version']) is int and row['version'] == 2,'compact row version')
        def unpack(value):
            if type(value) is list:
                return [unpack(x) for x in value]
            if type(value) is not dict:
                return value
            if set(value) == {'@'}:
                ident = value['@']; require(type(ident) is int and ident in self.objects,'bound relationship reference')
                return deepcopy(self.objects[ident])
            if set(value) == {'!','+'}:
                ident = value['!']; require(type(ident) is int and ident in self.objects,'bound observation template')
                changing = unpack(value['+'])
                obj(changing,'target_endpoint leader_endpoint innovation prediction_id')
                template = deepcopy(self.objects[ident])
                require(not set(template)&set(changing),'closed observation template')
                return {**template,**changing}
            result = {}
            for symbol,item in value.items():
                if symbol.startswith(':'):
                    key = symbol[1:]
                else:
                    require(symbol in KEYS,'closed key symbol')
                    key = KEYS[symbol]
                require(key not in result,'unique decoded field')
                result[key] = unpack(item)
            if 'delta_book' in result:
                delta = result.pop('delta_book'); require(type(delta) is bool,'book delta label')
                key = tuple(result['key'])
                if delta:
                    require(key in self.books,'book delta predecessor')
                    for side in ('bids','asks'):
                        levels = dict(self.books[key][side]); changes = result[side]
                        require(len({p for p,q in changes}) == len(changes),'unique book delta price')
                        for p,q in changes:
                            require(type(p) is int and type(q) is int and q >= 0,'native book delta')
                            if q: levels[p] = q
                            else: levels.pop(p,None)
                        result[side] = [[p,q] for p,q in sorted(levels.items(),reverse=side=='bids')]
                self.books[key] = result
            return result
        value = unpack(row['payload'])
        require(type(value) is dict and value.get('version') == 2,'decoded row version')
        return value


class Writer:
    def __init__(self, writer, codec, *, batch=False):
        self.writer,self.codec = writer,codec
        self.stream = writer.stream
        self.batch = batch; self.time = None; self.pending = []

    def append(self, row):
        value = self.codec.encode({**row,'version':2})
        if not self.batch:
            self.writer.append(value); return
        time = row['observed_ns']
        if self.time is not None and self.time != time:
            self.flush()
        self.time = time; self.pending.append(value)
        require(len(self.pending) <= 768,'forecast batch bound')
        if len(self.pending) == 768:
            self.flush()

    def flush(self):
        if self.pending:
            self.writer.append({'version':2,'batch':self.pending})
            self.pending = []

    def finish(self):
        self.flush()
        return self.writer.finish()
