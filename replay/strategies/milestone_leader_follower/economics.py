"""Exact native orders, physical-source capacity and long-only scenario lots."""
from fractions import Fraction

from replay.economic_fills import walk
from replay.strategies._shared.fee_bridge import FeeEconomicsUnavailable
from replay.preparation import digest
from replay.streams.protocol import require
from .contract import number, r


def asset_record(asset):
    return {'kind': asset.kind.value, 'ledger': asset.chain, 'token': asset.token}


def atoms(amount, scale):
    value = amount * 10 ** scale
    return value.numerator if value.denominator == 1 else None


class Capacity:
    def __init__(self, mode):
        self.mode = mode
        self.debits = {}
        self.totals = {}

    def eligible(self, source, levels):
        if self.mode == 'ISOLATED_NONADDITIVE':
            return levels
        remaining = max(0, sum(q for _, q in levels) - self.totals.get(source, 0))
        result = []
        for price, quantity in levels:
            available = min(max(0, quantity - self.debits.get((source, price), 0)), remaining)
            if available:
                result.append((price, available))
                remaining -= available
        return tuple(result)

    def consume(self, order):
        if self.mode == 'ISOLATED_NONADDITIVE':
            return
        source = tuple(order['source'])
        for price, quantity in order['source_taken']:
            self.debits[source, price] = self.debits.get((source, price), 0) + quantity
            self.totals[source] = self.totals.get(source, 0) + quantity

    def record(self):
        return [{'source': list(source), 'total': str(self.totals[source]), 'prices': [[str(price), str(q)] for (s, price), q in sorted(self.debits.items()) if s == source]} for source in sorted(self.totals)]


class Pricing:
    def __init__(self, snapshot, fees, policy, experiment):
        self.plans = {(x['instrument'], x['orientation']): x for x in snapshot['plans']}
        self.rules = {(x['instrument'], x['orientation']): x for x in policy['rule_bindings']}
        self.fees, self.policy, self.experiment = fees, policy, experiment
        for item in policy['initial_cash']:
            require(fees._assets.get(item['venue']) is not None and item['asset'] == asset_record(fees._assets[item['venue']]), 'cash native asset binding')
        assets = {digest(item['asset']) for item in policy['initial_cash']}
        require(policy['valuation']['mode'] != 'SINGLE_NATIVE_ASSET' or len(assets) == 1, 'single native asset valuation')

    def weight(self, key):
        return number(self.policy['valuation']['weights'].get(self.plans[key]['venue'], '0'))

    def ladder(self, key, side, books, capacity=None):
        plan = self.plans[key]
        native = key
        physical_side = side
        if side == 'ask' and plan['venue'] == 'kalshi':
            native = (key[0], 'complement' if key[1] == 'outcome' else 'outcome')
            physical_side = 'bid'
        book = books.get(native)
        if book is None or book['validity'] != 'usable':
            return None, None
        levels = tuple(tuple(x) for x in book[physical_side + 's'])
        source = (*native, physical_side)
        if capacity is not None:
            levels = capacity.eligible(source, levels)
        if native != key:
            maximum = 10 ** int(plan['price_scale'])
            levels = tuple((maximum - p, q) for p, q in levels)
        return source, levels

    def midpoint(self, key, books):
        _, bids = self.ladder(key, 'bid', books)
        _, asks = self.ladder(key, 'ask', books)
        if not bids or not asks or bids[0][0] > asks[0][0]:
            return None
        return Fraction(bids[0][0] + asks[0][0], 2 * 10 ** int(self.plans[key]['price_scale']))

    def shift(self, key, bids, displacement):
        scale = 10 ** int(self.plans[key]['price_scale'])
        increment = int(self.rules[key]['price_increment_atoms'])
        merged = {}
        for price, quantity in bids:
            shifted = max(Fraction(0), min(Fraction(scale), price + displacement * scale))
            rounded = (shifted // increment) * increment
            merged[rounded] = merged.get(rounded, 0) + quantity
        return tuple(sorted(merged.items(), reverse=True))

    def order(self, key, side, quantity_atoms, books, capacity, time, sequence, scope, lineage, *, forecast=None):
        rule = self.rules.get(key)
        if rule is None or self.fees.economics(key) is None:
            return None, 'FEE_UNKNOWN'
        if quantity_atoms <= 0 or quantity_atoms % int(rule['quantity_increment_atoms']):
            return None, 'UNREPRESENTABLE_QUANTITY'
        source, levels = self.ladder(key, 'ask' if side == 'BUY' else 'bid', books, capacity)
        if levels is None:
            return None, 'UNUSABLE'
        native_levels = levels
        if forecast is not None:
            levels = self.shift(key, levels, forecast)
        if not levels:
            return None, 'NO_EXIT_DEPTH' if side == 'SELL' else 'DEPTH_LIMITED'
        fill = walk(levels, (quantity_atoms,))[0]
        if fill.depth_limited:
            return None, 'DEPTH_LIMITED'
        plan = self.plans[key]
        try:
            assessed, reasons = self.fees.assess_orders(experiment=self.experiment, scope=scope,
                basket={'lineage': lineage}, direction=side, size=quantity_atoms, time=time, sequence=sequence,
                legs=({'market_id': lineage['market_id'], 'key': key, 'fill': fill, 'price_scale': int(plan['price_scale']), 'quantity_scale': int(plan['quantity_scale']), 'side': side},), account=STRATEGY_ACCOUNT)
        except FeeEconomicsUnavailable:
            return None, 'FEE_UNKNOWN'
        economics, results = assessed[0] if assessed[0] is not None else (None, ())
        if reasons or economics is None or any(x.net_deltas is None for x in results):
            return None, 'FEE_UNKNOWN'
        cash = outcome = Fraction(0)
        for result in results:
            for delta in result.net_deltas:
                value = Fraction(delta.atoms, 10 ** delta.scale)
                if delta.asset == economics.quote:
                    cash += value
                elif delta.asset == economics.outcome:
                    outcome += value
                else:
                    return None, 'FEE_UNKNOWN'
        quantity_scale = int(plan['quantity_scale'])
        retained_atoms = atoms(outcome if side == 'BUY' else -outcome, quantity_scale)
        if retained_atoms is None or retained_atoms <= 0 or retained_atoms % int(rule['quantity_increment_atoms']):
            return None, 'UNREPRESENTABLE_HOLDINGS'
        if side == 'SELL' and retained_atoms > quantity_atoms:
            return None, 'OUTCOME_DEBIT_EXCEEDS_OWNED'
        # A forecast order does not consume anything. Its physical mapping is
        # carried only for validation; actual source debits always use unshifted prices.
        if source[:2] != key:
            maximum = 10 ** int(plan['price_scale'])
            source_taken = [(maximum - p, q) for p, q in fill.taken]
        else:
            source_taken = list(fill.taken)
        return {'key': list(key), 'side': side, 'quantity_atoms': str(quantity_atoms), 'price_scale': plan['price_scale'], 'quantity_scale': plan['quantity_scale'], 'levels': [list(x) for x in native_levels], 'taken': [list(x) for x in fill.taken], 'consumed': [list(x) for x in fill.consumed], 'cost_atoms': str(fill.cost), 'source': list(source), 'source_taken': [list(x) for x in source_taken], 'cash': r(cash), 'outcome': r(outcome), 'retained_atoms': str(retained_atoms), 'asset': asset_record(economics.quote), 'assessment_ids': [x.identity for x in results], 'assumptions': sorted({a for x in results for a in x.assumptions}), 'evidence': sorted({x.evidence.value for x in results}), 'time_ns': str(time), 'sequence': sequence, 'scope': scope, 'lineage': lineage, 'forecast_displacement': None if forecast is None else r(forecast)}, None

    def sizes(self, target, books, capacity, time, sequence, scope, route, displacement, maximum=None):
        result = []
        scale = int(self.plans[target]['quantity_scale'])
        for size in self.policy['quantity_grid']:
            q = atoms(number(size), scale)
            if maximum is not None and q is not None and q > maximum:
                continue
            row = {'quantity': size, 'status': 'UNREPRESENTABLE_QUANTITY', 'buy': None, 'forecast_sale': None, 'initial_sale': None, 'forecast_charge': None, 'forecast_net': None, 'initial_net': None, 'qualifies': False}
            if q is None:
                result.append(row); continue
            buy, reason = self.order(target, 'BUY', q, books, capacity, time, sequence, scope, route)
            if reason:
                row['status'] = reason; result.append(row); continue
            row['buy'] = buy
            retained = int(buy['retained_atoms'])
            future, reason = self.order(target, 'SELL', retained, books, capacity, time + int(self.policy['exit_horizon_ns']), sequence, scope, route, forecast=displacement)
            if reason:
                row['status'] = reason; result.append(row); continue
            row['forecast_sale'] = future
            current, reason = self.order(target, 'SELL', retained, books, capacity, time, sequence, scope, route)
            if reason:
                row['status'] = 'INITIAL_MARK_' + reason; result.append(row); continue
            row['initial_sale'] = current
            cost = -Fraction(buy['cash'])
            charge = cost * number(self.policy['holding_rate_per_ns']) * int(self.policy['exit_horizon_ns'])
            net = (Fraction(future['cash']) - cost - charge) * self.weight(target)
            initial = (Fraction(current['cash']) - cost) * self.weight(target)
            scalar_cost = cost * self.weight(target)
            qualifies = net > 0 and net >= number(self.policy['minimum_forecast_margin']) and 10000 * net >= number(self.policy['minimum_forecast_return_bps']) * scalar_cost and initial >= -number(self.policy['stop_loss_fraction']) * scalar_cost
            row.update(status='AVAILABLE', forecast_charge=r(charge), forecast_net=r(net), initial_net=r(initial), qualifies=qualifies)
            result.append(row)
        return result


STRATEGY_ACCOUNT = 'milestone_leader_follower_v1'


class Account:
    def __init__(self, policy, pricing):
        self.policy, self.pricing = policy, pricing
        self.cash = {x['venue']: number(x['amount']) for x in policy['initial_cash']}
        self.initial = dict(self.cash)
        self.capacity = Capacity(policy['capacity_mode'])
        self.positions = {}
        self.event_spend = Fraction(0)
        self.entries = 0
        self.closed_lot_pnl = Fraction(0)
        self.holding_charge = Fraction(0)

    def outstanding(self):
        return sum(p['basis'] * self.pricing.weight(p['target']) for p in self.positions.values() if p['holding_atoms'])

    def gate(self, order):
        if self.policy['capacity_mode'] == 'ISOLATED_NONADDITIVE':
            return 'ISOLATED_DETECTION_ONLY'
        key = tuple(order['key']); venue = self.pricing.plans[key]['venue']
        cost = -Fraction(order['cash']); scalar = cost * self.pricing.weight(key)
        if key in self.positions and self.positions[key]['holding_atoms']:
            return 'TARGET_RESIDUAL'
        if self.entries >= self.policy['max_entries_per_event']:
            return 'MAX_ENTRIES'
        if sum(p['holding_atoms'] > 0 for p in self.positions.values()) >= self.policy['maximum_concurrent_positions']:
            return 'MAX_CONCURRENT'
        if self.cash.get(venue, Fraction(0)) < cost:
            return 'INSUFFICIENT_CASH'
        if scalar > number(self.policy['transaction_budget']):
            return 'TRANSACTION_BUDGET'
        if scalar + self.event_spend > number(self.policy['event_budget']):
            return 'EVENT_BUDGET'
        if scalar + self.outstanding() > number(self.policy['maximum_outstanding_cost']):
            return 'OUTSTANDING_COST'
        return None

    def open(self, order, selected, time, knowledge):
        require(self.gate(order) is None, 'entry account gate')
        target = tuple(order['key']); venue = self.pricing.plans[target]['venue']
        before = self.cash[venue]
        self.cash[venue] += Fraction(order['cash'])
        self.capacity.consume(order)
        position = {'id': digest([selected['attempt_id'], time, target]), 'target': target, 'market_id': selected['route']['market_id'], 'shape_id': selected['route']['shape_id'], 'claim_keys': frozenset(selected['route']['target_keys']), 'route': selected['route'], 'holding_atoms': int(order['retained_atoms']), 'opening_atoms': int(order['retained_atoms']), 'cost': -Fraction(order['cash']), 'basis': -Fraction(order['cash']), 'opened': time, 'horizon': time + int(self.policy['exit_horizon_ns']), 'milestones': tuple(knowledge['milestones']), 'exit_reason': None, 'exit_time': None, 'closed_pnl': Fraction(0), 'holding_charge': Fraction(0), 'state': 'OPEN', 'settlement_due': None, 'payout': None, 'settlement_identity': None, 'attempt_id': selected['attempt_id'], 'baseline_predictions': selected['baseline_predictions']}
        self.positions[target] = position
        self.entries += 1
        self.event_spend += position['cost'] * self.pricing.weight(target)
        return position, {'cash_before': r(before), 'cash_after': r(self.cash[venue]), 'holdings_before': '0', 'holdings_after': str(position['holding_atoms']), 'allocated_basis': r(position['cost']), 'lot_pnl': '0', 'holding_charge': '0'}

    def dispose(self, position, order, time, *, payout=None):
        target = position['target']; venue = self.pricing.plans[target]['venue']
        before_cash, before_h = self.cash[venue], position['holding_atoms']
        sold = before_h if payout is not None else int(order['retained_atoms'])
        require(0 < sold <= before_h, 'no short/over-disposal')
        credit = payout if payout is not None else Fraction(order['cash'])
        basis = position['basis'] * Fraction(sold, before_h)
        charge = basis * number(self.policy['holding_rate_per_ns']) * (time - position['opened'])
        position['basis'] -= basis
        position['holding_atoms'] -= sold
        position['closed_pnl'] += credit - basis
        position['holding_charge'] += charge
        self.closed_lot_pnl += (credit - basis) * self.pricing.weight(target)
        self.holding_charge += charge * self.pricing.weight(target)
        self.cash[venue] += credit
        if payout is None:
            self.capacity.consume(order)
        if not position['holding_atoms']:
            position['state'] = 'CLOSED'
            position['closed'] = time
        else:
            position['state'] = 'EXIT_PENDING'
        return {'cash_before': r(before_cash), 'cash_after': r(self.cash[venue]), 'holdings_before': str(before_h), 'holdings_after': str(position['holding_atoms']), 'allocated_basis': r(basis), 'lot_pnl': r(credit - basis), 'holding_charge': r(charge)}

    def record(self, position, time):
        return {'position_id': position['id'], 'attempt_id': position['attempt_id'], 'target': list(position['target']), 'market_id': position['market_id'], 'claim_keys': sorted(position['claim_keys']), 'shape_id': position['shape_id'], 'state': position['state'], 'opened_ns': str(position['opened']), 'horizon_ns': str(position['horizon']), 'exit_reason': position['exit_reason'], 'exit_time_ns': None if position['exit_time'] is None else str(position['exit_time']), 'closed_ns': None if 'closed' not in position else str(position['closed']), 'holding_atoms': str(position['holding_atoms']), 'opening_atoms': str(position['opening_atoms']), 'opening_cost': r(position['cost']), 'residual_basis': r(position['basis']), 'closed_lot_pnl': r(position['closed_pnl']), 'holding_charge': r(position['holding_charge']), 'hypothetical_closed_pnl': r(position['closed_pnl']) if position['state'] == 'CLOSED' else None, 'holding_cost_adjusted_closed_pnl': r(position['closed_pnl'] - position['holding_charge']) if position['state'] == 'CLOSED' else None, 'residual_holding_charge': r(position['basis'] * number(self.policy['holding_rate_per_ns']) * (time - position['opened'])), 'milestones': list(position['milestones']), 'baseline_predictions': position['baseline_predictions']}
