"""Pure hypothetical assessment and a separate observed-total consistency API."""

from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction

from .domain import (
    AccountClass,
    Asset,
    AssetAmount,
    AssetDelta,
    AssetKind,
    Basis,
    Closed,
    Component,
    Evidence,
    FeeCharge,
    Fixed,
    HypotheticalFill,
    Rebate,
    Role,
    Side,
    Venue,
    ceil_grid,
    fixed,
)
from .schedules import (
    AccountRebate,
    Kalshi,
    KalshiKind,
    LimitlessAmm,
    LimitlessClob,
    Polymarket,
    ResolvedScheduleSet,
    Resolver,
    Rounding,
    TradeBasis,
    UnknownReason,
    UnknownSchedule,
    ZeroFee,
)


@dataclass(frozen=True, slots=True)
class Policy(Closed):
    name: str = "conservative-v1"
    include_account_rebates: bool = False
    include_rounding_refunds: bool = False
    assume_unknown_role_taker: bool = True
    kalshi_member_class: AccountClass = AccountClass.NON_DIRECT
    pm_ceil5_scenario: bool = True
    limitless_buy_bps: int = 300
    limitless_sell_bps: int = 150

    def __post_init__(self):
        Closed.__post_init__(self)
        if self.kalshi_member_class is AccountClass.UNKNOWN:
            raise ValueError("configured Kalshi member class must be known")
        if any(
            not 0 <= bps <= 10000
            for bps in (self.limitless_buy_bps, self.limitless_sell_bps)
        ):
            raise ValueError("Limitless rates must be in 0..10000 bps")


@dataclass(frozen=True, slots=True)
class OrderState(Closed):
    account: str
    subaccount: str
    order_key: str
    asset: Asset
    grid_scale: int
    last_fill_index: int
    last_event_time: int
    carry: Fixed
    policy_identity: str

    def __post_init__(self):
        Closed.__post_init__(self)
        if (
            self.grid_scale not in (2, 4)
            or self.last_fill_index < 0
            or self.last_event_time < 0
            or (self.carry.value * 10**6).denominator != 1
        ):
            raise ValueError("invalid Kalshi accumulator state")


@dataclass(frozen=True, slots=True)
class FeeAssessment(Closed):
    fill: HypotheticalFill
    resolved: ResolvedScheduleSet
    policy: Policy
    basis: Basis
    evidence: Evidence
    charges: tuple[FeeCharge, ...]
    rebates: tuple[Rebate, ...]
    unknowns: tuple[UnknownSchedule, ...]
    net_deltas: tuple[AssetDelta, ...] | None
    contract_fee_payout_impact: AssetAmount | None
    assumptions: tuple[str, ...]
    exclusions: tuple[str, ...]
    prior_order_state: OrderState | None
    engine_version: str = "fee-engine-v1"


@dataclass(frozen=True, slots=True)
class FeeEngine(Closed):
    """One immutable configuration for no-builder hypothetical fills."""

    policy: Policy = Policy()

    def assess(
        self,
        fill: HypotheticalFill,
        resolved: ResolvedScheduleSet,
        order_state: OrderState | None = None,
    ) -> tuple[FeeAssessment, OrderState | None]:
        policy = self.policy
        if (
            type(fill) is not HypotheticalFill
            or type(resolved) is not ResolvedScheduleSet
            or (order_state is not None and type(order_state) is not OrderState)
        ):
            raise TypeError("closed hypothetical inputs required")
        if resolved.context != fill.context or resolved.event_time != fill.event_time:
            raise ValueError("resolution belongs to a different fill context/time")
        charges, rebates, unknowns, assumptions = [], [], [], []
        exclusions = [
            "gas_bridge_deposit_withdrawal_fcm_costs",
            "maker_rewards_and_bonuses",
            "builder_fees_no_builder_route",
        ]
        if not policy.include_account_rebates:
            exclusions.append("hypothetical_account_rebates")
        role = fill.role
        if role is Role.UNKNOWN and policy.assume_unknown_role_taker:
            role = Role.TAKER
            assumptions.append("unknown_role_assumed_taker")
        basis = Basis.MAKER if role is Role.MAKER else Basis.BASE_TAKER
        e = fill.economics
        quantity, p, notional = (
            fill.quantity.value,
            fill.probability.value,
            fill.gross_notional.amount.value,
        )
        next_state = None

        def unknown(component, reason):
            unknowns.append(UnknownSchedule(component, reason))

        def charge(component, asset, amount, evidence=Evidence.EXACT_MODEL):
            charges.append(FeeCharge(component, AssetAmount(asset, amount), evidence))

        def rounded(raw, selection, rounding):
            if rounding is Rounding.CEIL:
                return ceil_grid(raw, selection.schedule.fee_scale)
            if (
                rounding is Rounding.EXACT
                and (raw * 10**selection.schedule.fee_scale).denominator == 1
            ):
                return fixed(raw, selection.schedule.fee_scale)
            unknown(selection.schedule.component, UnknownReason.ROUNDING)
            return None

        def validate_selection(selection):
            if isinstance(selection, UnknownSchedule):
                unknowns.append(selection)
                return False
            if selection.schedule.economics != e:
                raise ValueError("schedule economics identity mismatch")
            return True

        platform = resolved.platform
        if validate_selection(platform):
            s, model = platform.schedule, platform.model
            dimensional = True
            if isinstance(model, (Polymarket, Kalshi)):
                dimensional = (
                    e.payout.asset == e.quote
                    and e.payout.amount.value == 1
                    and fill.price.value == p
                    and s.fee_asset == e.quote
                )
                dimensional = dimensional and (
                    e.quote.kind is AssetKind.USD
                    if isinstance(model, Kalshi)
                    else e.quote.kind in (AssetKind.USDC, AssetKind.PUSD)
                )
            if not dimensional:
                unknown(Component.PLATFORM, UnknownReason.DIMENSIONS)
            elif isinstance(model, ZeroFee):
                # Kalshi platform-zero still needs the balance-rounding model.
                if fill.context.venue is Venue.KALSHI:
                    unknown(Component.PLATFORM, UnknownReason.UNSUPPORTED)
                else:
                    charge(Component.PLATFORM, s.fee_asset, Fixed(0, s.fee_scale))
            elif role is Role.UNKNOWN:
                unknown(Component.PLATFORM, UnknownReason.UNSUPPORTED)
            elif isinstance(model, Polymarket):
                raw = (
                    Fraction(0)
                    if role is Role.MAKER and model.taker_only
                    else quantity * model.rate.value * (p * (1 - p)) ** model.exponent
                )
                if raw == 0:
                    charge(Component.PLATFORM, s.fee_asset, Fixed(0, s.fee_scale))
                elif model.rounding is Rounding.PM_CEIL5_SCENARIO:
                    if policy.pm_ceil5_scenario and s.fee_scale == 5:
                        assumptions.append(
                            "ceil5_per_declared_fill_not_whole_execution_bound"
                        )
                        charge(
                            Component.PLATFORM,
                            s.fee_asset,
                            ceil_grid(raw, 5),
                            Evidence.ESTIMATE,
                        )
                    else:
                        unknown(Component.PLATFORM, UnknownReason.ROUNDING)
                else:
                    amount = rounded(raw, platform, model.rounding)
                    if amount is not None:
                        charge(Component.PLATFORM, s.fee_asset, amount)
            elif isinstance(model, Kalshi):
                if (
                    model.kind not in (KalshiKind.QUADRATIC, KalshiKind.MAKER)
                    or model.multiplier is None
                    or s.fee_scale != 6
                ):
                    unknown(Component.PLATFORM, UnknownReason.UNSUPPORTED)
                elif (notional * 10**6).denominator != 1:
                    raise ValueError(
                        "Kalshi revenue must be exactly representable at six decimals"
                    )
                else:
                    rate = (
                        Fraction(7, 100)
                        if role is Role.TAKER
                        else (
                            Fraction(7, 400)
                            if model.kind is KalshiKind.MAKER
                            else Fraction(0)
                        )
                    )
                    trade_fee = ceil_grid(
                        rate * model.multiplier.value * quantity * p * (1 - p), 6
                    )
                    charge(Component.PLATFORM, e.quote, trade_fee)
                    grid_scale = (
                        4 if policy.kalshi_member_class is AccountClass.DIRECT else 2
                    )
                    grid = Fraction(1, 10**grid_scale)
                    revenue = -notional if fill.side is Side.BUY else notional
                    change = revenue - trade_fee.value
                    rounding = change - (change // grid) * grid
                    charge(Component.ROUNDING, e.quote, fixed(rounding, 6))
                    state_ok = False
                    previous = Fraction(0)
                    if order_state is None:
                        state_ok = fill.new_order
                    else:
                        state_ok = (
                            not fill.new_order
                            and (
                                order_state.account,
                                order_state.subaccount,
                                order_state.order_key,
                                order_state.asset,
                                order_state.grid_scale,
                                order_state.last_fill_index + 1,
                                order_state.policy_identity,
                            )
                            == (
                                fill.context.account,
                                fill.context.subaccount,
                                fill.order_key,
                                e.quote,
                                grid_scale,
                                fill.fill_index,
                                policy.identity,
                            )
                            and order_state.last_event_time <= fill.event_time
                        )
                        previous = order_state.carry.value
                    state_ok = state_ok and fill.role is not Role.UNKNOWN
                    if state_ok:
                        accumulated = previous + rounding
                        rebate = grid * min(
                            accumulated // grid,
                            (trade_fee.value + rounding) // grid,
                        )
                        next_state = OrderState(
                            fill.context.account,
                            fill.context.subaccount,
                            fill.order_key,
                            e.quote,
                            grid_scale,
                            fill.fill_index,
                            fill.event_time,
                            fixed(accumulated - rebate, 6),
                            policy.identity,
                        )
                        if policy.include_rounding_refunds:
                            rebates.append(
                                Rebate(
                                    Component.ROUNDING,
                                    AssetAmount(e.quote, fixed(rebate, 6)),
                                    Evidence.EXACT_MODEL,
                                )
                            )
                    elif policy.include_rounding_refunds:
                        unknown(Component.ROUNDING, UnknownReason.STATE)
                    if not policy.include_rounding_refunds:
                        assumptions.append("kalshi_per_fill_rebates_omitted")
            elif isinstance(model, LimitlessClob):
                if role is Role.MAKER and model.maker_free:
                    charge(Component.PLATFORM, s.fee_asset, Fixed(0, s.fee_scale))
                elif role is Role.TAKER:
                    asset = e.outcome if fill.side is Side.BUY else e.quote
                    # CLOB collateral is pinned on the schedule; the BUY token
                    # is independently pinned by its InstrumentEconomics.
                    if s.fee_asset != e.quote or s.fee_scale != 6:
                        unknown(Component.PLATFORM, UnknownReason.DIMENSIONS)
                    else:
                        raw = (
                            quantity * Fraction(policy.limitless_buy_bps, 10000)
                            if fill.side is Side.BUY
                            else notional * Fraction(policy.limitless_sell_bps, 10000)
                        )
                        assumptions.append(
                            "limitless_configured_rate_and_ceil6_declared_fill"
                        )
                        charge(
                            Component.PLATFORM,
                            asset,
                            ceil_grid(raw, 6),
                            Evidence.ESTIMATE,
                        )
                else:
                    unknown(Component.PLATFORM, UnknownReason.UNSUPPORTED)
            elif isinstance(model, LimitlessAmm):
                asset = e.quote if model.basis is TradeBasis.NOTIONAL else e.outcome
                if s.fee_asset != asset or model.rate.value != Fraction(4, 1000):
                    unknown(Component.PLATFORM, UnknownReason.DIMENSIONS)
                else:
                    raw = (
                        notional if model.basis is TradeBasis.NOTIONAL else quantity
                    ) * model.rate.value
                    amount = rounded(raw, platform, model.rounding)
                    if amount is not None:
                        charge(Component.PLATFORM, asset, amount)
            else:
                unknown(Component.PLATFORM, UnknownReason.UNSUPPORTED)

        if (
            policy.include_account_rebates
            and fill.context.venue is Venue.POLYMARKET
            and role is Role.TAKER
        ) and validate_selection(resolved.account):
            selection = resolved.account
            if isinstance(selection.model, AccountRebate):
                base = next(
                    (c for c in charges if c.component is Component.PLATFORM), None
                )
                if (
                    base is not None
                    and base.amount.asset == selection.schedule.fee_asset
                ):
                    amount = rounded(
                        base.amount.amount.value * selection.model.rate.value,
                        selection,
                        selection.model.rounding,
                    )
                    if amount is not None:
                        rebates.append(
                            Rebate(
                                Component.ACCOUNT,
                                AssetAmount(selection.schedule.fee_asset, amount),
                                base.evidence,
                            )
                        )
                        basis = Basis.ACCOUNT_ADJUSTED
                else:
                    unknown(Component.ACCOUNT, UnknownReason.DIMENSIONS)
            else:
                unknown(Component.ACCOUNT, UnknownReason.UNSUPPORTED)

        net, payout = None, None
        if not unknowns:
            sign = 1 if fill.side is Side.BUY else -1
            amounts = {e.quote: -sign * notional, e.outcome: sign * quantity}
            contract_fee = Fraction(0)
            for entries, direction in ((charges, -1), (rebates, 1)):
                for item in entries:
                    asset, value = item.amount.asset, item.amount.amount.value
                    amounts[asset] = amounts.get(asset, Fraction(0)) + direction * value
                    if asset == e.outcome:
                        contract_fee -= direction * value
            # Native deltas, never a scalar total across currencies/tokens.
            net = tuple(
                AssetDelta(a, int(v * 10**18), 18)
                for a, v in sorted(amounts.items(), key=lambda pair: pair[0].identity)
            )
            payout = AssetAmount(
                e.payout.asset, fixed(contract_fee * e.payout.amount.value, 18)
            )
        evidence = Evidence.UNKNOWN if unknowns else Evidence.EXACT_MODEL
        if not unknowns and (
            assumptions or any(c.evidence is Evidence.ESTIMATE for c in charges)
        ):
            evidence = Evidence.ESTIMATE
            if assumptions == ["kalshi_per_fill_rebates_omitted"] and all(
                c.evidence is Evidence.EXACT_MODEL for c in charges
            ):
                evidence = Evidence.CONDITIONAL_BOUND
        return FeeAssessment(
            fill,
            resolved,
            policy,
            basis,
            evidence,
            tuple(charges),
            tuple(rebates),
            tuple(unknowns),
            net,
            payout,
            tuple(assumptions),
            tuple(exclusions),
            order_state,
        ), next_state

    def assess_many(
        self,
        fills: tuple[HypotheticalFill, ...],
        resolver: Resolver,
        knowledge_cutoff: int | None = None,
    ) -> tuple[FeeAssessment, ...]:
        """Input order is authoritative. Unknown/intervening fills invalidate carry."""
        states, seen, results = {}, set(), []
        last_time = -1
        for fill in fills:
            key = (
                fill.context.venue,
                fill.context.account,
                fill.context.subaccount,
                fill.order_key,
            )
            if fill.event_time < last_time or fill.identity in seen:
                raise ValueError("duplicate or out-of-order fills")
            if fill.new_order and key in states:
                raise ValueError("duplicate new-order declaration")
            last_time = fill.event_time
            seen.add(fill.identity)
            result, state = self.assess(
                fill,
                resolver.resolve(fill.context, fill.event_time, knowledge_cutoff),
                states.get(key),
            )
            states[key] = state
            results.append(result)
        return tuple(results)


@dataclass(frozen=True, slots=True)
class ObservedTotal(Closed):
    gross: AssetAmount
    fee: AssetAmount
    net: AssetAmount


@dataclass(frozen=True, slots=True)
class ObservedExecution(Closed):
    execution_id: str
    source_identity: str
    totals: tuple[ObservedTotal, ...]


@dataclass(frozen=True, slots=True)
class ObservedFeeAssessment(Closed):
    observed: ObservedExecution
    evidence: Evidence = Evidence.ACTUAL_OBSERVED


def validate_actual(observed: ObservedExecution) -> ObservedFeeAssessment:
    """Validate caller-supplied observed received-asset totals, not authenticity."""
    if (
        type(observed) is not ObservedExecution
        or not observed.execution_id
        or not observed.source_identity
        or not observed.totals
    ):
        raise ValueError("observed execution with source identity required")
    seen = set()
    for total in observed.totals:
        if (
            not total.gross.asset == total.fee.asset == total.net.asset
            or total.gross.asset in seen
        ):
            raise ValueError("observed asset mismatch or duplicate")
        if total.gross.amount.value - total.fee.amount.value != total.net.amount.value:
            raise ValueError("observed gross/fee/net mismatch")
        seen.add(total.gross.asset)
    return ObservedFeeAssessment(observed)
