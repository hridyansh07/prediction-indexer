"""Offline contract-shaped vectors; no live payload fixtures or historical claims."""

import random
import subprocess
import sys
import tempfile
import unittest
from dataclasses import FrozenInstanceError, replace
from fractions import Fraction
from pathlib import Path
from unittest.mock import patch

from replay.fees import *
from replay.fees.artifacts import (
    build_catalog,
    load_catalog,
    parse_canonical,
    source_from_bytes,
)
from replay.fees.domain import (
    AccountClass,
    Asset,
    AssetAmount,
    AssetKind,
    Basis,
    Builder,
    BuilderStatus,
    Component,
    Context,
    Evidence,
    Fixed,
    InstrumentEconomics,
    Multiplier,
    Notional,
    Probability,
    Product,
    Quantity,
    QuotePrice,
    Rate,
    Role,
    Side,
    Venue,
    canonical,
    fixed,
)
from replay.fees.schedules import (
    AccountRebate,
    BuilderFee,
    Kalshi,
    KalshiKind,
    LimitlessAmm,
    LimitlessClob,
    Polymarket,
    Rounding,
    Schedule,
    Scope,
    Selection,
    TradeBasis,
    UnknownReason,
    ZeroFee,
)

SOURCE_BYTES = b"hand-authored fee contract for offline tests; not venue evidence\n"
SOURCE = source_from_bytes("https://example.invalid/offline-contract", 10, SOURCE_BYTES)
EXACT = Policy(
    "model-with-rebates-v1", include_account_rebates=True, include_rounding_refunds=True
)
DIRECT = replace(EXACT, kalshi_member_class=AccountClass.DIRECT)
CONSERVATIVE = Policy()


def fill(venue=Venue.POLYMARKET, price="0.5", quantity="100", **changes):
    quote = (
        Asset(AssetKind.USD, "kalshi-ledger", "USD")
        if venue is Venue.KALSHI
        else Asset(AssetKind.USDC, "test-chain", "test-usdc")
    )
    outcome = Asset(AssetKind.OUTCOME, "test-chain", "market-yes-token")
    economics = InstrumentEconomics(
        quote, outcome, AssetAmount(quote, Fixed(1, 0)), 6, 6, 6
    )
    context = Context(
        venue,
        Product.CLOB,
        "market",
        "event",
        "series",
        "category",
        "instrument",
        "yes",
        "account",
        "subaccount",
    )
    p, q = QuotePrice.parse(price), Quantity.parse(quantity)
    value = HypotheticalFill(
        "fill",
        context,
        economics,
        Side.BUY,
        p,
        Probability(p.atoms, p.scale),
        q,
        Notional(quote, fixed(p.value * q.value, 6)),
        20,
        Role.TAKER,
        "order",
        0,
        True,
    )
    return replace(value, **changes)


def schedule(f, model=None, **changes):
    if model is None:
        model = {
            Venue.POLYMARKET: Polymarket(Rate.parse("0.07"), 1, True),
            Venue.KALSHI: Kalshi(KalshiKind.QUADRATIC, Multiplier(1, 0)),
            Venue.LIMITLESS: LimitlessClob(True),
        }[f.context.venue]
    scale = 5 if isinstance(model, Polymarket) else 6
    asset = f.economics.quote
    values = {
        "component": Component.PLATFORM,
        "scope": Scope(f.context.venue, f.context.product, market=f.context.market),
        "economics": f.economics,
        "fee_asset": asset,
        "fee_scale": scale,
        "model": model,
        "sources": (SOURCE,),
        "effective_from": 10,
        "effective_to": 100,
        "effective_evidence": "synthetic test interval",
        "extractor_version": "test-extractor-v1",
        "model_version": "test-model-v1",
    }
    values.update(changes)
    return Schedule(**values)


def assess(f, schedules=None, policy=CONSERVATIVE, state=None):
    resolver = Resolver(
        Catalog.build([schedule(f)] if schedules is None else schedules)
    )
    return FeeEngine(policy).assess(
        f, resolver.resolve(f.context, f.event_time, None), state
    )


def delta(result, asset):
    return next(v.value for v in result.net_deltas if v.asset == asset)


def total_fee(result):
    # Used only in single-quote-asset Kalshi assertions.
    return sum((c.amount.amount.value for c in result.charges), Fraction(0)) - sum(
        (r.amount.amount.value for r in result.rebates), Fraction(0)
    )


class ConfiguredEngineTests(unittest.TestCase):
    def evaluate(self, engine, f, state=None):
        resolved = Resolver(Catalog.build([schedule(f)])).resolve(
            f.context, f.event_time, None
        )
        return engine.assess(f, resolved, order_state=state)

    def test_default_limitless_native_and_dust_vectors(self):
        engine = FeeEngine()
        for side, price, quantity, fee, contracts, quote in (
            (Side.BUY, "0.4", "100", "3", "97", "-40"),
            (Side.SELL, "0.5", "100", "0.75", "-100", "49.25"),
            (Side.BUY, "0.4", "0.00005", "0.000002", "0.000048", "-0.00002"),
            (Side.SELL, "0.5", "0.0001", "0.000001", "-0.0001", "0.000049"),
        ):
            with self.subTest(side=side, quantity=quantity):
                f = fill(Venue.LIMITLESS, price, quantity, side=side)
                result, _ = self.evaluate(engine, f)
                self.assertFalse(result.unknowns)
                self.assertEqual(total_fee(result), Fraction(fee))
                self.assertEqual(delta(result, f.economics.outcome), Fraction(contracts))
                self.assertEqual(delta(result, f.economics.quote), Fraction(quote))

    def test_independent_rates_and_identity(self):
        buy = fill(Venue.LIMITLESS, "0.4", "100")
        sell = fill(Venue.LIMITLESS, "0.5", "100", side=Side.SELL)
        policies = (
            Policy(),
            Policy(limitless_buy_bps=350),
            Policy(limitless_sell_bps=200),
        )
        identities = set()
        for policy, buy_fee, sell_fee in zip(
            policies, ("3", "3.5", "3"), ("0.75", "0.75", "1")
        ):
            engine = FeeEngine(policy)
            result, _ = self.evaluate(engine, buy)
            self.assertEqual(total_fee(result), Fraction(buy_fee))
            self.assertEqual(result.policy, policy)
            identities.add(result.identity)
            self.assertEqual(
                total_fee(self.evaluate(engine, sell)[0]), Fraction(sell_fee)
            )
        self.assertEqual(len(identities), 3)
        self.assertEqual(len({p.identity for p in policies}), 3)

    def test_member_config_is_only_authority_across_fills(self):
        direct = FeeEngine(Policy(kalshi_member_class=AccountClass.DIRECT))
        ordinary = FeeEngine()
        for old_member in AccountClass:
            f = fill(Venue.KALSHI, "0.01", "1")
            f = replace(f, context=replace(f.context, account_class=old_member))
            self.assertEqual(total_fee(self.evaluate(direct, f)[0]), Fraction("0.0007"))
            self.assertEqual(total_fee(self.evaluate(ordinary, f)[0]), Fraction("0.01"))
            self.assertNotEqual(
                self.evaluate(direct, f)[0].identity,
                self.evaluate(ordinary, f)[0].identity,
            )
        for side, expected in ((Side.BUY, "-0.06"), (Side.SELL, "0.05")):
            f = fill(Venue.KALSHI, "0.055", "1", side=side)
            result, _ = self.evaluate(ordinary, f)
            self.assertEqual(total_fee(result), Fraction("0.005"))
            self.assertEqual(delta(result, f.economics.quote), Fraction(expected))

    def test_rounding_refunds_are_independent_and_configured_state_continues(self):
        f = fill(Venue.KALSHI, "0.01", "1")
        second = replace(f, fill_id="second", fill_index=1, new_order=False)
        resolver = Resolver(Catalog.build([schedule(f)]))
        for refunds in (False, True):
            for account_rebates in (False, True):
                engine = FeeEngine(
                    Policy(
                        include_rounding_refunds=refunds,
                        include_account_rebates=account_rebates,
                    )
                )
                results = engine.assess_many((f, second), resolver)
                self.assertEqual(total_fee(results[0]), Fraction("0.01"))
                self.assertEqual(
                    total_fee(results[1]), Fraction(0) if refunds else Fraction("0.01")
                )
                self.assertEqual(
                    tuple(r.amount.amount.value for r in results[1].rebates),
                    (Fraction("0.01"),) if refunds else (),
                )
                self.assertTrue(all(r.policy == engine.policy for r in results))
        engine = FeeEngine(
            Policy(kalshi_member_class=AccountClass.DIRECT, include_rounding_refunds=True)
        )
        state = None
        for index, old_member in enumerate(AccountClass):
            current = replace(
                f,
                fill_id=str(index),
                fill_index=index,
                new_order=index == 0,
                context=replace(f.context, account_class=old_member),
            )
            result, state = self.evaluate(engine, current, state)
            self.assertFalse(result.unknowns)
            self.assertEqual(total_fee(result), Fraction("0.0007"))
            self.assertEqual(state.grid_scale, 4)
            self.assertEqual(state.carry.value, Fraction(7 * (index + 1), 1000000))

    def test_no_builder_and_unknown_role(self):
        f = fill(role=Role.UNKNOWN)
        f = replace(
            f, context=replace(f.context, builder=Builder(BuilderStatus.UNKNOWN))
        )
        result, _ = self.evaluate(FeeEngine(), f)
        self.assertFalse(result.unknowns)
        self.assertEqual(total_fee(result), Fraction("1.75"))
        self.assertEqual(result.fill, f)

    def test_closed_immutable_config_and_arithmetic_identity(self):
        policy = Policy()
        for field in ("limitless_buy_bps", "limitless_sell_bps"):
            for invalid in (True, 1.5, -1, 10001):
                with (
                    self.subTest(field=field, invalid=invalid),
                    self.assertRaises((TypeError, ValueError)),
                ):
                    replace(policy, **{field: invalid})
            for valid in (0, 10000):
                self.assertEqual(getattr(replace(policy, **{field: valid}), field), valid)
        with self.assertRaises(ValueError):
            Policy(kalshi_member_class=AccountClass.UNKNOWN)
        for field, value in (
            ("kalshi_member_class", AccountClass.DIRECT),
            ("include_rounding_refunds", True),
            ("include_account_rebates", True),
        ):
            self.assertNotEqual(
                policy.identity, replace(policy, **{field: value}).identity
            )
        engine = FeeEngine(policy)
        with self.assertRaises(FrozenInstanceError):
            engine.policy = Policy(limitless_buy_bps=350)
        with self.assertRaises(TypeError):
            FeeEngine(None)
        f = fill()
        resolved = Resolver(Catalog.build([schedule(f)])).resolve(f.context, 20, None)
        with self.assertRaises(TypeError):
            engine.assess(f, resolved, policy=Policy())


class FeeDomainTests(unittest.TestCase):
    def test_checked_rescale_rejects_invalid_scale_before_arithmetic(self):
        from replay.fees.domain import ceil_grid

        for convert in (fixed, ceil_grid):
            for scale in (-1, 19):
                with self.assertRaises(ValueError):
                    convert(Fraction(1, 2), scale)

    def test_no_float_coercion_or_unbounded_exponents(self):
        for value in (0.5, "1e1000000", "NaN", "-1", "0." + "1" * 19):
            with self.assertRaises((ValueError, TypeError)):
                Fixed.parse(value)
        for exponent in (Fraction(1, 2), 0.5, True, -1, 17):
            with self.assertRaises((ValueError, TypeError)):
                Polymarket(Rate.parse("0.07"), exponent, True)
        with self.assertRaises(TypeError):
            Fixed(1.0, 2)
        with self.assertRaises(TypeError):
            Policy(include_account_rebates=1)
        with self.assertRaises(TypeError):
            Policy(include_rounding_refunds=1)

    def test_notional_mismatch_and_distinct_quantity(self):
        f = fill()
        with self.assertRaisesRegex(ValueError, "notional mismatch"):
            replace(f, gross_notional=Notional(f.economics.quote, Fixed(49, 0)))
        with self.assertRaises(TypeError):
            replace(f, quantity=Fixed(100, 0))
        with self.assertRaises(ValueError):
            replace(f, quantity=Quantity(0, 0))

    def test_immutable_deep_closed_and_canonical(self):
        s = schedule(fill())
        with self.assertRaises(FrozenInstanceError):
            s.model.rate = Rate(0, 0)
        with self.assertRaises(TypeError):
            replace(s, sources=[SOURCE])
        self.assertEqual(parse_canonical(canonical(s)), s)
        self.assertNotEqual(s.identity, replace(s, model_version="v2").identity)
        self.assertNotEqual(
            Catalog.build([s]).identity,
            Catalog.build([replace(s, model_version="v2")]).identity,
        )

    def test_asset_identity_and_dimension_checks(self):
        f = fill()
        usd = Asset(AssetKind.USD, "ledger", "USD")
        pusd = Asset(AssetKind.PUSD, "test-chain", "test-usdc")
        self.assertEqual(len({usd, pusd, f.economics.quote, f.economics.outcome}), 4)
        with self.assertRaisesRegex(ValueError, "economics identity"):
            assess(
                f, [replace(schedule(f), economics=replace(f.economics, quote=pusd))]
            )
        e = replace(f.economics, payout=AssetAmount(f.economics.quote, Fixed(2, 0)))
        f = replace(f, economics=e)
        result, _ = assess(f)
        self.assertIsNone(result.net_deltas)
        self.assertEqual(result.unknowns[0].reason, UnknownReason.DIMENSIONS)


class VenueVectorTests(unittest.TestCase):
    def test_kalshi_fractional_quantity_and_microfee_boundary(self):
        for multiplier, expected in (
            ("0.0003999", "0.000007"),
            ("0.0004", "0.000007"),
            ("0.0004001", "0.000008"),
        ):
            f = fill(Venue.KALSHI, "0.5", "1")
            s = schedule(f, Kalshi(KalshiKind.QUADRATIC, Multiplier.parse(multiplier)))
            result, _ = assess(f, [s], EXACT)
            self.assertEqual(result.charges[0].amount.amount.value, Fraction(expected))
        f = fill(Venue.KALSHI, "0.055", "0.1")
        result, _ = assess(f, policy=DIRECT)
        self.assertEqual(result.charges[0].amount.amount.value, Fraction("0.000364"))
        self.assertEqual(total_fee(result), Fraction("0.0004"))
        p, q = QuotePrice.parse("0.00001"), Quantity.parse("0.01")
        f = replace(
            f,
            economics=replace(f.economics, quote_scale=7),
            price=p,
            probability=Probability(p.atoms, p.scale),
            quantity=q,
            gross_notional=Notional(f.economics.quote, Fixed(1, 7)),
        )
        with self.assertRaisesRegex(ValueError, "six decimals"):
            assess(f)

    def test_polymarket_asymmetric_vectors(self):
        for price, exponent, expected in (
            ("0.5", 1, "1.75"),
            ("0.3", 1, "1.47"),
            ("0.3", 2, "0.3087"),
        ):
            f = fill(price=price)
            result, _ = assess(
                f, [schedule(f, Polymarket(Rate.parse("0.07"), exponent, True))]
            )
            self.assertEqual(result.charges[0].amount.amount.value, Fraction(expected))
            self.assertEqual(result.evidence, Evidence.ESTIMATE)
            self.assertEqual(
                delta(result, f.economics.quote),
                -(f.gross_notional.amount.value + Fraction(expected)),
            )

    def test_pm_ceil5_below_at_above_and_split(self):
        for raw, expected in (
            ("0.0000049", "0.00001"),
            ("0.000005", "0.00001"),
            ("0.0000051", "0.00001"),
            ("0.00001", "0.00001"),
            ("0.0000101", "0.00002"),
        ):
            f = fill(quantity="1")
            result, _ = assess(f, [schedule(f, Polymarket(Rate.parse(raw), 0, True))])
            self.assertEqual(total_fee(result), Fraction(expected))
        f = fill(quantity="1")
        s = schedule(f, Polymarket(Rate.parse("0.000015"), 0, True))
        single = assess(fill(quantity="2"), [s])[0]
        split = assess(f, [s])[0]
        self.assertEqual(total_fee(single), Fraction("0.00003"))
        self.assertEqual(total_fee(split) * 2, Fraction("0.00004"))

    def test_maker_zero_is_not_missing_and_builder_metadata_is_ignored(self):
        f = fill(role=Role.MAKER)
        self.assertEqual(total_fee(assess(f)[0]), 0)
        self.assertIsNone(assess(f, [])[0].net_deltas)
        c = replace(f.context, builder=Builder(BuilderStatus.KNOWN, "builder"))
        f = replace(f, context=c)
        s = schedule(
            f,
            BuilderFee(100, 50, Rounding.CEIL),
            component=Component.BUILDER,
            scope=Scope(Venue.POLYMARKET, Product.CLOB, builder="builder"),
            fee_asset=f.economics.quote,
        )
        result, _ = assess(f, [schedule(f), s])
        self.assertEqual(
            [c.component for c in result.charges],
            [Component.PLATFORM],
        )
        self.assertEqual(total_fee(result), 0)
        missing, _ = assess(f)
        self.assertEqual(len(missing.charges), 1)
        self.assertIsNotNone(missing.net_deltas)
        unknown_builder = replace(
            f, context=replace(c, builder=Builder(BuilderStatus.UNKNOWN))
        )
        self.assertIsNotNone(assess(unknown_builder)[0].net_deltas)

    def test_unknown_role_and_no_default_schedule(self):
        f = fill(role=Role.UNKNOWN)
        result, _ = assess(f)
        self.assertIn("unknown_role_assumed_taker", result.assumptions)
        self.assertEqual(total_fee(result), Fraction("1.75"))
        result, _ = assess(f, policy=Policy(assume_unknown_role_taker=False))
        self.assertIsNone(result.net_deltas)
        self.assertIsNone(assess(f, [])[0].net_deltas)

    def test_kalshi_direct_non_direct_and_sell_signed_floor(self):
        f = fill(Venue.KALSHI, "0.01", "1")
        result, _ = assess(f, policy=DIRECT)
        self.assertEqual(result.charges[0].amount.amount.value, Fraction("0.000693"))
        self.assertEqual(total_fee(result), Fraction("0.0007"))
        self.assertEqual(result.evidence, Evidence.EXACT_MODEL)
        f = replace(
            f, context=replace(f.context, account_class=AccountClass.NON_DIRECT)
        )
        self.assertEqual(total_fee(assess(f, policy=EXACT)[0]), Fraction("0.01"))
        f = fill(Venue.KALSHI, "0.055", "1", side=Side.SELL)
        f = replace(
            f, context=replace(f.context, account_class=AccountClass.NON_DIRECT)
        )
        result, _ = assess(f, policy=EXACT)
        self.assertEqual(result.charges[0].amount.amount.value, Fraction("0.003639"))
        self.assertEqual(total_fee(result), Fraction("0.005"))
        self.assertEqual(delta(result, f.economics.quote), Fraction("0.05"))

    def test_kalshi_maker_quadratic_and_zero_balance_rounding(self):
        f = fill(Venue.KALSHI, role=Role.MAKER)
        result, _ = assess(
            f, [schedule(f, Kalshi(KalshiKind.MAKER, Multiplier(1, 0)))], DIRECT
        )
        self.assertEqual(total_fee(result), Fraction("0.4375"))
        f = fill(Venue.KALSHI, "0.055", "1", role=Role.MAKER)
        f = replace(
            f, context=replace(f.context, account_class=AccountClass.NON_DIRECT)
        )
        result, state = assess(f, policy=EXACT)
        self.assertEqual(result.charges[0].amount.amount.value, 0)
        self.assertEqual(total_fee(result), Fraction("0.005"))
        self.assertEqual(state.carry.value, Fraction("0.005"))

    def test_kalshi_capped_carry_can_exceed_grid_and_transition(self):
        f = fill(Venue.KALSHI, "0.054", "1", role=Role.MAKER)
        f = replace(
            f, context=replace(f.context, account_class=AccountClass.NON_DIRECT)
        )
        state = None
        for index in range(3):
            current = replace(
                f, fill_id=str(index), fill_index=index, new_order=index == 0
            )
            result, state = assess(current, policy=EXACT, state=state)
            self.assertEqual(total_fee(result), Fraction("0.006"))
        self.assertEqual(state.carry.value, Fraction("0.018"))
        first = fill(Venue.KALSHI, "0.01", "1")
        first = replace(
            first, context=replace(first.context, account_class=AccountClass.NON_DIRECT)
        )
        _, state = assess(first, policy=EXACT)
        second = replace(
            first, fill_id="second", fill_index=1, new_order=False, role=Role.MAKER
        )
        result, state = assess(second, policy=EXACT, state=state)
        self.assertEqual(state.carry.value, Fraction("0.009307"))
        self.assertEqual(total_fee(result), 0)

    def test_kalshi_split_order_rebate_and_state_rejections(self):
        f = fill(Venue.KALSHI, "0.01", "1")
        f = replace(
            f, context=replace(f.context, account_class=AccountClass.NON_DIRECT)
        )
        first, state = assess(f, policy=EXACT)
        second = replace(f, fill_id="second", fill_index=1, new_order=False)
        result, next_state = assess(second, policy=EXACT, state=state)
        self.assertEqual(total_fee(first) + total_fee(result), Fraction("0.01"))
        self.assertEqual(result.rebates[0].amount.amount.value, Fraction("0.01"))
        self.assertEqual(next_state.carry.value, Fraction("0.008614"))
        for invalid in (
            None,
            replace(state, subaccount="other"),
            replace(state, last_fill_index=2),
            replace(state, grid_scale=4),
            replace(state, policy_identity="different"),
        ):
            result, next_state = assess(second, policy=EXACT, state=invalid)
            self.assertIsNone(result.net_deltas)
            self.assertIsNone(next_state)
        result, _ = assess(second)  # explicit conservative omission needs no carry
        self.assertEqual(total_fee(result), Fraction("0.01"))
        self.assertEqual(result.rebates, ())

    def test_unknown_member_and_unsupported_kalshi(self):
        f = fill(Venue.KALSHI)
        f = replace(f, context=replace(f.context, account_class=AccountClass.UNKNOWN))
        result, _ = assess(f)
        self.assertIsNotNone(result.net_deltas)
        self.assertEqual(result.evidence, Evidence.CONDITIONAL_BOUND)
        for kind in (KalshiKind.FLAT, KalshiKind.COMBO):
            self.assertIsNone(
                assess(f, [schedule(f, Kalshi(kind, Multiplier(1, 0)))])[0].net_deltas
            )

    def test_limitless_native_fee_and_payout_vectors(self):
        f = fill(Venue.LIMITLESS, "0.4", "100")
        result, _ = assess(f)
        self.assertEqual(result.evidence, Evidence.ESTIMATE)
        self.assertEqual(delta(result, f.economics.outcome), 97)
        self.assertEqual(delta(result, f.economics.quote), -40)
        self.assertEqual(result.contract_fee_payout_impact.amount.value, 3)
        f = fill(Venue.LIMITLESS, "0.5", "100", side=Side.SELL)
        result, _ = assess(f)
        self.assertEqual(total_fee(result), Fraction("0.75"))
        self.assertEqual(delta(result, f.economics.quote), Fraction("49.25"))
        self.assertEqual(delta(result, f.economics.outcome), -100)
        self.assertEqual(total_fee(assess(replace(f, role=Role.MAKER))[0]), 0)

    def test_limitless_amm_explicit_basis(self):
        f = fill(Venue.LIMITLESS)
        f = replace(f, context=replace(f.context, product=Product.AMM))
        s = schedule(
            f,
            LimitlessAmm(Rate.parse("0.004"), TradeBasis.NOTIONAL, Rounding.EXACT),
            fee_asset=f.economics.quote,
        )
        self.assertEqual(total_fee(assess(f, [s])[0]), Fraction("0.2"))
        self.assertIsNone(
            assess(f, [replace(s, model=replace(s.model, rounding=Rounding.UNKNOWN))])[
                0
            ].net_deltas
        )

    def test_actual_is_separate_and_consistency_checked(self):
        f = fill(Venue.LIMITLESS)

        def amount(n):
            return AssetAmount(f.economics.outcome, Fixed(n, 0))

        observed = ObservedExecution(
            "observed-id",
            SOURCE.identity,
            (ObservedTotal(amount(100), amount(3), amount(97)),),
        )
        actual = validate_actual(observed)
        self.assertEqual(actual.evidence, Evidence.ACTUAL_OBSERVED)
        self.assertEqual(assess(f)[0].evidence, Evidence.ESTIMATE)
        with self.assertRaises(ValueError):
            validate_actual(
                replace(
                    observed,
                    totals=(ObservedTotal(amount(100), amount(3), amount(98)),),
                )
            )
        with self.assertRaises(TypeError):
            FeeEngine().assess(
                observed,
                Resolver(Catalog.build([])).resolve(f.context, 20, None),
            )


class ResolutionTests(unittest.TestCase):
    def test_half_open_precedence_boundary_and_conflict(self):
        f = fill()
        broad = schedule(f, scope=Scope(Venue.POLYMARKET, Product.CLOB))
        narrow = schedule(
            f,
            Polymarket(Rate.parse("0.05"), 1, True),
            effective_from=20,
            effective_to=30,
        )
        resolver = Resolver(Catalog.build([broad, narrow]))
        self.assertEqual(resolver.resolve(f.context, 19, None).platform.schedule, broad)
        self.assertEqual(
            resolver.resolve(f.context, 20, None).platform.schedule, narrow
        )
        self.assertEqual(resolver.resolve(f.context, 29, None).next_boundary, 30)
        self.assertEqual(resolver.resolve(f.context, 30, None).platform.schedule, broad)
        conflict = replace(narrow, model_version="different")
        r = Resolver(Catalog.build([broad, narrow, conflict])).resolve(
            f.context, 20, None
        )
        self.assertEqual(r.platform.reason, UnknownReason.CONFLICT)

    def test_knowledge_cutoff_and_unknown_history(self):
        f = fill()
        s = schedule(f, sources=(replace(SOURCE, retrieved_at=25),))
        resolver = Resolver(Catalog.build([s]))
        self.assertIsInstance(resolver.resolve(f.context, 20, None).platform, Selection)
        self.assertEqual(
            resolver.resolve(f.context, 20, 20).platform.reason, UnknownReason.MISSING
        )
        with self.assertRaises(ValueError):
            resolver.resolve(f.context, 20, 21)
        s = replace(s, effective_from=None, effective_to=None, effective_evidence=None)
        broad = schedule(f, scope=Scope(Venue.POLYMARKET, Product.CLOB))
        self.assertEqual(
            Resolver(Catalog.build([s, broad]))
            .resolve(f.context, 20, None)
            .platform.reason,
            UnknownReason.HISTORY,
        )

    def test_kalshi_nulls_clear_fields_independently(self):
        f = fill(Venue.KALSHI)
        series = schedule(
            f,
            Kalshi(KalshiKind.MAKER, Multiplier(2, 0)),
            scope=Scope(Venue.KALSHI, Product.CLOB, series="series"),
        )
        for override, expected in (
            (
                Kalshi(None, Multiplier(3, 0)),
                Kalshi(KalshiKind.MAKER, Multiplier(3, 0)),
            ),
            (
                Kalshi(KalshiKind.QUADRATIC, None),
                Kalshi(KalshiKind.QUADRATIC, Multiplier(2, 0)),
            ),
            (Kalshi(None, None), series.model),
        ):
            event = schedule(
                f,
                override,
                scope=Scope(Venue.KALSHI, Product.CLOB, event="event", series="series"),
            )
            r = Resolver(Catalog.build([series, event])).resolve(f.context, 20, None)
            self.assertEqual(r.platform.model, expected)
            self.assertEqual(len(r.platform.dependencies), 2)

    def test_supersedes_and_resolution_identity_binding(self):
        f = fill()
        old = schedule(f)
        new = replace(old, model_version="v2", supersedes=(old.identity,))
        resolver = Resolver(Catalog.build([old, new]))
        r = resolver.resolve(f.context, 20, None)
        self.assertEqual(r.platform.schedule, new)
        with self.assertRaises(ValueError):
            FeeEngine().assess(replace(f, event_time=21), r)

    def test_account_adjustment_explicit_only(self):
        f = fill()
        platform = schedule(f, Polymarket(Rate.parse("0.07"), 1, True, Rounding.EXACT))
        account = schedule(
            f,
            AccountRebate(Rate.parse("0.18"), Rounding.EXACT),
            component=Component.ACCOUNT,
            scope=Scope(
                Venue.POLYMARKET,
                Product.CLOB,
                account="account",
                subaccount="subaccount",
            ),
            fee_asset=f.economics.quote,
        )
        result, _ = assess(f, [platform, account], EXACT)
        self.assertEqual(result.basis, Basis.ACCOUNT_ADJUSTED)
        self.assertEqual(result.rebates[0].amount.amount.value, Fraction("0.315"))
        self.assertEqual(assess(f, [platform, account])[0].rebates, ())
        self.assertIsNone(assess(f, [platform], EXACT)[0].net_deltas)
        for refunds in (False, True):
            for account_rebates in (False, True):
                policy = Policy(
                    include_rounding_refunds=refunds,
                    include_account_rebates=account_rebates,
                )
                result, _ = assess(f, [platform, account], policy)
                self.assertFalse(result.unknowns)
                self.assertEqual(
                    total_fee(result),
                    Fraction("1.435") if account_rebates else Fraction("1.75"),
                )

    def test_instrument_discriminators_and_series_change_under_null(self):
        f = fill()
        yes = schedule(
            f,
            scope=Scope(
                Venue.POLYMARKET,
                Product.CLOB,
                market="market",
                instrument="instrument",
                orientation="yes",
            ),
        )
        no = replace(
            yes, scope=replace(yes.scope, instrument="other", orientation="no")
        )
        r = Resolver(Catalog.build([yes, no])).resolve(f.context, 20, None)
        self.assertEqual(r.platform.schedule, yes)
        f = fill(Venue.KALSHI)
        old = schedule(
            f, scope=Scope(Venue.KALSHI, Product.CLOB, series="series"), effective_to=21
        )
        new = replace(
            old,
            effective_from=21,
            effective_to=100,
            model=Kalshi(KalshiKind.MAKER, Multiplier(3, 0)),
        )
        event = schedule(
            f,
            Kalshi(None, None),
            scope=Scope(Venue.KALSHI, Product.CLOB, event="event", series="series"),
        )
        resolver = Resolver(Catalog.build([old, new, event]))
        self.assertEqual(
            resolver.resolve(f.context, 20, None).platform.model, old.model
        )
        self.assertEqual(
            resolver.resolve(f.context, 21, None).platform.model, new.model
        )


class PropertyTests(unittest.TestCase):
    def test_limitless_one_catalog_supports_both_directions(self):
        buy = fill(Venue.LIMITLESS, "0.4", "100")
        sell = replace(buy, side=Side.SELL)
        s = schedule(buy, fee_asset=buy.economics.quote)
        for f, expected in ((buy, Fraction(3)), (sell, Fraction("0.6"))):
            result, _ = assess(f, [s])
            self.assertIsNotNone(result.net_deltas)
            self.assertEqual(total_fee(result), expected)

    def test_uncertain_fill_cannot_seed_exact_future_state(self):
        f = fill(Venue.KALSHI, role=Role.UNKNOWN)
        _, state = assess(f, policy=EXACT)
        self.assertIsNone(state)

    def test_fee_free_vs_missing_and_conservative_evidence(self):
        f = fill()
        self.assertEqual(
            total_fee(assess(f, [schedule(f, ZeroFee("proven fee-free"))])[0]), 0
        )
        self.assertIsNone(assess(f, [])[0].net_deltas)
        f = fill(Venue.KALSHI)
        result, _ = assess(f)
        self.assertEqual(result.evidence, Evidence.CONDITIONAL_BOUND)

    def test_random_kalshi_against_integer_reference(self):
        rng = random.Random(517)
        for member, grid in (
            (AccountClass.DIRECT, 100),
            (AccountClass.NON_DIRECT, 10000),
        ):
            carry = 0
            state = None
            for index in range(150):
                p = rng.randrange(1, 100)
                q = rng.randrange(1, 300)
                role = rng.choice((Role.TAKER, Role.MAKER))
                side = rng.choice((Side.BUY, Side.SELL))
                f = fill(
                    Venue.KALSHI,
                    f"0.{p:02}",
                    str(q),
                    role=role,
                    side=side,
                    fill_id=str(index),
                    fill_index=index,
                    new_order=index == 0,
                )
                f = replace(f, context=replace(f.context, account_class=member))
                model = Kalshi(KalshiKind.MAKER, Multiplier(1, 0))
                # Independent integer micro-dollar calculation, not ceil_grid.
                numerator = 7 * q * p * (100 - p)
                denominator = 1 if role is Role.TAKER else 4
                trade = (numerator + denominator - 1) // denominator
                revenue = q * p * 10000 * (-1 if side is Side.BUY else 1)
                remainder = (revenue - trade) % grid
                rebate = (
                    min((carry + remainder) // grid, (trade + remainder) // grid) * grid
                )
                carry = carry + remainder - rebate
                result, state = assess(
                    f,
                    [schedule(f, model)],
                    replace(EXACT, kalshi_member_class=member),
                    state,
                )
                self.assertEqual(
                    total_fee(result), Fraction(trade + remainder - rebate, 10**6)
                )
                self.assertEqual(state.carry.value, Fraction(carry, 10**6))
                self.assertGreaterEqual(total_fee(result), 0)

    def test_ordered_many_invalidates_missing_intermediate(self):
        f = fill(Venue.KALSHI)
        fills = tuple(
            replace(
                f, fill_id=str(i), fill_index=i, new_order=i == 0, event_time=20 + i
            )
            for i in range(3)
        )
        resolver = Resolver(
            Catalog.build(
                [schedule(f, effective_to=21), schedule(f, effective_from=22)]
            )
        )
        results = FeeEngine(EXACT).assess_many(fills, resolver)
        self.assertIsNotNone(results[0].net_deltas)
        self.assertIsNone(results[1].net_deltas)
        self.assertIsNone(results[2].net_deltas)
        with self.assertRaises(ValueError):
            FeeEngine(EXACT).assess_many((f, f), resolver)


class ArtifactTests(unittest.TestCase):
    def test_each_fsync_failure_exposes_no_partial_committed_catalog(self):
        from replay.fees import artifacts

        catalog = Catalog.build([schedule(fill())])
        real_fsync = artifacts.os.fsync
        for fail_at in range(1, 12):
            with self.subTest(fail_at=fail_at), tempfile.TemporaryDirectory() as temp:
                calls = 0

                def fail_one(fd, fail_at=fail_at):
                    nonlocal calls
                    calls += 1
                    if calls == fail_at:
                        raise OSError("injected fsync failure")
                    return real_fsync(fd)

                root = Path(temp)
                with (
                    patch.object(artifacts.os, "fsync", fail_one),
                    self.assertRaises(OSError),
                ):
                    build_catalog(root, catalog, {SOURCE.sha256: SOURCE_BYTES})
                directory = root / catalog.identity
                if (directory / "receipt.json").exists():
                    self.assertEqual(load_catalog(directory), catalog)
                else:
                    with self.assertRaises(ValueError):
                        load_catalog(directory)
                self.assertFalse(list(root.rglob(".fee-open-*")))

    def test_immutable_conflict_preserved_and_symlink_rejected(self):
        catalog = Catalog.build([schedule(fill())])
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            directory = build_catalog(root, catalog, {SOURCE.sha256: SOURCE_BYTES})
            source = directory / f"source-{SOURCE.sha256}.blob"
            source.write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "immutable artifact conflict"):
                build_catalog(root, catalog, {SOURCE.sha256: SOURCE_BYTES})
            self.assertEqual(source.read_bytes(), b"tampered")
            source.unlink()
            target = root / "outside"
            target.write_bytes(SOURCE_BYTES)
            source.symlink_to(target)
            with self.assertRaises(ValueError):
                load_catalog(directory)

    def test_roundtrip_idempotence_and_independent_tamper_checks(self):
        catalog = Catalog.build([schedule(fill())])
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            destination = build_catalog(root, catalog, {SOURCE.sha256: SOURCE_BYTES})
            self.assertEqual(load_catalog(destination), catalog)
            self.assertEqual(
                build_catalog(root, catalog, {SOURCE.sha256: SOURCE_BYTES}), destination
            )
            for name in (
                "manifest.json",
                "receipt.json",
                f"schedule-{catalog.schedules[0].identity}.json",
                f"source-{SOURCE.sha256}.blob",
            ):
                path = destination / name
                saved = path.read_bytes()
                path.write_bytes(saved + b" ")
                with self.assertRaises(ValueError):
                    load_catalog(destination)
                path.write_bytes(saved)
            (destination / "receipt.json").unlink()
            with self.assertRaises(ValueError):
                load_catalog(destination)

    def test_strict_unknown_duplicate_noncanonical_and_float_fields(self):
        data = canonical(schedule(fill()))
        for invalid in (
            data.replace(b'"type":"Schedule"', b'"extra":1,"type":"Schedule"'),
            data.replace(b'"fee_scale":5', b'"fee_scale":5,"fee_scale":5'),
            data.replace(b'"fee_scale":5', b'"fee_scale":5.0'),
            data + b" ",
        ):
            with self.assertRaises(ValueError):
                parse_canonical(invalid)

    def test_receipt_last_crash_and_bad_source(self):
        catalog = Catalog.build([schedule(fill())])
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.assertRaises(ValueError):
                build_catalog(root, catalog, {SOURCE.sha256: b"wrong"})
            from replay.fees import artifacts

            original = artifacts._put

            def fail_receipt(path, data):
                if path.name == "receipt.json":
                    raise OSError("injected before receipt")
                original(path, data)

            with (
                patch.object(artifacts, "_put", fail_receipt),
                self.assertRaises(OSError),
            ):
                build_catalog(root, catalog, {SOURCE.sha256: SOURCE_BYTES})
            with self.assertRaises(ValueError):
                load_catalog(root / catalog.identity)
            self.assertEqual(
                load_catalog(
                    build_catalog(root, catalog, {SOURCE.sha256: SOURCE_BYTES})
                ),
                catalog,
            )

    def test_offline_import_and_assessment_boundary(self):
        script = """
import sys
import socket
import unittest.mock
def blocked(*args, **kwargs):
    raise AssertionError("network attempted")
socket.socket = blocked
from replay.fees import FeeEngine
from tests.test_fee_sdk import fill, assess
assess(fill())
for prefix in ("replay.books", "replay.economics", "targeter", "splices", "redis", "engine", "analysis", "archive"):
    assert not any(n == prefix or n.startswith(prefix + ".") for n in sys.modules), prefix
"""
        completed = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, check=False
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
