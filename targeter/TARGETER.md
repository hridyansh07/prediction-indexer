# Legacy Targeter v1 — deciding what to watch

> This document is retained as the v1 crypto target writer and splice-file
> protocol. The current sports-first motivation and operator entry point is
> [`README.md`](README.md). Targeter v2 discovery, archive, publication, and
> audit are implemented but remain an explicit deployment opt-in until shadow
> monitoring produces useful event selections.

A separate process from every splice, on purpose. The splice must keep recording
while this is being edited, rerun, or broken. A targeter that could take capture
down with it would make every later coverage claim conditional on this script's
uptime.

## The interface is a file

```json
{"version": 2,
 "venue": "polymarket",
 "note": "generated 2026-07-28T19:45:00+00:00 tag=crypto",
 "digest": "3325a5c1313d42d15a0dc75e716224f3",
 "metadata_digest": "7904e81c…",
 "metadata_path": "metadata/polymarket/7904e81c….json",
 "targets": [{"asset_id": "1029797…", "market_id": "5218",
              "condition_id": "0xb5c0…", "note": "btc-above-100k#0",
              "resolution": {"version": 1, "venue": "polymarket",
                "catalogue_record_hash": "d0b4…",
                "catalogue_record": {"description": "…"}}}]}
```

The targeter writes it atomically; the splice polls it. Neither has to be up for
the other to be useful, and the subscription set is a thing you can read, diff, and
commit to git after the fact.

A control socket would buy nothing here and would add a second live dependency
between two processes that currently share only a filesystem.

`asset_id` holds whatever the venue's socket subscribes by — a CLOB token id on
Polymarket, a market slug on Limitless. The splice takes the identifiers it is
given; deciding what counts as one market is the targeter's job.

## The digest

Identity of a subscription set, over `(venue, sorted set of asset_ids)` only.

Reordering the file or editing a note must **not** move it — a digest that moved on
those would force a reconnect, and therefore a book resync, for an edit that
changed nothing.

When it does move, the splice writes a `subscription_changed` record carrying the
before/after digest and the added/removed assets, *then* reconnects with the new
set. Without that record, a market with no data is indistinguishable from a market
that was never subscribed, and there is no way to recover the difference later.

## Raw resolution evidence

The subscription digest deliberately excludes annotations and catalogue metadata.
Each selected target also carries the complete JSON-decoded venue catalogue record
and its canonical SHA-256 under `resolution`; this is raw evidence, not a claim
that the settlement oracle or observation method has already been interpreted.

A separate `metadata_digest` covers those records. Before replacing the live
targets file, the targeter writes the corresponding content-addressed snapshot
under `metadata/<venue>/<metadata_digest>.json`. A rules/catalogue change with the
same asset ids therefore leaves the socket connected, but the splice writes
`target_metadata_changed` with the old/new metadata digests and snapshot path.
`connection_opened` carries the initial metadata digest and path as well.

## Rejections are data

`--rejected-out` records every candidate considered and dropped, with the reason.
Without it a selector bug looks exactly like a venue not listing the market, and
the two have very different fixes.

## Refusals

An empty target set is legal — it means "watch nothing yet" and lets the targeter
run before it has decided anything. A duplicate or blank `asset_id` is not: the
socket would silently collapse them and the coverage record would then overstate
what was actually subscribed.

## Discovery cadence

For short-dated markets this matters more than what gets selected. A five-minute
Limitless market discovered a minute late has lost a fifth of its life, and the
missing fifth is the opening price-discovery window.

Both venues push new-market notifications the splices already record —
Polymarket's `NewMarketEvent`, Limitless's `marketCreated`. That tape is the route
to subscribing from inception rather than from the next poll; until it is wired up,
`coverage-from-inception` is a number we cannot yet report and should not assume.

## Implementations

| File | Venue | Selects |
|---|---|---|
| `targeter/polymarket.py` | Polymarket | Gamma `/markets`, by tag, slug substring, min liquidity |
| `targeter/limitless.py` | Limitless | `/markets/active`, by horizon (5min/15min/hourly/daily) and underlying; CLOB only |
| `targeter/kalshi.py` | Kalshi | `/markets?series_ticker=…`, whole strike ladders from the crypto series |

Limitless's API answers 400 above `limit=25`, so paging is the only way to widen
the search. AMM markets are dropped: they have no order book, so `orderbookUpdate`
never arrives and they would sit in the subscription looking like a market that had
simply gone quiet.

### Ladders are selected whole, never in part

The Kalshi targeter needs no credentials — the market catalogue is public — so it
runs and is verifiable today even though the splice it feeds is waiting on a key.

A Kalshi event such as `KXBTCD-26JUL2904` holds every strike for one settlement
hour, and the P0 experiment is a monotonicity test across that chain: P(BTC >
72,299) must never exceed P(BTC > 71,299). Half a ladder cannot answer that, so
`--max-targets` truncates at an **event boundary** and drops the partial rather
than subscribing to a fragment. The rejection log says which ladders were dropped
and why, so a thin result is never mistaken for a thin market.

`event_ticker` and the strike travel into the targets file so the analysis layer
reconstructs the implication chain without re-querying.

Measured live: `KXBTCD` 188 strikes (53,599.99–72,299.99), `KXETHD` 300,
`KXSOLD` 300 — three complete hourly ladders, 788 markets.
