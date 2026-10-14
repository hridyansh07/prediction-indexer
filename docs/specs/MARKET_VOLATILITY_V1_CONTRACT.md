# Market volatility V1 implementation contract

The supplied [specification](MARKET_VOLATILITY_V1.md) is preserved byte for byte,
including its pasted `proposed` status. The user authorized its implementation.
The standalone group is implemented; retained-data acceptance remains pending.
These conventions make its implicit row shapes and units precise.

## Policy and units

Only standalone profile policy version 2 admits `volatility`. It requires
`transitions` and exactly one closed `volatility` object with the six fields in
the supplied specification. The object is forbidden when the group is absent.
Milliseconds, version and thresholds are JSON integers, never booleans, floats
or decimal strings. Both lists are nonempty, positive and strictly increasing;
horizons have at most eight entries and thresholds at most six. Milliseconds
must convert to unsigned 64-bit nanoseconds. Horizons need not divide, or be a
multiple of, the sampling period. Existing policy and output identities are
unchanged when the group is absent.

Transitions keep native prices. Volatility converts each observed top price
exactly to scale 4: `native_atoms * 10000 / 10**native_scale`. A nonintegral
result fails the attempt with `volatility scale-4 precision`; V1 never rounds.
This is a visible unsupported precision, not an undefined sample. The supplied
integer scale-4 format cannot carry such a quote exactly.

A book sample is `bid + ask` in scale-4 atoms, twice its mid. A gap sample is
`2 * (ask_a + ask_b)`, twice its combined cost. Both row types use identical
doubled-unit horizon statistics and the jump test `abs(move) >= 2 * jump_atoms`.
Divide RMS by two to obtain scale-4 atoms, or by 20000 to obtain quote currency
per contract. `spread_sum2` can be negative for an observed crossed book.

## Causal time

Scopes and buckets are half-open. Scope-relative samples are
`scope_start + k * sample_ms * 1000000`, strictly before scope end. There is no
sample at the final end. All cuts at a sample or destination instant apply
before it is read. Before a later top row refreshes copied state, earlier sample
and destination instants are caught up. Terminal catches up every scope.
Opening transition rows supply pre-start state and quiet scope entries.

An arbitrary horizon's destination is `s + H`, even off the sample grid. Both
endpoints must be defined inside the same scope; intermediate one-sided time
does not disqualify two defined endpoints. The move belongs to the origin
bucket, including when its destination is in a later bucket. No samples or
pending moves cross scope boundaries. The group declares no extra publisher
retention boundaries or offsets.

`two_sided_ns`, `defined_ns` and `below_unit_ns` integrate exact state intervals,
split at bucket boundaries, including nanosecond remainders. `top_updates`
counts every non-opening top row, including forced snapshot and invalidation
rows with an unchanged top. `mid_changes` counts changes between adjacent
defined mids in those rows; it neither bridges an undefined mid nor counts
quantity-only changes. Every equal-time top row contributes to update counts;
only the final state contributes to samples and durations.

## Closed rows

Book rows contain exactly the fields in the supplied example:
`type`, `scope`, `book`, `bucket`, `start_ns`, `end_ns`, `samples`,
`two_sided_ns`, `mid_changes`, `top_updates`, `spread_sum2`, `horizons`.

Gap rows contain exactly:
`type`, `scope`, `pair`, `bucket`, `start_ns`, `end_ns`, `samples`,
`defined_ns`, `below_unit_ns`, `horizons`.

Every configured horizon is present, including when `n` is zero. Each horizon
contains exactly `n`, `sum_abs2`, `sum_sq4`, `max_abs2`, `jumps`; every threshold
is present in `jumps`. Times, sums and maxima are canonical decimal strings;
indexes and counts are JSON integers. Rows are canonical JSON plus LF, ordered
by `(scope, bucket, type, book_or_pair)` with `book` preceding `gap`. Only buckets
with a defined sample are emitted. Bucket ends are clipped to the scope.

## Pair and unavailable evidence

Pairs use only scope-pinned native masks and an `EXHAUSTIVE` outcome space,
as required by the existing outcome-mask partition contract. Masks must be
`MASKED`, share the space, be disjoint and cover it, and markets must differ.
This is normal-resolution evidence only; settlement compatibility and execution
are not inferred.

Side names are bought native orientations, `outcome` or `complement`. Non-Kalshi
books offer only their written orientation. A written Kalshi book offers its
own ask and the opposite side's ask at `10000 - bid`; that opposite native
orientation must be present in the scope. Normally the written book is Yes
(`outcome`). An exclusively planned No book remains in the existing transitions
table and offers its No ask. Both written books must be usable and both bought
sides present for a gap to be defined.

`gap_pairs` is a global table ordered by scope then canonical
`(book_a, side_a, book_b, side_b)`, with `(book_a, side_a) < (book_b, side_b)`.
Entries contain exactly `scope`, `pair`, `book_a`, `side_a`, `book_b`, `side_b`,
`market_a`, `market_b`, `mask_a`, `mask_b`. `pair` is a dense global index;
each mask is exactly `{space_shape_id, claim_id, negated}`. Pairs are distinct
between scopes.

Unmasked candidate sides appear in `unavailable`, ordered by `(scope, book,
side)`, as exactly `{scope, book, side, status, reason}`. Original statuses and
reasons are preserved, including unknown masks; incomplete spaces report
`INCOMPLETE_COVERAGE`. This avoids guessing unknown pair equivalences or
expanding unavailable candidates quadratically. With `gap_pairs: null` both
tables are empty and mask discovery does no work.

## Summary, verification and limits

`summary.volatility` contains exactly `books`, `gaps`, `gap_pairs`, `unavailable`.
`books` lists `{book, samples, horizons}` for every written book, summing scopes.
`gaps` lists `{pair, samples, horizons}` for every pair. Zero totals are retained.
Maxima combine by maximum; other statistics combine by addition.

The writer observes copied top rows in the transitions replay pass. Its pending
forward destination heap retains only unfinished moves, and buckets flush after
their last horizon can finish. The reader imports no collector or recorder:
it independently rebuilds mask pairs as bitsets, verifies transitions, runs
backward lookups on shifted destination clocks, and compares every complete
row and total. Finalization compares collector totals with reader totals.
Both use shared format and codec helpers, not another Zstandard implementation.

Existing streaming frame commit order and stored/logical identities apply.
Volatility has a 1 MiB LF-inclusive line limit, 50000000 rows and 16 GiB logical
bytes. At most 4096 pairs across scopes, 100000 unfinished bucket/series entries
and 1000000 pending scalar sample values are admitted. Tables, totals, buckets
and samples charge the 512 MiB state budget; conservative shape costs include
all horizon and threshold maps. Exceeding a limit fails visibly. State stops
growing with elapsed run time once buckets and horizon history have expired.

The five-bundle profile run and per-game/per-market 1 s/3 s RMS/jump report need
the retained sample. Comparison with delayed-fill leg losses also needs the
separate execution-delay result. Neither input was available in this worktree.
No execution-realism strategy or live execution engine is implemented here.
