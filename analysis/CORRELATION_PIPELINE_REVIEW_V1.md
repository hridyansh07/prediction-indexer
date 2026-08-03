# Review — Correlation Candidate Pipeline v1 (EWC Dota Playoffs)

Issues ordered by how much they change the decision, not by how easy they are to fix.
Each entry: what the problem is, the mechanism that causes it, and the change.

**What's right first, because it's most of the spec.** Hash-addressed runs with
input/output hashes per stage; immutable raw HTTP artifacts and zero-request reruns;
leave-pair-events-out factor construction; no-lookahead resampling with an explicit quote
staleness rejection; controls scored blind and labels joined afterward; explicit
classification of every target as complete / confirmed_zero / partial / error with only the
first two passing the gate. That is a well-built research harness. Everything below is
about what it's pointed at and how the statistics behave at this sample size.

---

## 1. The dataset likely contains no logically-implied relationships — CRITICAL

**Problem.** 28 outcome series across 8 games is ~3.5 per game, which is what you get from
two outcomes × two venues. If that's what it is, you have series moneyline only: no map
winners, no correct score, no over/under maps.

**Why it matters.** The product thesis is the graph of markets that are the *same bet
without being the same contract*. With moneyline only, the sole real relationship present
is cross-venue same-outcome identity — which is price comparison, and which Oddpool
already ships as a commodity. The novel-candidate leaderboards would then be searching
for cross-event relationships between logically independent matches, where any signal
found is a confounder by construction (see §8).

**Change.** Before writing another line: group the 28 series by game and count distinct
market *types*. If the answer is "moneyline, both venues," this dataset can validate a
matcher and cannot touch the graph. Keep it as a plumbing test and get the real signal from
a dataset with sibling coverage.

**Cost of not fixing:** you build four leaderboards, two of which cannot contain a true
positive.

---

## 2. Effective sample size is ~30 bars — CRITICAL

**Problem.** A Bo3 Dota match spans a few hours of market life. At 300s bars that is
roughly 30–50 observations, before the eligibility filters.

**Why it matters.** Spearman's standard error at n=30 is roughly 0.18. Your ranking metric
is `|factor-adjusted Spearman|` percentile. At that SE, the difference between rank 3 and
rank 30 on a leaderboard is inside the noise band. You will get a stable-looking ordered
list that does not reproduce on the next tournament, and there is no way to tell that from
the output alone.

**Change.** Make 60s the default for this dataset rather than the sensitivity run — it is
the only lever that moves n materially. Then quantify what survives the filters *before*
scoring: emit a histogram of usable bars per pair after the `0.05 < p < 0.95` bound, the
30-minute terminal exclusion, and the consecutive-bar requirement. If the median pair has
under ~100 usable bars, report that as a hard gate failure rather than proceeding.

---

## 3. The block permutation is degenerate at this series length — CONCRETE BUG

**Problem.** 12-bar blocks on a ~36-bar series gives 3 blocks.

**Why it matters.** With `b` blocks there are at most `b!` distinct permutations. At b=3
that is 6. Your 1,000 permutation draws are overwhelmingly duplicates, and the empirical
p-value has a hard floor near 1/6 ≈ 0.17. No pair can achieve q ≤ 0.05 under BH, so the
exploratory badge can never fire, and the significance machinery silently does nothing.

**Change.** Block length must be small relative to series length — a common rule of thumb
is on the order of `n^(1/3)`, so 3–4 bars at n=36, or keep 12-bar blocks only after moving
to 60s bars where n is 4–5x larger. Add an assertion: `n_bars / block_len >= 10`, fail the
stage otherwise. That assertion would have caught this before the run.

---

## 4. Jump detection estimates 3σ from 12 observations

**Problem.** `|Δlogit| > 3 × rolling_vol` with a 12-bar rolling window.

**Why it matters.** The standard error of a volatility estimate from 12 points is roughly
`σ/√(2×12)` ≈ 20% of σ. So your 3σ threshold is itself wandering by ±0.6σ, and the first
12 bars are burn-in you don't have to spare. Most detected "jumps" will be volatility
estimation error, and combined with the ≥5-source-jump floor, most pairs will return null
on all jump components — which then routes them into the imputation problem in §5.

**Change.** At 60s bars use a longer window (48–60 bars) so the threshold is stable, and
report the realised jump rate per instrument. If the realised rate is far from what a 3σ
Gaussian threshold implies, the returns are fat-tailed enough that a fixed multiplier is
the wrong detector and you want a quantile-based threshold instead.

---

## 5. Neutral 0.5 imputation biases the ranking toward sparse pairs

**Problem.** Missing jump components receive a 0.5 percentile, and the score is the
equal-weight mean of four percentiles.

**Why it matters.** A pair with no jump data scores on two real components and receives
median credit on the other two. A dense pair that genuinely scored badly on all four
receives four low percentiles. The sparse pair can therefore outrank the dense one *because
it has less data*, which is backwards. Disclosing coverage in a field does not remove the
bias from the ranking itself.

**Change.** Either rank within coverage strata (separate leaderboards for pairs with and
without jump components), or require all four components and drop pairs that can't produce
them. Do not impute into a ranking metric.

---

## 6. Within-event complements are not controls

**Problem.** They are listed as a control class alongside cross-venue same-outcome pairs.

**Why it matters.** The two complementary outcomes of one binary market have midpoints
summing to ~1 by construction, so their logit returns are mechanically near-perfectly
anti-correlated. They will rank first on every run regardless of whether the pipeline works.
A control that cannot fail is a smoke test, and reporting it alongside real controls
inflates apparent recall.

**Change.** Reclassify them as a smoke test with a hard assertion (`|ρ| > 0.95`, fail the
stage otherwise). Report control recall using only cross-venue same-outcome and
cross-venue opposing-outcome pairs.

---

## 7. Control recall is explicitly non-gating — this is the one I'd change first

**Problem.** "Report top-decile control recall for information only; it does not gate this
first exploratory run."

**Why it matters.** This is the only falsification in the entire plan. Everything else in
the spec is reproducibility machinery — it guarantees you get the *same* answer twice, not
that the answer means anything. Control recall is the single check that distinguishes "the
pipeline found structure" from "the pipeline ranked noise consistently." Making it
non-gating means a failing run still produces four leaderboards, a report, and plots, and
you will spend the following week interpreting them.

**Change.** Make it a hard gate. If cross-venue same-outcome pairs — which are *known*
identical bets — do not land in the top decile, stop and fix the pipeline. Set the
threshold now, in the config, before the first run.

---

## 8. No control for shared market makers

**Problem.** The leave-pair-out PC1 residualisation handles common news arrival. It does
not handle shared liquidity provision.

**Why it matters.** A common factor absorbs the component that is common to *all* series.
Shared-maker effects are **pairwise** — one desk quoting two specific matches will move
those two books together through inventory management, and PC1 cannot remove a pairwise
effect. In thin esports books this is likely the *dominant* source of cross-event
co-movement, which means your novel-candidate leaderboard is at high risk of ranking
"markets quoted by the same bot" as though it were logical relatedness.

**Change.** Polymarket fills settle on-chain with maker addresses exposed, so this is
observable rather than inferential. Compute notional-weighted maker overlap per pair and
add it as a covariate — the finding you want is co-movement that *survives* conditioning on
it. At minimum, report maker overlap alongside every novel candidate so a human can see it
before believing the rank.

---

## 9. Nothing in the pipeline measures money

**Problem.** Every output is a correlation, a rank, or a p-value. Nothing touches depth,
fills, fees, or basket cost.

**Why it matters.** A pair can be genuinely, strongly, causally related and carry zero
tradeable edge if the books never diverge by more than fees at a real ticket size. The
pipeline as specified cannot distinguish "important relationship" from "important
relationship with no money in it," so a passing run does not advance the decision about
whether to build the product.

**Change.** Run `PARTITION_SUM_TEST_SPEC.md` first. It reuses acquire and normalize, adds
a VWAP walker and a size sweep, and answers the economic question directly on the data you
already have. If it fails on EWC, re-run on the World Cup and Kalshi Fed buckets before
concluding anything.

---

## 10. Quantify the exclusion filters before trusting them

**Problem.** `0.05 < p < 0.95` plus a 30-minute terminal exclusion, on esports matches.

**Why it matters.** Dota matches are frequently lopsided, and once a series is 1-0 with a
strong favourite the price sits outside the bound for most of its remaining life. The
30-minute terminal window may also be a large fraction of a short-lived market. Between
them these filters could remove the majority of observations in exactly the matches that
have the most price action.

**Change.** Emit retention counts per filter per instrument as a validation artifact:
raw bars → after probability bound → after terminal exclusion → after consecutive-bar
requirement. If any single filter is removing more than half, revisit it rather than
accepting the survivors.

---

## 11. Percentile scores are not comparable across leaderboards

**Problem.** Score = mean of *within-leaderboard* percentile ranks, then four leaderboards
are reported side by side.

**Why it matters.** A 0.9 percentile on a leaderboard of 12 pairs and a 0.9 on a
leaderboard of 300 mean very different things, and a reader comparing the top-20 tables
across sections will implicitly compare them. It also means a leaderboard with no true
positives still produces a confident-looking top 20.

**Change.** Report the raw component values alongside the percentile score in every table,
and print the leaderboard size in the header. Consider absolute thresholds rather than
percentiles for the "is this worth a human look" decision.

---

## 12. The four ranking components are correlated with each other

**Problem.** Factor-adjusted Spearman, best lagged correlation, co-jump lift, and beta
stability are combined by equal weight.

**Why it matters.** Best-lagged-correlation and Spearman measure nearly the same thing on
these series, so the equal-weight mean effectively double-weights linear co-movement and
under-weights the jump-conditioned components — which are the ones that actually
distinguish structural links from a shared news clock. The weighting is a modelling choice
presented as a neutral default.

**Change.** Report the components separately and rank on each, at least for v1. If you want
a composite, justify the weights against the control set rather than defaulting to equal.

---

## Suggested sequence

1. Count market types per game (§1). **Ten minutes. May end the rest of this list.**
2. Verify ladder completeness on a raw snapshot.
3. Build and run the partition sum test (§9). **This is the economic gate.**
4. Emit filter retention counts (§10) and usable-bar histograms (§2).
5. Fix the block length assertion (§3) and the imputation (§5).
6. Make control recall a gate (§7).
7. Then run the correlation pipeline — preferably on the World Cup, where n supports it.