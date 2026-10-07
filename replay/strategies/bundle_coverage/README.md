# Bundle coverage

Reports exact book, member and bundle availability over a pinned historical
scope. Missing members stay in the denominator. Availability under Risk
policy does not prove vendor completeness.

The algorithm and independent reader live in `strategy.py` and `output.py`.
They use shared snapshot preparation and stream cuts without economic SDK
pricing. [bench.example.json](bench.example.json) runs this factory and reader.

Factory: `replay.strategies.bundle_coverage:build`.
Completed bench reader: `replay.strategies.bundle_coverage:read_completed`.

Configuration: [config.example.json](config.example.json). Full contract: [SPEC.md](SPEC.md).
Tests remain under `replay/tests/`; shared SDK and bench instructions are
linked from the [strategy index](../README.md).
