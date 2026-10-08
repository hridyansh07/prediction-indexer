"""Read either physical table layout for strategy semantic assertions."""
import json


def records(root, name, scope_count):
    path = root / name
    if path.exists():
        return [json.loads(line) for line in path.read_bytes().splitlines()]
    if name.endswith("entities.json"):
        group = name[:-len("entities.json")]
        descriptors = {r["hash"]: r for r in records(root, group + "descriptors.ndjson", scope_count)}
        scopes = [[] for _ in range(scope_count)]
        for row in records(root, group + "entities.ndjson", scope_count):
            assert row["entity"] == len(scopes[row["scope"]])
            scopes[row["scope"]].append(descriptors[row["hash"]])
        return [{"scopes": scopes}]
    if name == "reasons.json":
        rows = records(root, "reasons.ndjson", scope_count)
        assert [r["reason"] for r in rows] == list(range(len(rows)))
        return [{"reasons": [r["value"] for r in rows]}]
    raise FileNotFoundError(path)
