"""Exact temporal integrals and opening-state raw tiles, with a disposable disk spool."""
import json
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path

from replay.research.io import encoded, need, rows, write, write_chunks
from replay.research.layout import layout

SECOND = 10**9
TILE = 600 * SECOND


def bucket_series(intervals, trades, start, end, width, book_count):
    count = (end - start + width - 1) // width
    need(count * book_count <= 65536, "chart cell bound")
    cells = [[[None, None, 0, 0, 0, -1] for _ in range(count)] for _ in range(book_count)]
    for book, bid, ask, a, b, validity in intervals:
        a, b = max(start, a), min(end, b)
        if b <= a:
            continue
        for i in range((a - start) // width, (b - 1 - start) // width + 1):
            cell = cells[book][i]
            left, right = max(a, start + i * width), min(b, end, start + (i + 1) * width)
            if validity == "usable":
                cell[2] += right - left
            for j, value in enumerate((bid, ask)):
                value = value if validity == "usable" else None
                if value is not None:
                    price = int(value)
                    old = cell[j]
                    cell[j] = [price, price, price] if old is None else [min(old[0], price), max(old[1], price), price]
                elif cell[j] is not None:
                    cell[j][2] = None
            cell[5] = right
    for book, t, quantity in trades:
        if start <= t < end:
            cell = cells[book][(t - start) // width]
            cell[3] += 1
            cell[4] += quantity
    result = []
    for book, values in enumerate(cells):
        buckets = []
        for i, (bid, ask, usable, n, qty, last_until) in enumerate(values):
            stop = min(end, start + (i + 1) * width)
            quotes = []
            for quote in (bid, ask):
                if not usable or quote is None:
                    quotes.append(None)
                else:
                    quote[2] = quote[2] if last_until == stop else None
                    quotes.append([None if v is None else str(v) for v in quote])
            buckets.append({"bid": quotes[0], "ask": quotes[1],
                            "usable_ppm": usable * 10**6 // (stop - start - i * width),
                            "trades": n, "trade_qty": str(qty)})
        result.append({"book": book, "buckets": buckets})
    return {"start_ns": str(start), "end_ns": str(end), "bucket_ns": str(width), "books": result}


def _raw_chunks(db, a, b, opening, scopes):
    # Sorted JSON keys: rows, scopes, state_at_start. Exact upstream row objects.
    yield b'{"rows":['
    first = True
    for (payload,) in db.execute("SELECT row FROM raw WHERE t>=? AND t<? ORDER BY pos", (a, b)):
        if not first:
            yield b","
        yield payload.encode()
        first = False
    yield b'],"scopes":' + encoded(scopes).rstrip(b"\n")
    yield b',"state_at_start":' + encoded(opening).rstrip(b"\n") + b"}\n"


def build_charts(destination, source, snapshot, transitions):
    root = Path(destination)
    root.mkdir(parents=True, exist_ok=True)
    books, memberships, _ = layout(snapshot)
    start = int(snapshot["scopes"][0]["start_ns"])
    end = int(snapshot["scopes"][-1]["end_ns"])
    need(0 <= start < end <= 2**63 - 1 and end - start <= 7 * 86400 * SECOND, "V1 capture duration bound")
    need(len(books) <= 64 and len(snapshot["scopes"]) <= 128, "pack book/scope bound")
    identities = {}
    with tempfile.TemporaryDirectory(prefix="research-spool-", dir=root) as temporary:
        with closing(sqlite3.connect(Path(temporary) / "rows.sqlite3")) as db:
            db.executescript("""PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA cache_size=-4096;
                PRAGMA mmap_size=0; PRAGMA temp_store=FILE;
                CREATE TABLE raw(pos INTEGER PRIMARY KEY,t INTEGER NOT NULL,row TEXT NOT NULL);
                CREATE INDEX raw_time ON raw(t);
                CREATE TABLE spans(book INTEGER,a INTEGER,b INTEGER,bid TEXT,ask TEXT,validity TEXT,row TEXT);
                CREATE INDEX span_time ON spans(a,b);
                CREATE TABLE trades(book INTEGER,t INTEGER,qty TEXT);
                CREATE INDEX trade_time ON trades(t);""")
            held, stored_bytes = {}, 0
            def span(row, stop):
                a = int(row["t_ns"])
                if stop > a:
                    db.execute("INSERT INTO spans VALUES(?,?,?,?,?,?,?)", (row["book"], a, stop,
                               None if row["bid"] is None else row["bid"][0],
                               None if row["ask"] is None else row["ask"][0], row["validity"],
                               encoded(row).rstrip(b"\n").decode()))
            for pos, row in enumerate(rows(source, "transitions.ndjson.zst", transitions)):
                t, key = int(row["t_ns"]), (row["scope"], row["book"])
                payload = encoded(row).rstrip(b"\n")
                stored_bytes += 2 * len(payload) + 128
                need(stored_bytes <= 4 * 1024**3, "chart spool byte bound")
                db.execute("INSERT INTO raw VALUES(?,?,?)", (pos, t, payload.decode()))
                if row["type"] == "top":
                    if key in held:
                        span(held[key], t)
                    held[key] = row
                else:
                    db.execute("INSERT INTO trades VALUES(?,?,?)", (row["book"], t, row["qty"]))
            for (scope, _), row in held.items():
                span(row, int(snapshot["scopes"][scope]["end_ns"]))
            db.commit()
            need((Path(temporary) / "rows.sqlite3").stat().st_size <= 4 * 1024**3, "chart spool byte bound")
            def buckets(a, b, width):
                spans = ((book, bid, ask, left, right, validity) for book, left, right, bid, ask, validity in
                         db.execute("SELECT book,a,b,bid,ask,validity FROM spans WHERE a<? AND b>? ORDER BY a,book", (b, a)))
                trades = ((book, t, int(qty)) for book, t, qty in db.execute(
                    "SELECT book,t,qty FROM trades WHERE t>=? AND t<? ORDER BY t,book", (a, b)))
                return bucket_series(spans, trades, a, b, width, len(books))
            # <=2048 buckets, additionally keep a 30-book overview inside the first-paint budget.
            count = min(2048, max(1, 1800000 // (max(1, len(books)) * 160)))
            width = (end - start + count - 1) // count
            identities["overview.json"] = write(root / "overview.json", buckets(start, end, width))
            for a in range(start, end, TILE):
                b = min(end, a + TILE)
                scopes = [{"scope": s, "books": sorted(memberships[s]), "start_ns": scope["start_ns"], "end_ns": scope["end_ns"]}
                          for s, scope in enumerate(snapshot["scopes"])
                          if memberships[s] and int(scope["start_ns"]) < b and int(scope["end_ns"]) > a]
                if not scopes:
                    continue
                tile = str((a - start) // (60 * SECOND))
                name = "tiles/1s/" + tile + ".json"
                identities[name] = write(root / name, buckets(a, b, SECOND))
                opening = [json.loads(payload) for (payload,) in db.execute(
                    "SELECT row FROM spans WHERE a<? AND b>? ORDER BY book", (a, a))]
                name = "tiles/raw/" + tile + ".json"
                identities[name] = write_chunks(root / name, _raw_chunks(db, a, b, opening, scopes))
    return identities
