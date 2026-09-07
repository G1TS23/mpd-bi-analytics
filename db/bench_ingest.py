#!/usr/bin/env python3
"""
Banc d'essai de l'ingestion MPD -> DuckDB, instrumente par etape.
Sert a mesurer / comparer des variantes (voir db/PERFORMANCE.md).

    python db/bench_ingest.py -n 200 --tag baseline
    python db/bench_ingest.py -n 200 --fast --tag fast
    python db/bench_ingest.py -n 200 --fast --no-stg-item --tag fast_novue
    python db/bench_ingest.py -n 200 --no-preserve-order --threads 6

Options :
    -n N              ne traiter que les N premieres slices (0 = tout)
    --fast            schema sans FK ni PRIMARY KEY composite
    --no-stg-item     _stg_item en sous-requete au lieu d'une table materialisee
    --threads K       PRAGMA threads=K
    --memory '8GB'    PRAGMA memory_limit
    --no-preserve-order  PRAGMA preserve_insertion_order=false
"""
import argparse
import glob
import os
import re
import resource
import time

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(HERE)
SCHEMA = open(os.path.join(HERE, "schema.sql"), encoding="utf-8").read()


def strip_constraints(ddl):
    ddl = re.sub(r"\s+REFERENCES\s+\w+\s*\([^)]*\)", "", ddl)
    ddl = re.sub(r",\s*\n\s*PRIMARY KEY\s*\([^)]*\)", "", ddl)
    return ddl


def rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)


ITEM_UNNEST = "SELECT pid, unnest(tracks, recursive := true) FROM _stg_playlist"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(PROJECT, "data"))
    ap.add_argument("--db", default="/tmp/bench_ingest.duckdb")
    ap.add_argument("-n", type=int, default=0)
    ap.add_argument("--fast", action="store_true")
    ap.add_argument("--threads", type=int, default=0)
    ap.add_argument("--memory", default="")
    ap.add_argument("--no-stg-item", action="store_true")
    ap.add_argument("--no-preserve-order", action="store_true")
    ap.add_argument("--tag", default="run")
    a = ap.parse_args()

    files = sorted(glob.glob(os.path.join(a.data, "mpd.slice.*.json")))
    total_slices = len(files)
    if a.n:
        files = files[: a.n]
    src = (
        "['" + "', '".join(files) + "']"
        if a.n and len(files) < total_slices
        else "'" + os.path.join(a.data, "mpd.slice.*.json") + "'"
    )
    bytes_in = sum(os.path.getsize(f) for f in files)

    if os.path.exists(a.db):
        os.remove(a.db)
    con = duckdb.connect(a.db)
    tags = []
    if a.threads:
        con.execute(f"PRAGMA threads={a.threads}")
        tags.append(f"threads={a.threads}")
    if a.memory:
        con.execute(f"PRAGMA memory_limit='{a.memory}'")
        tags.append(f"mem={a.memory}")
    if a.no_preserve_order:
        con.execute("PRAGMA preserve_insertion_order=false")
        tags.append("preserve_order=off")
    thr = con.execute("SELECT current_setting('threads')").fetchone()[0]
    mem = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]

    schema = strip_constraints(SCHEMA) if a.fast else SCHEMA
    item = f"({ITEM_UNNEST})" if a.no_stg_item else "_stg_item"

    steps = [
        ("schema", schema),
        (
            "stg_playlist",
            f"CREATE OR REPLACE TABLE _stg_playlist AS SELECT pl.* FROM ("
            f"SELECT unnest(playlists) AS pl FROM read_json({src}, "
            f"maximum_object_size=200000000, sample_size=-1))",
        ),
    ]
    if not a.no_stg_item:
        steps.append(("stg_item", f"CREATE OR REPLACE TABLE _stg_item AS {ITEM_UNNEST}"))
    steps += [
        ("insert_artist", f"INSERT INTO artist SELECT DISTINCT split_part(artist_uri,':',3), artist_name FROM {item}"),
        ("insert_album", f"INSERT INTO album SELECT DISTINCT split_part(album_uri,':',3), album_name FROM {item}"),
        ("insert_track", f"INSERT INTO track SELECT DISTINCT split_part(track_uri,':',3), track_name, duration_ms, "
                         f"split_part(artist_uri,':',3), split_part(album_uri,':',3) FROM {item}"),
        ("insert_playlist", "INSERT INTO playlist SELECT pid, name, collaborative='true', "
                            "epoch_ms(modified_at*1000)::DATE, nullif(trim(description),''), num_followers, num_edits, "
                            "num_tracks, num_albums, num_artists, duration_ms FROM _stg_playlist"),
        ("insert_playlist_track", f"INSERT INTO playlist_track SELECT pid, pos, split_part(track_uri,':',3) FROM {item}"),
        ("create_indexes", "CREATE INDEX ix_track_artist_uri ON track(artist_uri); "
                           "CREATE INDEX ix_track_album_uri ON track(album_uri); "
                           "CREATE INDEX ix_playlist_track_track_uri ON playlist_track(track_uri)"),
        ("drop_staging", "DROP TABLE IF EXISTS _stg_item; DROP TABLE _stg_playlist"),
    ]

    print(f"\n=== {a.tag} | duckdb {duckdb.__version__} | {len(files)}/{total_slices} slices "
          f"| {bytes_in/1e9:.1f} Go JSON | threads={thr} mem={mem} "
          f"{' '.join(tags)} | fast={a.fast} no_stg_item={a.no_stg_item} ===")
    print(f"{'etape':<22}{'s':>9}{'cumul':>9}{'db Mo':>10}{'+Mo':>8}")
    t0 = time.perf_counter()
    prev = 0.0
    for label, sql in steps:
        s = time.perf_counter()
        con.execute(sql)
        con.execute("CHECKPOINT")
        sz = os.path.getsize(a.db) / 1e6
        print(f"{label:<22}{time.perf_counter()-s:>9.1f}{time.perf_counter()-t0:>9.1f}{sz:>10.0f}{sz-prev:>8.0f}")
        prev = sz
    total = time.perf_counter() - t0

    rows = {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            for t in ["artist", "album", "track", "playlist", "playlist_track"]}
    con.close()
    final = os.path.getsize(a.db) / 1e6
    print("-" * 58)
    print(f"TOTAL {total:.1f}s | {bytes_in/1e6/total:.0f} Mo/s JSON | "
          f"{rows['playlist_track']/total/1000:.0f}k lignes assoc/s")
    print(f"base {final:.0f} Mo ({final*1e6/bytes_in*100:.0f}% du JSON) | RSS max {rss_mb():.0f} Mo")
    print("rows:", rows)


if __name__ == "__main__":
    main()
