#!/usr/bin/env python3
"""
Construit la base relationnelle DuckDB a partir du Spotify Million Playlist
Dataset (dossier ./data).

    schema.sql   -> structure (tables, cles, index)   [Modele Physique]
    build.sql    -> ETL (lecture JSON, remplissage)

Niveaux de contraintes (voir db/PERFORMANCE.md) :
    (defaut)   PK seules   -- FK retirees ; integrite verifiee apres chargement
    --strict   PK + FK     -- le moteur refuse toute ligne orpheline
    --fast     aucune      -- ni FK ni PK composite (le + rapide / petit)

Usage :
    python db/load_mpd.py                       # tout le dataset -> db/mpd.duckdb (PK seules)
    python db/load_mpd.py -n 50                 # 50 slices seulement (iteration rapide)
    python db/load_mpd.py --strict             # PK + FK imposees par le moteur
    python db/load_mpd.py --fast               # sans aucune contrainte
    python db/load_mpd.py --db /tmp/test.duckdb --data data

Pre-requis : le module python `duckdb`
    python3 -m venv .venv && .venv/bin/pip install duckdb
    (puis lancer  .venv/bin/python db/load_mpd.py)
"""
import argparse
import glob
import os
import re
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(HERE)


def human(n):
    return f"{n:,}".replace(",", " ")


def strip_fk(ddl: str) -> str:
    """Retire les seules cles etrangeres (mode par defaut : PK conservees)."""
    return re.sub(r"\s+REFERENCES\s+\w+\s*\([^)]*\)", "", ddl)


def strip_all(ddl: str) -> str:
    """Retire FK et PK composite (--fast)."""
    ddl = strip_fk(ddl)
    return re.sub(r",\s*\n\s*PRIMARY KEY\s*\([^)]*\)", "", ddl)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=os.path.join(HERE, "mpd.duckdb"), help="fichier base DuckDB (defaut: db/mpd.duckdb)")
    ap.add_argument("--data", default=os.path.join(PROJECT, "data"), help="dossier des mpd.slice.*.json")
    ap.add_argument("-n", "--sample", type=int, default=0, metavar="N", help="ne charger que les N premieres slices")
    ap.add_argument("--schema", default=os.path.join(HERE, "schema.sql"))
    ap.add_argument("--build", default=os.path.join(HERE, "build.sql"))
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--strict", action="store_true", help="PK + FK imposees par le moteur")
    mode.add_argument("--fast", action="store_true", help="aucune contrainte (ni FK ni PK composite)")
    ap.add_argument("--keep", action="store_true", help="ne pas ecraser une base existante")
    args = ap.parse_args()

    try:
        import duckdb
    except ModuleNotFoundError:
        sys.exit(
            "Le module python 'duckdb' est absent.\n"
            "  python3 -m venv .venv && .venv/bin/pip install duckdb\n"
            "  .venv/bin/python db/load_mpd.py"
        )

    files = sorted(glob.glob(os.path.join(args.data, "mpd.slice.*.json")))
    if not files:
        sys.exit(f"Aucun mpd.slice.*.json dans {args.data}")
    if args.sample:
        files = files[: args.sample]

    # Source pour read_json() : glob si tout, liste sinon
    all_files = sorted(glob.glob(os.path.join(args.data, "mpd.slice.*.json")))
    if args.sample and len(files) < len(all_files):
        src = "[" + ", ".join("'" + f.replace("'", "''") + "'" for f in files) + "]"
    else:
        src = "'" + os.path.join(args.data, "mpd.slice.*.json").replace("'", "''") + "'"

    stg_db = args.db + ".stg"
    for p in (args.db, stg_db, stg_db + ".wal"):
        if os.path.exists(p):
            if p == args.db and args.keep:
                sys.exit(f"{args.db} existe deja (option --keep).")
            os.remove(p)

    schema_sql = open(args.schema, encoding="utf-8").read()
    build_sql = (open(args.build, encoding="utf-8").read()
                 .replace("@SRC@", src)
                 .replace("@STG@", stg_db.replace("'", "''")))
    if args.fast:
        schema_sql, mode = strip_all(schema_sql), "aucune (--fast)"
    elif args.strict:
        mode = "PK + FK (--strict)"
    else:
        schema_sql, mode = strip_fk(schema_sql), "PK seules (defaut)"

    print(f"Base      : {args.db}")
    print(f"Slices    : {len(files)} / {len(all_files)}")
    print(f"Contraintes : {mode}")
    print("-" * 60)

    con = duckdb.connect(args.db)
    con.execute("PRAGMA enable_progress_bar")

    t0 = time.perf_counter()
    con.execute(schema_sql)
    print(f"[schema]  ok            ({time.perf_counter()-t0:.1f}s)")

    t1 = time.perf_counter()
    con.execute(build_sql)
    print(f"[build ]  ETL terminee  ({time.perf_counter()-t1:.1f}s)")
    for p in (stg_db, stg_db + ".wal"):
        if os.path.exists(p):
            os.remove(p)

    print("-" * 60)
    tables = ["artist", "album", "track", "playlist", "playlist_track"]
    for tb in tables:
        n = con.execute(f"SELECT count(*) FROM {tb}").fetchone()[0]
        print(f"  {tb:<16} {human(n):>14}")

    # Controles de coherence
    print("-" * 60)
    checks = con.execute(
        """
        SELECT
          (SELECT count(*) FROM playlist WHERE num_tracks <>
              (SELECT count(*) FROM playlist_track pt WHERE pt.pid = playlist.pid)) AS num_tracks_ko,
          (SELECT count(*) FROM playlist_track pt LEFT JOIN track t USING(track_uri) WHERE t.track_uri IS NULL) AS fk_track_ko,
          (SELECT count(*) FROM track t LEFT JOIN artist a USING(artist_uri) WHERE a.artist_uri IS NULL) AS fk_artist_ko,
          (SELECT count(*) FROM track t LEFT JOIN album  al USING(album_uri)  WHERE al.album_uri IS NULL) AS fk_album_ko
        """
    ).fetchone()
    labels = ["playlist.num_tracks == COUNT reel", "playlist_track.track_uri -> track",
              "track.artist_uri -> artist", "track.album_uri -> album"]
    for lab, v in zip(labels, checks):
        print(f"  [{'OK ' if v == 0 else 'KO '}] {lab}" + ("" if v == 0 else f"  ({v} anomalies)"))

    # Clin d'oeil : on retrouve les chiffres Beyonce
    bey = con.execute(
        """
        WITH b AS (SELECT track_uri FROM track WHERE artist_uri = '6vWDO969PvNqNYHIOW5v0m')
        SELECT
          (SELECT count(*) FROM playlist_track WHERE track_uri IN (SELECT track_uri FROM b)) AS occurrences,
          (SELECT count(DISTINCT pid) FROM playlist_track WHERE track_uri IN (SELECT track_uri FROM b)) AS playlists
        """
    ).fetchone()
    print("-" * 60)
    print(f"  Beyonce : {human(bey[0])} occurrences / {human(bey[1])} playlists")

    con.close()
    print("-" * 60)
    print(f"Termine en {time.perf_counter()-t0:.1f}s  ->  {args.db}")


if __name__ == "__main__":
    main()
