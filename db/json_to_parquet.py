#!/usr/bin/env python3
"""
Convertit les sources JSON (data/mpd.slice.*.json) en dataset Parquet,
le plus vite possible, via DuckDB.

Deux dispositions possibles (le banc `--bench` mesure laquelle est la + rapide) :

  two  (defaut)  ->  playlists.parquet        1 ligne / playlist  (sans les pistes)
                     playlist_tracks.parquet  1 ligne / (playlist, position)
  one            ->  mpd_flat.parquet         1 ligne / (playlist, position),
                                              colonnes playlist repetees

Le JSON n'est parse qu'UNE fois (table temporaire `_sp`), les fichiers Parquet
sont ensuite ecrits par `COPY ... TO`.

Usage :
    python db/json_to_parquet.py                     # -> db/parquet/ (disposition 'two')
    python db/json_to_parquet.py --layout one
    python db/json_to_parquet.py --bench -n 200      # compare les dispositions/options
    python db/json_to_parquet.py --out /tmp/pq --compression zstd --per-thread-output
"""
import argparse
import glob
import os
import time

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(HERE)

# colonnes playlist (sans 'tracks'), renommage de name pour eviter toute ambiguite
PL_COLS = ("pid, name AS playlist_name, collaborative, modified_at, description, "
           "num_followers, num_edits, num_tracks, num_albums, num_artists, "
           "duration_ms AS playlist_duration_ms")


def sizeof(path):
    if os.path.isdir(path):
        return sum(os.path.getsize(os.path.join(path, f)) for f in os.listdir(path))
    return os.path.getsize(path)


def fmt(n):
    return f"{n/1e6:.0f} Mo" if n < 1e9 else f"{n/1e9:.2f} Go"


def copy_to(con, select_sql, target, compression, per_thread):
    opts = [f"FORMAT parquet", f"COMPRESSION {compression}"]
    if per_thread:
        opts.append("PER_THREAD_OUTPUT true")
        os.makedirs(target, exist_ok=True)
    con.execute(f"COPY ({select_sql}) TO '{target}' ({', '.join(opts)})")


def build(con, layout, out, compression, per_thread, ext):
    # L'unnest recursif d'une colonne STRUCT[] est ~5x plus rapide materialise
    # dans une table que streame directement vers le writer Parquet.
    made = []
    if layout in ("two", "both"):
        p_pl = os.path.join(out, "playlists" + ext)
        p_it = os.path.join(out, "playlist_tracks" + ext)
        s = time.perf_counter()
        copy_to(con, f"SELECT {PL_COLS} FROM _sp", p_pl, compression, per_thread)
        t_pl = time.perf_counter() - s
        s = time.perf_counter()
        con.execute("CREATE OR REPLACE TABLE _it AS "
                    "SELECT pid, unnest(tracks, recursive := true) FROM _sp")
        if layout == "two":
            con.execute("DROP TABLE _sp")   # libere la memoire avant l'ecriture Parquet
        t_expl = time.perf_counter() - s
        s = time.perf_counter()
        copy_to(con, "SELECT * RENAME (duration_ms AS track_duration_ms) FROM _it",
                p_it, compression, per_thread)
        t_wr = time.perf_counter() - s
        con.execute("DROP TABLE _it")
        made += [("two:playlists", p_pl, t_pl), ("two:playlist_tracks", p_it, t_expl + t_wr)]
        print(f"  two  : playlists {t_pl:4.1f}s ({fmt(sizeof(p_pl))})  |  "
              f"playlist_tracks : unnest {t_expl:5.1f}s + write {t_wr:4.1f}s "
              f"({fmt(sizeof(p_it))})")
    if layout in ("one", "both"):
        p_flat = os.path.join(out, "mpd_flat" + ext)
        s = time.perf_counter()
        con.execute(f"CREATE OR REPLACE TABLE _flat AS "
                    f"SELECT {PL_COLS}, unnest(tracks, recursive := true) FROM _sp")
        t_expl = time.perf_counter() - s
        s = time.perf_counter()
        copy_to(con, "SELECT * RENAME (duration_ms AS track_duration_ms) FROM _flat",
                p_flat, compression, per_thread)
        t_wr = time.perf_counter() - s
        con.execute("DROP TABLE _flat")
        made.append(("one:mpd_flat", p_flat, t_expl + t_wr))
        print(f"  one  : mpd_flat : unnest {t_expl:5.1f}s + write {t_wr:4.1f}s "
              f"({fmt(sizeof(p_flat))})")
    return made


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=os.path.join(PROJECT, "data"))
    ap.add_argument("--out", default=os.path.join(HERE, "parquet"))
    ap.add_argument("-n", "--sample", type=int, default=0)
    ap.add_argument("--layout", choices=["two", "one"], default="two")
    ap.add_argument("--compression", choices=["snappy", "zstd", "uncompressed"], default="zstd")
    ap.add_argument("--per-thread-output", action="store_true",
                    help="ecrit un dossier de fichiers (1 / thread) au lieu d'un seul fichier")
    ap.add_argument("--threads", type=int, default=0)
    ap.add_argument("--bench", action="store_true", help="compare dispositions x options sur l'echantillon")
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(args.data, "mpd.slice.*.json")))
    if not files:
        raise SystemExit(f"aucun mpd.slice.*.json dans {args.data}")
    if args.sample:
        files = files[: args.sample]
    all_n = len(sorted(glob.glob(os.path.join(args.data, "mpd.slice.*.json"))))
    src = ("['" + "', '".join(files) + "']" if args.sample and len(files) < all_n
           else "'" + os.path.join(args.data, "mpd.slice.*.json") + "'")
    bytes_in = sum(os.path.getsize(f) for f in files)

    # Staging sur disque (base DuckDB jetable) : `_sp` contient ~tout le dataset ;
    # le garder en memoire fait spiller/thrasher le writer Parquet (x15 sur 66 M
    # lignes). Une table sur disque garde la memoire bornee.
    stg = os.path.join(os.path.dirname(args.out) or ".", "_stg_parquet.duckdb")
    for p in (stg, stg + ".wal"):
        if os.path.exists(p):
            os.remove(p)
    con = duckdb.connect(stg)
    if args.threads:
        con.execute(f"PRAGMA threads={args.threads}")
    con.execute("PRAGMA enable_progress_bar")

    print(f"{len(files)}/{all_n} slices | {bytes_in/1e9:.1f} Go JSON | duckdb {duckdb.__version__}")
    t0 = time.perf_counter()
    con.execute(f"""
        CREATE TABLE _sp AS
        SELECT pl.* FROM (
            SELECT unnest(playlists) AS pl
            FROM read_json({src}, maximum_object_size=200000000, sample_size=-1)
        )
    """)
    parse_s = time.perf_counter() - t0
    npl = con.execute("SELECT count(*) FROM _sp").fetchone()[0]
    print(f"parse JSON -> _sp : {parse_s:.1f}s  ({npl} playlists)")

    if args.bench:
        for comp in ("snappy", "zstd"):
            for pto in (False, True):
                out = f"/tmp/pqbench_{comp}_{'pto' if pto else 'file'}"
                os.system(f"rm -rf {out}")
                os.makedirs(out, exist_ok=True)
                ext = "" if pto else ".parquet"
                print(f"\n[{comp} | {'per-thread dir' if pto else 'fichier unique'}]")
                build(con, "both", out, comp, pto, ext)
                os.system(f"rm -rf {out}")
        con.close()
        for p in (stg, stg + ".wal"):
            if os.path.exists(p):
                os.remove(p)
        print(f"\n(parse commun : {parse_s:.1f}s ; total = parse + ecriture)")
        return

    os.makedirs(args.out, exist_ok=True)
    ext = "" if args.per_thread_output else ".parquet"
    made = build(con, args.layout, args.out, args.compression, args.per_thread_output, ext)
    total = time.perf_counter() - t0
    con.close()
    for p in (stg, stg + ".wal"):
        if os.path.exists(p):
            os.remove(p)
    print("-" * 60)
    for name, path, dt in made:
        print(f"  {name:<22} {dt:6.1f}s  {fmt(sizeof(path)):>9}  {path}")
    print(f"TOTAL {total:.1f}s  ({bytes_in/1e6/total:.0f} Mo/s JSON)  -> {args.out}")


if __name__ == "__main__":
    main()
