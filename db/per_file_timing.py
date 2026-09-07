#!/usr/bin/env python3
"""
Temps de traitement par fichier `mpd.slice.*.json`.

Mesure, fichier par fichier (donc SERIALISE, contrairement au pipeline reel qui
lit le glob en parallele), le cout de : parse JSON + unnest(playlists) +
unnest(tracks). Ecrit un CSV (une ligne par fichier) + un resume.

    python db/per_file_timing.py [--data data] [--csv db/per_file_timing.csv]
"""
import argparse
import csv
import glob
import os
import statistics as st
import time

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(HERE)


def pearson(x, y):
    mx, my = st.mean(x), st.mean(y)
    num = sum((a - mx) * (b - my) for a, b in zip(x, y))
    den = (sum((a - mx) ** 2 for a in x) * sum((b - my) ** 2 for b in y)) ** 0.5
    return num / den if den else float("nan")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(PROJECT, "data"))
    ap.add_argument("--csv", default=os.path.join(HERE, "per_file_timing.csv"))
    a = ap.parse_args()

    files = sorted(glob.glob(os.path.join(a.data, "mpd.slice.*.json")))
    if not files:
        raise SystemExit(f"aucun mpd.slice.*.json dans {a.data}")
    con = duckdb.connect()  # en memoire

    rows = []
    t_all = time.perf_counter()
    for i, f in enumerate(files, 1):
        sz = os.path.getsize(f)
        s = time.perf_counter()
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _sp AS
            SELECT pl.* FROM (
                SELECT unnest(playlists) AS pl
                FROM read_json('{f}', maximum_object_size=200000000, sample_size=-1)
            )
        """)
        con.execute("CREATE OR REPLACE TEMP TABLE _it AS "
                    "SELECT pid, unnest(tracks, recursive := true) FROM _sp")
        npl = con.execute("SELECT count(*) FROM _sp").fetchone()[0]
        ntr = con.execute("SELECT count(*) FROM _it").fetchone()[0]
        rows.append((os.path.basename(f), sz, npl, ntr, time.perf_counter() - s))
        if i % 100 == 0:
            print(f"  {i}/{len(files)}  (cumul {time.perf_counter()-t_all:.0f}s)")
    total = time.perf_counter() - t_all

    with open(a.csv, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["file", "bytes", "n_playlists", "n_tracks", "parse_s"])
        w.writerows(rows)

    times = [r[4] for r in rows]
    sizes = [r[1] for r in rows]
    tracks = [r[3] for r in rows]
    mbps = [r[1] / 1e6 / r[4] for r in rows]
    ordered = sorted(times)

    print(f"\n=== Temps par fichier (n={len(files)}, serialise, connexion en memoire) ===")
    print(f"total serialise : {total:.0f}s  (le pipeline reel traite ces fichiers EN PARALLELE)")
    print(f"min / moy / med : {min(times)*1000:.0f} / {st.mean(times)*1000:.0f} / {st.median(times)*1000:.0f} ms")
    print(f"p95 / max / sd  : {ordered[int(.95*len(ordered))]*1000:.0f} / {max(times)*1000:.0f} / {st.pstdev(times)*1000:.0f} ms")
    print(f"debit / fichier : {st.mean(mbps):.0f} Mo/s (moy), {min(mbps):.0f}..{max(mbps):.0f}")
    print(f"taille fichier  : {st.mean(sizes)/1e6:.1f} Mo (moy), {min(sizes)/1e6:.1f}..{max(sizes)/1e6:.1f}")
    print(f"pistes / fichier: {int(st.mean(tracks))} (moy), {min(tracks)}..{max(tracks)}")
    print(f"corr(temps, taille)     : {pearson(times, sizes):.2f}")
    print(f"corr(temps, nb pistes)  : {pearson(times, tracks):.2f}")

    print("\n8 fichiers les plus lents :")
    for name, sz, npl, ntr, dt in sorted(rows, key=lambda r: -r[4])[:8]:
        print(f"  {dt*1000:5.0f} ms  {name:<28} {sz/1e6:5.1f} Mo  {ntr:6d} pistes")
    print(f"\nCSV : {a.csv}")


if __name__ == "__main__":
    main()
