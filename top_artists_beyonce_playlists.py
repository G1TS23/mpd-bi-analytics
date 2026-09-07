#!/usr/bin/env python3
"""
Top des artistes qui co-apparaissent dans les playlists contenant Beyonce.

Perimetre : les playlists ayant AU MOINS une piste dont l'artiste principal est
Beyonce (artist_uri canonique). Dans ces playlists on classe tous les autres
artistes selon deux mesures :
  - playlists : nombre de playlists distinctes (parmi celles de Beyonce) ou
    l'artiste apparait  --> mesure principale du classement
  - occurrences : nombre total de pistes de cet artiste dans ces playlists

Beyonce elle-meme est exclue du classement (presente dans 100 % par definition).

Meme socle technique que count_beyonce.py : multiprocessing, orjson si dispo,
un seul passage sur le dataset.

Usage :
    python3 top_artists_beyonce_playlists.py [N]      # N = taille du top (defaut 3)
"""
import glob
import os
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor

try:
    import orjson

    def load_bytes(b):
        return orjson.loads(b)
except ModuleNotFoundError:
    import json

    def load_bytes(b):
        return json.loads(b)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
BEYONCE_URI = "spotify:artist:6vWDO969PvNqNYHIOW5v0m"


def process_file(path):
    occ = Counter()        # uri -> occurrences de pistes (dans les playlists Beyonce)
    pl_count = Counter()   # uri -> nb de playlists Beyonce distinctes
    names = {}             # uri -> nom (representatif)
    beyonce_playlists = 0

    with open(path, "rb") as f:
        data = load_bytes(f.read())

    for pl in data["playlists"]:
        tracks = pl["tracks"]
        if not any(t["artist_uri"] == BEYONCE_URI for t in tracks):
            continue
        beyonce_playlists += 1
        seen = set()
        for t in tracks:
            uri = t["artist_uri"]
            occ[uri] += 1
            if uri not in names:
                names[uri] = t["artist_name"]
            seen.add(uri)
        for uri in seen:
            pl_count[uri] += 1

    return occ, pl_count, names, beyonce_playlists


def fmt(n):
    return f"{n:,}".replace(",", " ")


def main():
    topn = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    files = sorted(glob.glob(os.path.join(DATA_DIR, "mpd.slice.*.json")))
    if not files:
        sys.exit(f"Aucun fichier mpd.slice.*.json trouve dans {DATA_DIR}")
    engine = "orjson" if "orjson" in sys.modules else "json (stdlib)"
    workers = os.cpu_count() or 4
    print(f"{len(files)} fichiers | {workers} coeurs | parseur: {engine}", file=sys.stderr)
    t0 = time.perf_counter()

    occ = Counter()
    pl_count = Counter()
    names = {}
    beyonce_playlists = 0

    with ProcessPoolExecutor(max_workers=workers) as ex:
        for i, (o, p, nm, bp) in enumerate(
            ex.map(process_file, files, chunksize=4), 1
        ):
            occ.update(o)
            pl_count.update(p)
            for uri, name in nm.items():
                names.setdefault(uri, name)
            beyonce_playlists += bp
            if i % 200 == 0:
                print(f"  {i}/{len(files)}", file=sys.stderr)

    dt = time.perf_counter() - t0

    # retire Beyonce du classement
    occ.pop(BEYONCE_URI, None)
    pl_count.pop(BEYONCE_URI, None)

    print()
    print("=" * 72)
    print(f"Playlists contenant Beyonce : {fmt(beyonce_playlists)}")
    print(f"Artistes distincts co-apparaissant : {fmt(len(pl_count))}")
    print("=" * 72)

    print(f"\nTOP {topn} par nombre de playlists (mesure principale)")
    print(f"{'#':>3}  {'playlists':>10} {'(part)':>8}  {'occurrences':>12}  artiste")
    for rank, (uri, c) in enumerate(pl_count.most_common(topn), 1):
        share = 100 * c / beyonce_playlists
        print(f"{rank:>3}  {fmt(c):>10} {share:6.1f}%  {fmt(occ[uri]):>12}  {names.get(uri, uri)}")

    print(f"\nTOP {topn} par nombre d'occurrences de pistes")
    print(f"{'#':>3}  {'occurrences':>12}  {'playlists':>10}  artiste")
    for rank, (uri, c) in enumerate(occ.most_common(topn), 1):
        print(f"{rank:>3}  {fmt(c):>12}  {fmt(pl_count[uri]):>10}  {names.get(uri, uri)}")

    print(f"\n[temps de traitement : {dt:.1f} s]", file=sys.stderr)


if __name__ == "__main__":
    main()
