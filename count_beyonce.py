#!/usr/bin/env python3
"""
Compte les apparitions de Beyonce dans le Spotify Million Playlist Dataset (dossier ./data).

Deroulement :
  1. AUDIT DES ORTHOGRAPHES : on liste chaque couple (artist_name, artist_uri)
     dont le nom contient "beyonc" (insensible a la casse et aux accents).
  2. COMPTAGE (feat exclus) : seules les pistes ou Beyonce est artiste principal
     (match sur artist_uri canonique) sont comptees. Les featurings sont exclus
     et comptes a part pour information.
  3. Sous-detail solo / "Beyonce feat. ...".

Optimisations vs version initiale (~34 s -> ~10-15 s) :
  - plus aucun appel a unicodedata dans la boucle : ".lower()" + test de
    sous-chaine "beyonc" suffit ("beyonce" est un prefixe de "beyonce"/"beyonce");
  - track_name.lower() calcule une seule fois par piste, reutilise pour le
    test featuring et la detection de mention;
  - regex de featuring pre-compilee sans IGNORECASE (applique au texte deja
    minuscule);
  - orjson utilise s'il est installe (sinon json standard sur des bytes);
  - taille de lot (chunksize) pour reduire les echanges inter-processus,
    parallelisme sur tous les coeurs.

Usage :
    python3 count_beyonce.py
"""
import glob
import os
import re
import sys
import time
from collections import Counter, defaultdict
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

# artist_uri canonique de Beyonce dans le MPD
BEYONCE_URI = "spotify:artist:6vWDO969PvNqNYHIOW5v0m"

# featuring dans un titre -- applique a du texte DEJA en minuscules
FEAT_RE = re.compile(r"\b(feat\.?|featuring|ft\.?|with)\b")


def process_file(path):
    spellings = Counter()          # {(name, uri): occurrences} pour tout nom ~ "beyonc"
    primary_tracks = 0             # Beyonce = artiste principal (URI)
    primary_solo = 0
    primary_collab = 0
    playlists_primary = 0
    feat_only_tracks = 0           # Beyonce citee dans le titre d'un autre artiste
    playlists_feat_only = 0

    with open(path, "rb") as f:
        data = load_bytes(f.read())

    for pl in data["playlists"]:
        has_primary = False
        has_feat_only = False
        for t in pl["tracks"]:
            uri = t["artist_uri"]

            if uri == BEYONCE_URI:
                primary_tracks += 1
                has_primary = True
                spellings[(t["artist_name"], uri)] += 1
                if FEAT_RE.search(t["track_name"].lower()):
                    primary_collab += 1
                else:
                    primary_solo += 1
                continue

            # pistes d'un autre artiste principal : mention de Beyonce ?
            tnl = t["track_name"].lower()
            if "beyonc" in tnl:
                feat_only_tracks += 1
                has_feat_only = True

            nl = t["artist_name"].lower()
            if "beyonc" in nl:
                spellings[(t["artist_name"], uri)] += 1

        if has_primary:
            playlists_primary += 1
        if has_feat_only:
            playlists_feat_only += 1

    return {
        "spellings": spellings,
        "primary_tracks": primary_tracks,
        "primary_solo": primary_solo,
        "primary_collab": primary_collab,
        "playlists_primary": playlists_primary,
        "feat_only_tracks": feat_only_tracks,
        "playlists_feat_only": playlists_feat_only,
    }


def fmt(n):
    return f"{n:,}".replace(",", " ")


def main():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "mpd.slice.*.json")))
    if not files:
        sys.exit(f"Aucun fichier mpd.slice.*.json trouve dans {DATA_DIR}")
    engine = "orjson" if "orjson" in sys.modules else "json (stdlib)"
    workers = os.cpu_count() or 4
    print(f"{len(files)} fichiers | {workers} coeurs | parseur: {engine}", file=sys.stderr)
    t0 = time.perf_counter()

    agg = {
        "primary_tracks": 0, "primary_solo": 0, "primary_collab": 0,
        "playlists_primary": 0, "feat_only_tracks": 0, "playlists_feat_only": 0,
    }
    spellings = Counter()

    with ProcessPoolExecutor(max_workers=workers) as ex:
        for i, r in enumerate(ex.map(process_file, files, chunksize=4), 1):
            for k in agg:
                agg[k] += r[k]
            spellings.update(r["spellings"])
            if i % 200 == 0:
                print(f"  {i}/{len(files)}", file=sys.stderr)

    dt = time.perf_counter() - t0

    print()
    print("=" * 68)
    print('1. AUDIT DES ORTHOGRAPHES  (tout nom d\'artiste contenant "beyonc")')
    print("=" * 68)
    by_uri = defaultdict(list)
    for (name, uri), c in spellings.items():
        by_uri[uri].append((name, c))
    for uri, items in sorted(by_uri.items(), key=lambda kv: -sum(c for _, c in kv[1])):
        tag = "  <-- URI canonique" if uri == BEYONCE_URI else "  <-- AUTRE URI"
        print(f"\n{uri}{tag}")
        for name, c in sorted(items, key=lambda x: -x[1]):
            print(f"    {c:>8}  {name!r}")
    print(f"\n  -> {len(by_uri)} URI distincte(s) portant un nom ~ Beyonce.")

    print()
    print("=" * 68)
    print("2. COMPTAGE - FEAT EXCLUS  (Beyonce = artiste principal uniquement)")
    print("=" * 68)
    print(f"Occurrences de pistes (Beyonce lead)   : {fmt(agg['primary_tracks'])}")
    print(f"Playlists distinctes concernees        : {fmt(agg['playlists_primary'])}")
    print(f"  dont titres solo (sans feat.)        : {fmt(agg['primary_solo'])}")
    print(f"  dont titres 'Beyonce feat./with ...' : {fmt(agg['primary_collab'])}")

    print()
    print("=" * 68)
    print("3. EXCLUS - Beyonce citee seulement en featuring")
    print("=" * 68)
    print(f"  occurrences                          : {fmt(agg['feat_only_tracks'])}")
    print(f"  playlists distinctes concernees      : {fmt(agg['playlists_feat_only'])}")

    print(f"\n[temps de traitement : {dt:.1f} s]", file=sys.stderr)


if __name__ == "__main__":
    main()
