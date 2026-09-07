#!/usr/bin/env python3
"""
Export JSON -> Parquet (zstd) SANS DuckDB : orjson + pyarrow + multiprocessing.

Chaque worker traite un fichier `mpd.slice.*.json` de bout en bout (parse,
construction des colonnes, ecriture Parquet). Sortie = un seul dossier, le
prefixe du nom de fichier distingue les deux tables (un fichier par slice) :

    <out>/playlists-XXXX.parquet          1 ligne / playlist
    <out>/playlist_tracks-XXXX.parquet    1 ligne / (playlist, position)

Lecture ensuite : pyarrow.dataset, pandas, polars, DuckDB (`read_parquet(
'<out>/playlist_tracks-*.parquet')`), Spark...

    python db/json_to_parquet_arrow.py                 # -> db/parquet_arrow/
    python db/json_to_parquet_arrow.py --out /tmp/pq --workers 8 --level 1
    python db/json_to_parquet_arrow.py -n 200          # echantillon
"""
import argparse
import glob
import os
import time
from concurrent.futures import ProcessPoolExecutor

import pyarrow as pa
import pyarrow.parquet as pq

try:
    import orjson as _j

    def loads(b):
        return _j.loads(b)
except ModuleNotFoundError:
    import json as _j

    def loads(b):
        return _j.loads(b)

# ---- schemas figes (evite l'inference, garantit des parts homogenes) --------
PLAYLIST_SCHEMA = pa.schema([
    ("pid", pa.int64()),
    ("playlist_name", pa.string()),
    ("collaborative", pa.string()),
    ("modified_at", pa.int64()),
    ("description", pa.string()),
    ("num_followers", pa.int64()),
    ("num_edits", pa.int64()),
    ("num_tracks", pa.int64()),
    ("num_albums", pa.int64()),
    ("num_artists", pa.int64()),
    ("playlist_duration_ms", pa.int64()),
])
ITEM_SCHEMA = pa.schema([
    ("pid", pa.int64()),
    ("pos", pa.int32()),
    ("artist_name", pa.string()),
    ("track_uri", pa.string()),
    ("artist_uri", pa.string()),
    ("track_name", pa.string()),
    ("album_uri", pa.string()),
    ("track_duration_ms", pa.int64()),
    ("album_name", pa.string()),
])

_CFG = {}   # rempli par l'initialiseur de pool : out, level


def _init(out, level):
    _CFG["out"] = out
    _CFG["level"] = level


def convert_one(arg):
    idx, path = arg
    out, level = _CFG["out"], _CFG["level"]
    with open(path, "rb") as fh:
        playlists = loads(fh.read())["playlists"]

    p_pid = []; p_name = []; p_collab = []; p_mod = []; p_desc = []
    p_fol = []; p_edt = []; p_nt = []; p_nal = []; p_nar = []; p_dur = []
    t_pid = []; t_pos = []; t_an = []; t_turi = []; t_auri = []
    t_tn = []; t_alu = []; t_dur = []; t_aln = []

    for pl in playlists:
        pid = pl["pid"]
        p_pid.append(pid)
        p_name.append(pl["name"])
        p_collab.append(pl["collaborative"])
        p_mod.append(pl["modified_at"])
        p_desc.append(pl.get("description"))
        p_fol.append(pl["num_followers"])
        p_edt.append(pl["num_edits"])
        p_nt.append(pl["num_tracks"])
        p_nal.append(pl["num_albums"])
        p_nar.append(pl["num_artists"])
        p_dur.append(pl["duration_ms"])
        for t in pl["tracks"]:
            t_pid.append(pid)
            t_pos.append(t["pos"])
            t_an.append(t["artist_name"])
            t_turi.append(t["track_uri"])
            t_auri.append(t["artist_uri"])
            t_tn.append(t["track_name"])
            t_alu.append(t["album_uri"])
            t_dur.append(t["duration_ms"])
            t_aln.append(t["album_name"])

    pl_tbl = pa.table(
        [p_pid, p_name, p_collab, p_mod, p_desc, p_fol, p_edt, p_nt, p_nal, p_nar, p_dur],
        schema=PLAYLIST_SCHEMA,
    )
    it_tbl = pa.table(
        [t_pid, t_pos, t_an, t_turi, t_auri, t_tn, t_alu, t_dur, t_aln],
        schema=ITEM_SCHEMA,
    )
    kw = dict(compression="zstd", compression_level=level)
    pq.write_table(pl_tbl, f"{out}/playlists-{idx:04d}.parquet", **kw)
    pq.write_table(it_tbl, f"{out}/playlist_tracks-{idx:04d}.parquet", **kw)
    return len(p_pid), len(t_pid)


def prefix_size(out, prefix):
    return sum(os.path.getsize(os.path.join(out, f)) for f in os.listdir(out)
               if f.startswith(prefix + "-") and f.endswith(".parquet"))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"))
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "parquet_arrow"))
    ap.add_argument("-n", "--sample", type=int, default=0)
    ap.add_argument("--workers", type=int, default=0, help="defaut : nb de coeurs")
    ap.add_argument("--level", type=int, default=1, help="niveau zstd (1 = rapide, defaut)")
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(args.data, "mpd.slice.*.json")))
    if not files:
        raise SystemExit(f"aucun mpd.slice.*.json dans {args.data}")
    if args.sample:
        files = files[: args.sample]
    bytes_in = sum(os.path.getsize(f) for f in files)
    workers = args.workers or os.cpu_count() or 4

    os.makedirs(args.out, exist_ok=True)

    print(f"{len(files)} slices | {bytes_in/1e9:.1f} Go JSON | {workers} workers | "
          f"zstd niveau {args.level} | parseur {_j.__name__}")
    t0 = time.perf_counter()
    npl = nit = 0
    with ProcessPoolExecutor(max_workers=workers, initializer=_init,
                             initargs=(args.out, args.level)) as ex:
        for a, b in ex.map(convert_one, enumerate(files), chunksize=4):
            npl += a
            nit += b
    dt = time.perf_counter() - t0

    sz_pl = prefix_size(args.out, "playlists")
    sz_it = prefix_size(args.out, "playlist_tracks")
    print("-" * 60)
    print(f"playlists-*.parquet       : {npl:>10} lignes  {sz_pl/1e6:8.0f} Mo")
    print(f"playlist_tracks-*.parquet : {nit:>10} lignes  {sz_it/1e9:8.2f} Go")
    print(f"TOTAL {dt:.1f} s  ({bytes_in/1e6/dt:.0f} Mo/s JSON)  -> {args.out}/")


if __name__ == "__main__":
    main()
