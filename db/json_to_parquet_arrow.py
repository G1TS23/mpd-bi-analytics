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

Barre de progression (temps ecoule / restant / debit) via `tqdm` s'il est
installe, sinon repli maison ; masquee si la sortie n'est pas un terminal.

    python db/json_to_parquet_arrow.py                 # -> db/parquet_arrow/
    python db/json_to_parquet_arrow.py --out /tmp/pq --workers 8 --level 1
    python db/json_to_parquet_arrow.py -n 200          # echantillon
"""
import argparse
import glob
import os
import sys
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

try:
    from tqdm import tqdm
except ModuleNotFoundError:
    tqdm = None


class _Bar:
    """Repli minimal si tqdm est absent : ecoule / restant / debit."""

    def __init__(self, total, unit_bytes):
        self.total, self.unit_bytes = total, unit_bytes
        self.n = self.done_bytes = 0
        self.t0 = time.perf_counter()
        self.tty = sys.stderr.isatty()

    def update(self, n=1, nbytes=0):
        self.n += n
        self.done_bytes += nbytes
        if not self.tty:
            return
        el = time.perf_counter() - self.t0
        rate = self.n / el if el else 0
        eta = (self.total - self.n) / rate if rate else 0
        frac = self.n / self.total
        bar = ("#" * int(frac * 28)).ljust(28)
        mbs = self.done_bytes / 1e6 / el if el else 0
        sys.stderr.write(
            f"\r[{bar}] {self.n:>4}/{self.total}  "
            f"ecoule {el:5.1f}s  restant ~{eta:4.1f}s  {mbs:4.0f} Mo/s"
        )
        sys.stderr.flush()

    def close(self):
        if self.tty:
            sys.stderr.write("\n")
            sys.stderr.flush()

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

    sizes = [os.path.getsize(f) for f in files]
    if tqdm:
        bar = tqdm(
            total=len(files), desc="JSON->Parquet", unit="slice", dynamic_ncols=True,
            bar_format="{desc} {percentage:3.0f}%|{bar}| {n}/{total} "
                       "[ecoule {elapsed} - reste {remaining} - {rate_fmt}{postfix}]",
        )
    else:
        bar = _Bar(len(files), None)

    t0 = time.perf_counter()
    npl = nit = done_bytes = 0
    with ProcessPoolExecutor(max_workers=workers, initializer=_init,
                             initargs=(args.out, args.level)) as ex:
        for i, (a, b) in enumerate(ex.map(convert_one, enumerate(files), chunksize=4)):
            npl += a
            nit += b
            done_bytes += sizes[i]
            el = time.perf_counter() - t0
            if tqdm:
                bar.set_postfix_str(f"{done_bytes/1e6/el:.0f} Mo/s", refresh=False)
                bar.update(1)
            else:
                bar.update(1, sizes[i])
    bar.close()
    dt = time.perf_counter() - t0

    sz_pl = prefix_size(args.out, "playlists")
    sz_it = prefix_size(args.out, "playlist_tracks")
    print("-" * 60)
    print(f"playlists-*.parquet       : {npl:>10} lignes  {sz_pl/1e6:8.0f} Mo")
    print(f"playlist_tracks-*.parquet : {nit:>10} lignes  {sz_it/1e9:8.2f} Go")
    print(f"TOTAL {dt:.1f} s  ({bytes_in/1e6/dt:.0f} Mo/s JSON)  -> {args.out}/")


if __name__ == "__main__":
    main()
