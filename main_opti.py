import glob
import os
import time
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
from dotenv import load_dotenv

load_dotenv()

INPUT_FOLDER = os.environ["INPUT_FOLDER"]
OUTPUT_FOLDER = os.environ["OUTPUT_FOLDER"]

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

WRITE_OPTS = dict(compression="zstd", compression_level=1)

def process_json(json_file):
    try:
        with open(json_file, "rb") as f:
            data = orjson.loads(f.read())

        pl_ids_t, track_uris, track_names = [], [], []
        artist_names, artist_uris, album_names, album_uris = [], [], [], []
        durations, positions = [], []

        pl_ids, pl_names, num_tracks, num_albums, num_followers, modified_ats = [], [], [], [], [], []

        for playlist in data.get("playlists", []):
            pid = playlist["pid"]
            pl_ids.append(pid)
            pl_names.append(playlist["name"])
            num_tracks.append(playlist["num_tracks"])
            num_albums.append(playlist["num_albums"])
            num_followers.append(playlist["num_followers"])
            modified_ats.append(playlist["modified_at"])

            for track in playlist["tracks"]:
                pl_ids_t.append(pid)
                track_uris.append(track["track_uri"])
                track_names.append(track["track_name"])
                artist_names.append(track["artist_name"])
                artist_uris.append(track["artist_uri"])
                album_names.append(track["album_name"])
                album_uris.append(track["album_uri"])
                durations.append(track["duration_ms"])
                positions.append(track["pos"])

        base = os.path.basename(json_file).replace(".json", "")

        pq.write_table(
            pa.table({
                "playlist_id":  pa.array(pl_ids_t,    type=pa.int32()),
                "track_uri":    pa.array(track_uris),
                "track_name":   pa.array(track_names),
                "artist_name":  pa.array(artist_names),
                "artist_uri":   pa.array(artist_uris),
                "album_name":   pa.array(album_names),
                "album_uri":    pa.array(album_uris),
                "duration_ms":  pa.array(durations,   type=pa.int64()),
                "position":     pa.array(positions,   type=pa.int32()),
            }),
            os.path.join(OUTPUT_FOLDER, f"{base}_tracks.parquet"),
            **WRITE_OPTS,
        )

        pq.write_table(
            pa.table({
                "playlist_id":   pa.array(pl_ids,        type=pa.int32()),
                "playlist_name": pa.array(pl_names),
                "num_tracks":    pa.array(num_tracks,    type=pa.int32()),
                "num_albums":    pa.array(num_albums,    type=pa.int32()),
                "num_followers": pa.array(num_followers, type=pa.int32()),
                "modified_at":   pa.array(modified_ats,  type=pa.int64()),
            }),
            os.path.join(OUTPUT_FOLDER, f"{base}_playlists.parquet"),
            **WRITE_OPTS,
        )

    except Exception as e:
        print(f"Erreur avec {json_file}: {e}")

if __name__ == "__main__":
    json_files = glob.glob(os.path.join(INPUT_FOLDER, "*.json"))
    # num_workers = max(cpu_count() // 2, 1)
    num_workers = max(cpu_count() -1 , 1)

    print(f"Utilisation de {num_workers} processus...")
    start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(process_json, f): f for f in json_files}
        for _ in tqdm(as_completed(futures), total=len(futures), desc="Playlists en traitement"):
            pass
    print(f"Terminé en {time.perf_counter() - start:.2f}s")
