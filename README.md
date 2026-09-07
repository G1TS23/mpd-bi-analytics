# mpd-bi-analytics

Projet **Business Intelligence & Analytics** (EFREI M2) sur le **Spotify Million Playlist Dataset**.

Objectif : passer d'une archive brute de 1000 fichiers JSON à un **schéma relationnel
interrogeable en SQL** (première étape avant un data warehouse), pour répondre à des
questions analytiques ad hoc du type _« combien de fois apparaît Beyoncé dans les playlists ? »_.

## Contenu du dépôt

| Chemin | Rôle |
|---|---|
| `db/MODELE_DONNEES.md` | **Document principal** : analyse des données, MCD / MLD / MPD, dictionnaire de données, exemples de requêtes, cible entrepôt |
| `db/schema.sql` | DDL du modèle physique (DuckDB) |
| `db/schema.dbml` | Même schéma au format [dbdiagram.io](https://dbdiagram.io) |
| `db/warehouse_star.dbml` | Brouillon du schéma en étoile (phase entrepôt) |
| `db/build.sql` | ETL SQL : JSON → tables |
| `db/load_mpd.py` | Orchestrateur du chargement (schéma + ETL + contrôles) |
| `db/PERFORMANCE.md` | Métriques d'ingestion (par étape, par fichier) et pistes d'optimisation |
| `db/bench_ingest.py` | Banc d'essai reproductible de l'ingestion |
| `db/per_file_timing.py` · `.csv` | Temps de traitement fichier par fichier |
| `db/json_to_parquet.py` | Export JSON → dataset Parquet (`playlists` + `playlist_tracks`, ~2,5 min) |
| `count_beyonce.py` | Script autonome : occurrences de Beyoncé + audit des orthographes |
| `top_artists_beyonce_playlists.py` | Script autonome : artistes co-présents avec Beyoncé |
| `CONTEXT.md` | Énoncé / pitch du projet |

## Données (non incluses)

Le dossier `data/` (~31 Go, 1000 × `mpd.slice.*.json`) **n'est pas versionné** : taille, et
licence du dataset qui n'autorise pas la redistribution. Le récupérer via
[AIcrowd — Spotify Million Playlist Dataset Challenge](https://www.aicrowd.com/challenges/spotify-million-playlist-dataset-challenge)
et le placer dans `data/`.

## Mise en route

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# Construction complète — défaut : PK seules, FK revalidées après coup
# (~5 min, produit db/mpd.duckdb ~5,2 Go)
.venv/bin/python db/load_mpd.py

# Itération rapide sur 50 slices
.venv/bin/python db/load_mpd.py -n 50

# PK + FK imposées par le moteur (~7 min, ~7 Go)
.venv/bin/python db/load_mpd.py --strict

# Aucune contrainte (~3,5 min, ~4,3 Go)
.venv/bin/python db/load_mpd.py --fast
```

Puis :

```bash
.venv/bin/python -c "import duckdb; c=duckdb.connect('db/mpd.duckdb', read_only=True); \
  print(c.sql('SELECT count(*) FROM playlist_track'))"
```

## Chiffres de référence (dataset complet)

| | |
|---|---|
| Playlists | 1 000 000 |
| Lignes `playlist_track` | 66 346 428 |
| Tracks / Artistes / Albums distincts | 2 262 292 / 295 860 / 734 684 |
| Beyoncé (artiste principal) | 230 857 occurrences · 97 468 playlists |
| Top 3 artistes co-présents avec Beyoncé | Rihanna, Drake, Kanye West |
