# Ingestion — métriques & performance

Mesures de la transformation `data/*.json` → `db/mpd.duckdb`
(DuckDB 1.5.5, MacBook, 11 threads, `memory_limit` 14 Gio).
Banc d'essai reproductible : `db/bench_ingest.py`.

---

## 1. Chiffres de référence (dataset complet)

| Configuration | Temps ETL | Fichier `.duckdb` | Débit JSON | Débit lignes assoc. |
|---|---:|---:|---:|---:|
| **Baseline** (v1 : table `_stg_item` matérialisée, index créés à vide, contraintes ON) | ~490–560 s | **19 Go** | ~60 Mo/s | ~120 k/s |
| **Optimisé, contraintes ON** (vue `_stg_item`, index différés) | **434 s** | **11 Go** | 76 Mo/s | 153 k/s |
| **Optimisé, `--fast`** (sans FK ni PK composite) | **185 s** | **7,4 Go** | 178 Mo/s | 359 k/s |

Entrée : 1000 fichiers, **33 Go de JSON**. Sortie : 66 346 428 lignes `playlist_track`,
2,26 M `track`, 734 684 `album`, 295 860 `artist`, 1 M `playlist`.

Les 3 configurations passent les **4 contrôles d'intégrité** de `load_mpd.py`
(`num_tracks` = COUNT réel ; FK `playlist_track→track`, `track→artist`, `track→album`) —
en `--fast` l'intégrité n'est pas *imposée* par le moteur mais *vérifiée après coup*.

---

## 2. Décomposition par étape (échantillon 200 slices — 6,7 Go JSON, 13,3 M lignes assoc.)

| Étape | Baseline (contraintes) | `--fast` | Commentaire |
|---|---:|---:|---|
| `read_json` + `unnest(playlists)` → `_stg_playlist` | 20,1 s | 19,5 s | parse + écriture de 1,1 Go de staging |
| `unnest(tracks)` → `_stg_item` (table) | 10,0 s | 9,8 s | **supprimé** dans la version optimisée (vue) |
| `INSERT artist` | 0,3 s | 0,3 s | |
| `INSERT album` | 0,8 s | 0,8 s | |
| `INSERT track` (DISTINCT ~1 M + PK + 2 FK) | 8,0 s | 4,1 s | les contraintes doublent le coût |
| `INSERT playlist` | 0,1 s | 0,1 s | |
| **`INSERT playlist_track`** (13,3 M + PK composite + 2 FK + 1 index) | **46,3 s** | **10,9 s** | **poste n°1** — ×4,2 à cause des contraintes |
| **Total** | **85,7 s** | **45,6 s** | |
| Fichier obtenu | 4 440 Mo | 3 382 Mo | |

Parsing JSON isolé (connexion en mémoire, sans écriture) : **~1,1 Go/s**.
Le surcoût de `_stg_playlist` (20 s vs 6 s de parse pur) = écriture disque + `CHECKPOINT`.

---

## 3. Ce qui a été appliqué

| Optimisation | Effet mesuré (échantillon 200) | Effet plein (33 Go) |
|---|---|---|
| **`_stg_item` en VUE** au lieu d'une table matérialisée | 85,7 s → 73,8 s (−14 %) ; 4 440 → 3 255 Mo | évite ~6 Go d'écriture de staging **et** l'espace mort résiduel : **19 Go → 11 Go** |
| **Index secondaires créés en fin de chargement** (table pleine) au lieu d'être maintenus à chaque `INSERT` | 73,8 s → 68,8 s (−7 %) | `INSERT playlist_track` : 42 → 32 s, puis index bâti en 8 s |

> DuckDB ne rend pas au système l'espace d'une table supprimée (`DROP TABLE _stg_item`)
> sans `VACUUM`/compaction : la v1 gardait ~8 Go de pages mortes dans le fichier.
> Ne jamais matérialiser `_stg_item` supprime le problème à la racine.

Sans effet mesurable, non retenus : `read_json(sample_size=…)` (−1 / 1024 / explicite : < 0,5 s d'écart),
`columns=` explicites (−0,3 s, non rentable vs la maintenance du schéma inline),
`PRAGMA preserve_insertion_order=false` (aucun gain, RSS plus élevé),
réduction du nombre de threads (défaut = 11 = optimal), `memory_limit` (RSS ~3,7 Go, aucune pression).

---

## 4. Pistes restantes, par impact

1. **Contraintes FK + PK composite = le principal levier.**
   ×2,3 sur le temps total (434 s → 185 s) et ×1,5 sur la taille (11 Go → 7,4 Go).
   → utiliser **`--fast`** pour les rechargements de travail (l'intégrité reste
   contrôlée après coup) ; garder les contraintes pour la base « de référence » livrée.

2. **Clé de substitution `INTEGER` pour `playlist_track`** (étape schéma en étoile).
   La table de faits fait 66 M lignes de `VARCHAR(22)` ; un `track_key INTEGER` la
   réduirait d'un facteur ~3, accélérerait le chargement (comparaisons d'entiers) et
   toutes les jointures analytiques. C'est le prochain gain **structurel**.

3. **Parsing JSON = plancher ~100–150 s** sur 33 Go (~1,1 Go/s en parse pur, moins
   avec l'écriture du staging). Peu compressible :
   - convertir une fois les slices en **Parquet** (`COPY … TO … (FORMAT parquet)`) puis
     recharger : intéressant seulement si on recharge souvent (la conversion coûte ~un parse).
   - `read_json` parallélise déjà sur les 1000 fichiers ; SSD ~saturé en lecture.

4. **Chaîne « une seule passe »** : construire les dimensions et les faits en une
   requête (CTE + `INSERT … RETURNING` ou `read_json` → plusieurs `INSERT` partageant
   un scan) évite de relire `_stg_playlist`. Gain attendu faible (le scan de
   `_stg_playlist` déjà en base est rapide) — à mesurer si besoin.

5. **`CHECKPOINT` final unique** plutôt qu'implicite : déjà le cas via `load_mpd.py`
   (un seul `con.close()`).

---

## 5. Reproduire

```bash
# banc d'essai : variantes sur un échantillon
.venv/bin/python db/bench_ingest.py -n 200 --tag baseline
.venv/bin/python db/bench_ingest.py -n 200 --fast --tag fast
.venv/bin/python db/bench_ingest.py -n 200 --fast --no-stg-item --tag fast_novue

# chargement réel instrumenté (2 lignes de résumé par phase)
.venv/bin/python db/load_mpd.py            # optimisé + contraintes  (~7 min, 11 Go)
.venv/bin/python db/load_mpd.py --fast     # optimisé sans contraintes (~3 min, 7,4 Go)
```
