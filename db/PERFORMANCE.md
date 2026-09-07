# Ingestion — métriques & performance

Mesures de la transformation `data/*.json` → `db/mpd.duckdb`
(DuckDB 1.5.5, MacBook, 11 threads, `memory_limit` 14 Gio).
Banc d'essai reproductible : `db/bench_ingest.py`.

---

## 1. Chiffres de référence (dataset complet)

| Configuration | Temps ETL | Fichier `.duckdb` |
|---|---:|---:|
| v1 (table `_stg_item` matérialisée, index à vide, PK + FK) | ~490–560 s | 19 Go |
| v2 (vue `_stg_item`, index différés, PK + FK) | 434 s | 11 Go |
| v3 (+ staging dans une base attachée jetable) — `--strict` | 434 s | ~7 Go |
| **v3 défaut : PK seules** (FK retirées) | **297 s** | **5,2 Go** |
| v3 `--fast` : aucune contrainte | 207 s | 4,3 Go |

Entrée : 1000 fichiers, **34 Go de JSON**. Sortie : 66 346 428 lignes `playlist_track`,
2,26 M `track`, 734 684 `album`, 295 860 `artist`, 1 M `playlist`.

Les 3 modes passent les **4 contrôles d'intégrité** de `load_mpd.py`
(`num_tracks` = COUNT réel ; FK `playlist_track→track`, `track→artist`, `track→album`).
Le défaut (PK seules) et `--fast` ne les *imposent* pas au moteur mais les *vérifient
après coup* — suffisant pour un entrepôt immuable et reconstructible.

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

## 2 bis. Temps de traitement par fichier

`db/per_file_timing.py` mesure fichier par fichier (donc **sérialisé**, connexion
en mémoire) le coût *parse JSON + `unnest(playlists)` + `unnest(tracks)`*.
Données par fichier : `db/per_file_timing.csv` (1000 lignes).

| Statistique (par fichier) | Valeur |
|---|---:|
| minimum | 354 ms |
| **moyenne** | **395 ms** |
| médiane | 389 ms |
| p95 | 432 ms |
| maximum | 1044 ms (1 valeur isolée) |
| écart-type | 38 ms (≈ 10 %) |
| débit | ~85 Mo/s par fichier |

```
350-375 ms  ▏████████████████                          177
375-400 ms  ▏██████████████████████████████████████████ 575
400-425 ms  ▏█████████████████                          187
425-450 ms  ▏███                                         28
450-500 ms  ▏█                                           15
   >500 ms  ▏██                                          18
```

- **Distribution très resserrée** : les slices sont quasi-uniformes
  (33,5 Mo ± 5 %, 66 346 pistes ± 5 %), donc les temps aussi.
- **Le temps par fichier ne dépend quasiment pas du contenu** :
  `corr(temps, taille) = 0,19`, `corr(temps, nb pistes) = 0,20`.
  Il est dominé par le coût fixe d'amorçage du parseur JSON (ouverture, inférence
  de schéma sur le fichier, allocation).
- **Total sérialisé : 395 s** pour les 1000 fichiers. Le pipeline réel passe le
  glob à un seul `read_json` qui **parallélise sur les 11 threads** → l'étape
  `_stg_playlist` complète prend ~30 s (et non 395 s) : le coût marginal réel
  d'un fichier est de l'ordre de **30 ms de temps mur**.
- Les 8 fichiers les plus lents (565–1044 ms) ne se distinguent ni par la taille
  ni par le nombre de pistes : ce sont des aléas d'ordonnancement / GC.

---

## 3. Ce qui a été appliqué

| Optimisation | Effet mesuré (éch. 200) | Effet plein (34 Go) |
|---|---|---|
| **`_stg_item` en VUE** au lieu d'une table matérialisée | 85,7 → 73,8 s (−14 %) | évite ~6 Go d'écriture de staging |
| **Index secondaires créés en fin de chargement** (sur table pleine) | 73,8 → 68,8 s (−7 %) | `INSERT playlist_track` : 42 → 32 s puis index en 8 s |
| **Staging dans une base `.duckdb` attachée jetable** (pas dans la base finale) | 3 168 → 2 033 Mo (−36 %), temps inchangé | fichier final **11 → ~7 Go** (`--strict`) — plus d'espace mort |
| **FK retirées par défaut** (PK conservées ; intégrité revalidée après coup) | phase `INSERT` 40 → 13 s | **434 → 297 s (−32 %)**, **~7 → 5,2 Go** |

> DuckDB ne rend jamais au système l'espace d'une table supprimée (pas de `VACUUM`
> compactant). Tant que le staging vivait dans le fichier final, `DROP` laissait
> l'équivalent de tout le dataset en pages mortes (v1 : ~8 Go). Le staging attaché
> jetable supprime le problème à la racine.

## 3 bis. Configuration DuckDB — testée, **sans gain**

Matrice sur l'échantillon 200 slices (pipeline optimisé, `db/bench_ingest.py`
et script ad hoc). Aucun réglage ne sort du bruit (~67 s ± 2) :

| Réglage | Total | Fichier |
|---|---:|---:|
| défauts (référence) | 67,4 s | 3 168 Mo |
| `checkpoint_threshold` 16 Mo → 8 Go | 69,4 s | 3 165 Mo |
| `preserve_insertion_order = false` | 69,1 s | 3 170 Mo |
| `allocator_background_threads = true` | 69,5 s | 3 162 Mo |
| `storage_compatibility_version = latest` | 67,0 s | 3 165 Mo |

- `default_block_size` : **déjà 256 Ko** (le maximum) en DuckDB 1.5.5 — l'ancien
  défaut 16 Ko n'existe plus, rien à changer.
- `threads` = 11 (= cœurs) déjà optimal ; en réduire ralentit, en ajouter n'aide pas.
- `memory_limit` 14 Gio : RSS max ~3,7 Go, **aucun spill**, `temp_directory` jamais
  sollicité.
- `read_json(sample_size = …)` : `-1` / `1024` / explicite → < 0,5 s d'écart.
  `columns =` explicites : −0,3 s, non rentable vs la maintenance du schéma inline.

**Conclusion : les défauts de DuckDB sont déjà adaptés à un chargement analytique
en masse ; il n'y a rien à régler côté `PRAGMA`/`SET`.** Les gains sont tous
structurels (§3).

---

## 4. Pistes restantes, par impact

1. **Parsing JSON = plancher, désormais dominant** (~210 s à froid, ~135 s cache OS
   chaud, sur 34 Go). Une fois les FK retirées c'est ~70 % du temps.
   → convertir une fois les slices en **Parquet** (`COPY (SELECT unnest(playlists)…)
   TO 'stg.parquet'`) puis charger depuis Parquet : le parse retombe à ~2–3× plus
   rapide. Rentable seulement si on recharge souvent (la conversion coûte ~un parse).
   `read_json` parallélise déjà sur les 1000 fichiers ; le SSD est ~saturé en lecture.

2. **Clé de substitution `INTEGER` pour `playlist_track`** (étape schéma en étoile).
   66 M lignes de `VARCHAR(22)` → un `track_key INTEGER` diviserait la table de faits
   par ~3 et accélérerait chargement (comparaisons d'entiers) et jointures. Prochain
   gain **structurel** ; arrive naturellement avec l'étoile.

3. **Chaîne « une seule passe »** : plusieurs `INSERT` partageant un même scan de
   `_stg_playlist` plutôt que de le relire 3×. Gain attendu faible (le scan d'une
   table déjà en base est rapide) — à mesurer si besoin.

4. **`--fast` pour les rechargements de travail** : 207 s / 4,3 Go, contre 297 s /
   5,2 Go en défaut. On ne perd que la PK composite de `playlist_track` (l'ordre
   des pistes reste porté par la colonne `position`).

---

## 5. Reproduire

```bash
# banc d'essai : variantes sur un échantillon
.venv/bin/python db/bench_ingest.py -n 200 --tag baseline
.venv/bin/python db/bench_ingest.py -n 200 --fast --tag fast
.venv/bin/python db/bench_ingest.py -n 200 --fast --no-stg-item --tag fast_novue

# temps de traitement fichier par fichier  ->  db/per_file_timing.csv
.venv/bin/python db/per_file_timing.py

# chargement réel (résumé + contrôles d'intégrité)
.venv/bin/python db/load_mpd.py            # défaut : PK seules      (~5 min, 5,2 Go)
.venv/bin/python db/load_mpd.py --strict   # PK + FK imposées        (~7 min, 7 Go)
.venv/bin/python db/load_mpd.py --fast     # aucune contrainte       (~3,5 min, 4,3 Go)
```
