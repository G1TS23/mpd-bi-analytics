-- ================================================================
--  ETL  Spotify Million Playlist Dataset  ->  schema relationnel
--  A executer APRES db/schema.sql, sur la meme base DuckDB.
--
--  Le jeton @SRC@ est remplace par db/load_mpd.py :
--    - chargement complet  : '<data>/mpd.slice.*.json'
--    - echantillon (-n N)  : ['<data>/mpd.slice.a.json', ...]
--  Pour lancer ce fichier a la main, remplacer @SRC@ par le glob.
--
--  Choix perf (voir db/PERFORMANCE.md) :
--   - le JSON n'est parse qu'UNE fois -> table _stg_playlist (pistes
--     encore imbriquees) ;
--   - _stg_item est une VUE, pas une table : evite d'ecrire ~1 Go
--     (200 slices) / ~6 Go (tout) de staging intermediaire sur disque ;
--   - les index secondaires sont crees en fin de chargement (etape 7).
-- ================================================================

-- 1. Staging : une ligne par playlist, pistes encore imbriquees ------
CREATE OR REPLACE TABLE _stg_playlist AS
SELECT pl.*
FROM (
    SELECT unnest(playlists) AS pl
    FROM read_json(@SRC@,
                   maximum_object_size = 200000000,   -- fichiers ~34 Mo
                   sample_size = -1)                   -- schema infere sur tout
);

-- 2. Vue : une ligne par piste de playlist (playlist x position) -----
--    Recalculee a chaque lecture (l'unnest depuis _stg_playlist deja
--    en base coute moins que materialiser+checkpointer 66 M lignes).
CREATE OR REPLACE VIEW _stg_item AS
SELECT pid, unnest(tracks, recursive := true)
FROM _stg_playlist;

-- 3. Dimensions ------------------------------------------------------
--    split_part(uri, ':', 3) = id base62 ('spotify:track:XXXX' -> 'XXXX').
--    Les colonnes *_uri de _stg_item portent l'URI complete.

INSERT INTO artist
SELECT DISTINCT split_part(artist_uri, ':', 3), artist_name
FROM _stg_item;

INSERT INTO album
SELECT DISTINCT split_part(album_uri, ':', 3), album_name
FROM _stg_item;

-- FD garantie (verifiee a l'analyse) => DISTINCT sur les 5 colonnes
-- produit exactement une ligne par track_uri.
INSERT INTO track
SELECT DISTINCT
       split_part(track_uri,  ':', 3),
       track_name,
       duration_ms,
       split_part(artist_uri, ':', 3),
       split_part(album_uri,  ':', 3)
FROM _stg_item;

-- 4. Playlist ------------------------------------------------------
INSERT INTO playlist
SELECT
    pid,
    name,
    collaborative = 'true',                       -- -> BOOLEAN
    epoch_ms(modified_at * 1000)::DATE,           -- epoch s -> DATE (sans fuseau)
    nullif(trim(description), ''),                -- '' -> NULL
    num_followers,
    num_edits,
    num_tracks,
    num_albums,
    num_artists,
    duration_ms
FROM _stg_playlist;

-- 5. Association --------------------------------------------------
INSERT INTO playlist_track
SELECT pid, pos, split_part(track_uri, ':', 3)
FROM _stg_item;

-- 6. Nettoyage staging ------------------------------------------
DROP VIEW  _stg_item;
DROP TABLE _stg_playlist;

-- 7. Index secondaires (batis une fois sur tables pleines) ------
CREATE INDEX ix_track_artist_uri         ON track(artist_uri);
CREATE INDEX ix_track_album_uri          ON track(album_uri);
CREATE INDEX ix_playlist_track_track_uri ON playlist_track(track_uri);
