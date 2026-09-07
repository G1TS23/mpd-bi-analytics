-- ================================================================
--  ETL  Spotify Million Playlist Dataset  ->  schema relationnel
--  A executer APRES db/schema.sql, sur la meme base DuckDB.
--
--  Jetons remplaces par db/load_mpd.py :
--    @SRC@  source read_json  : '<data>/mpd.slice.*.json'  (ou liste ['..','..'])
--    @STG@  base de staging   : '<db>.stg'  (fichier .duckdb jetable, attache)
--  Pour lancer a la main : remplacer @SRC@ par le glob et @STG@ par un
--  chemin temporaire (ex. '/tmp/mpd_stg.duckdb').
--
--  Choix perf (voir db/PERFORMANCE.md) :
--   - le JSON n'est parse qu'UNE fois -> table _stg_playlist ;
--   - le staging vit dans une base ATTACHEE jetable, pas dans la base
--     finale : celle-ci ne porte jamais l'espace mort du staging
--     (DuckDB ne compacte pas apres DROP)  -> fichier final ~2x plus petit ;
--   - _stg_item est une VUE (pas de 2e materialisation de 66 M lignes) ;
--   - index secondaires crees en fin de chargement (etape 7).
-- ================================================================

ATTACH '@STG@' AS stg;

-- 1. Staging : une ligne par playlist, pistes encore imbriquees ------
CREATE OR REPLACE TABLE stg._stg_playlist AS
SELECT pl.*
FROM (
    SELECT unnest(playlists) AS pl
    FROM read_json(@SRC@,
                   maximum_object_size = 200000000,   -- fichiers ~34 Mo
                   sample_size = -1)                   -- schema infere sur tout
);

-- 2. Vue : une ligne par piste de playlist (playlist x position) -----
CREATE OR REPLACE VIEW _stg_item AS
SELECT pid, unnest(tracks, recursive := true)
FROM stg._stg_playlist;

-- 3. Dimensions ------------------------------------------------------
--    split_part(uri, ':', 3) = id base62 ('spotify:track:XXXX' -> 'XXXX').

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
FROM stg._stg_playlist;

-- 5. Association --------------------------------------------------
INSERT INTO playlist_track
SELECT pid, pos, split_part(track_uri, ':', 3)
FROM _stg_item;

-- 6. Liberation du staging ------------------------------------
DROP VIEW _stg_item;
DETACH stg;                       -- le fichier @STG@ est supprime par load_mpd.py

-- 7. Index secondaires (batis une fois sur tables pleines) ------
CREATE INDEX ix_track_artist_uri         ON track(artist_uri);
CREATE INDEX ix_track_album_uri          ON track(album_uri);
CREATE INDEX ix_playlist_track_track_uri ON playlist_track(track_uri);
