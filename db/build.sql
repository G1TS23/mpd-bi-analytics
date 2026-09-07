-- ================================================================
--  ETL  Spotify Million Playlist Dataset  ->  schema relationnel
--  A executer APRES db/schema.sql, sur la meme base DuckDB.
--
--  Le jeton @SRC@ est remplace par db/load_mpd.py :
--    - chargement complet  : '<data>/mpd.slice.*.json'
--    - echantillon (-n N)  : ['<data>/mpd.slice.a.json', ...]
--  Pour lancer ce fichier a la main, remplacer @SRC@ par le glob.
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

-- 2. Staging : une ligne par piste de playlist (playlist x position) --
CREATE OR REPLACE TABLE _stg_item AS
SELECT pid, unnest(tracks, recursive := true)
FROM _stg_playlist;

-- 3. Dimensions ------------------------------------------------------
--    split_part(uri, ':', 3) = id base62 ('spotify:track:XXXX' -> 'XXXX')

INSERT INTO artist
SELECT DISTINCT split_part(artist_uri, ':', 3), artist_name
FROM _stg_item;

INSERT INTO album
SELECT DISTINCT split_part(album_uri, ':', 3), album_name
FROM _stg_item;

-- FD garantie (verifiee a l'analyse) => DISTINCT sur les 5 colonnes
-- produit exactement une ligne par track_id.
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

-- 6. Nettoyage --------------------------------------------------
DROP TABLE _stg_item;
DROP TABLE _stg_playlist;
