-- ================================================================
--  MPD - Modele Physique de Donnees  (cible : DuckDB >= 1.0)
--  Source : Spotify Million Playlist Dataset (dossier ./data)
--  Forme  : schema relationnel normalise (3NF), cles = id base62 Spotify
--  Genere / maintenu avec  db/load_mpd.py
-- ================================================================

-- Ordre inverse des dependances pour l'idempotence
DROP TABLE IF EXISTS playlist_track;
DROP TABLE IF EXISTS playlist;
DROP TABLE IF EXISTS track;
DROP TABLE IF EXISTS album;
DROP TABLE IF EXISTS artist;

-- ----------------------------------------------------------------
--  Dimensions
-- ----------------------------------------------------------------

-- Un artiste (principal). FD verifiee : artist_uri -> artist_name (0 incoherence / 66 M lignes)
CREATE TABLE artist (
    artist_id    VARCHAR PRIMARY KEY,      -- id base62 (22 c.), URI = 'spotify:artist:' || artist_id
    artist_name  VARCHAR NOT NULL
);

-- Un album / single. FD verifiee : album_uri -> album_name (0 incoherence)
-- Pas de FK vers artist : 38 235 albums sont multi-artistes (compilations).
CREATE TABLE album (
    album_id     VARCHAR PRIMARY KEY,
    album_name   VARCHAR NOT NULL
);

-- Un enregistrement precis (une piste sur une sortie donnee).
-- FD verifiee : track_uri -> (artist_uri, album_uri, track_name, duration_ms), 0 incoherence.
CREATE TABLE track (
    track_id     VARCHAR PRIMARY KEY,
    track_name   VARCHAR NOT NULL,
    duration_ms  INTEGER NOT NULL,        -- duree de la piste
    artist_id    VARCHAR NOT NULL REFERENCES artist(artist_id),
    album_id     VARCHAR NOT NULL REFERENCES album(album_id)
);

-- ----------------------------------------------------------------
--  Entite principale
-- ----------------------------------------------------------------

CREATE TABLE playlist (
    pid            INTEGER PRIMARY KEY,     -- identifiant global fourni par la source (0 .. 999999)
    name           VARCHAR NOT NULL,        -- non unique
    collaborative  BOOLEAN NOT NULL,        -- source 'true'/'false'  -> booleen
    modified_at    DATE    NOT NULL,        -- source : epoch secondes UTC, granularite jour
    description    VARCHAR,                 -- NULL si absente (~98 % des playlists)
    -- agregats pre-calcules par la source, conserves (0 ecart constate avec le reel) :
    num_followers  INTEGER NOT NULL,
    num_edits      INTEGER NOT NULL,
    num_tracks     INTEGER NOT NULL,        -- = COUNT(playlist_track)
    num_albums     INTEGER NOT NULL,        -- = COUNT(DISTINCT album)
    num_artists    INTEGER NOT NULL,        -- = COUNT(DISTINCT artist)
    duration_ms    BIGINT  NOT NULL         -- = SUM(track.duration_ms)
);

-- ----------------------------------------------------------------
--  Association playlist x track  (entite faible : identification
--  relative a playlist par 'position' -- un meme track peut figurer
--  plusieurs fois dans une playlist : 881 652 cas).
-- ----------------------------------------------------------------

CREATE TABLE playlist_track (
    pid        INTEGER NOT NULL REFERENCES playlist(pid),
    position   INTEGER NOT NULL,            -- rang 0-indexe, dense (0 .. num_tracks-1)
    track_id   VARCHAR NOT NULL REFERENCES track(track_id),
    PRIMARY KEY (pid, position)
);

-- ----------------------------------------------------------------
--  Index analytiques  (les PRIMARY KEY / FOREIGN KEY portent deja
--  leur propre index cote DuckDB ; on ajoute les acces "inverses")
-- ----------------------------------------------------------------
CREATE INDEX ix_track_artist          ON track(artist_id);
CREATE INDEX ix_track_album           ON track(album_id);
CREATE INDEX ix_playlist_track_track  ON playlist_track(track_id);
