-- ================================================================
--  MPD - Modele Physique de Donnees  (cible : DuckDB >= 1.0)
--  Source : Spotify Million Playlist Dataset (dossier ./data)
--  Forme  : schema relationnel normalise (3NF)
--  Cles   : colonnes *_uri = identifiant base62 Spotify (partie apres
--           le dernier ':' de l'URI). URI complete reconstructible :
--           'spotify:<type>:' || <valeur>.
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
    artist_uri   VARCHAR PRIMARY KEY,      -- id base62 (22 c.) ; URI = 'spotify:artist:' || artist_uri
    artist_name  VARCHAR NOT NULL
);

-- Un album / single. FD verifiee : album_uri -> album_name (0 incoherence)
-- Pas de FK vers artist : 38 235 albums sont multi-artistes (compilations).
CREATE TABLE album (
    album_uri    VARCHAR PRIMARY KEY,      -- URI = 'spotify:album:' || album_uri
    album_name   VARCHAR NOT NULL
);

-- Un enregistrement precis (une piste sur une sortie donnee).
-- FD verifiee : track_uri -> (artist_uri, album_uri, track_name, duration_ms), 0 incoherence.
CREATE TABLE track (
    track_uri    VARCHAR PRIMARY KEY,      -- URI = 'spotify:track:' || track_uri
    track_name   VARCHAR NOT NULL,
    duration_ms  INTEGER NOT NULL,        -- duree de la piste
    artist_uri   VARCHAR NOT NULL REFERENCES artist(artist_uri),
    album_uri    VARCHAR NOT NULL REFERENCES album(album_uri)
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
    track_uri  VARCHAR NOT NULL REFERENCES track(track_uri),
    PRIMARY KEY (pid, position)
);

-- ----------------------------------------------------------------
--  Index analytiques  (les PRIMARY KEY / FOREIGN KEY portent deja
--  leur propre index cote DuckDB ; on ajoute les acces "inverses").
--  Crees APRES le chargement par db/build.sql (§7) : batir un index
--  une fois sur table pleine est ~2x plus rapide que le maintenir
--  a chaque INSERT.
-- ----------------------------------------------------------------
--  ix_track_artist_uri          ON track(artist_uri)
--  ix_track_album_uri           ON track(album_uri)
--  ix_playlist_track_track_uri  ON playlist_track(track_uri)
