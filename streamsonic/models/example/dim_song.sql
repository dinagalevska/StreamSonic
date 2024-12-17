{{ config(materialized='table') }}

WITH base_song_data AS (
    SELECT
        {{ dbt_utils.surrogate_key(['song', 'artist']) }} AS songId, -- Generate a unique identifier for each song
        song,
        artist,
        duration
    FROM {{ source('staging', 'listen_events') }}
),

deduplicated_songs AS (
    SELECT DISTINCT
        songId,
        song,
        artist,
        duration
    FROM base_song_data
)

SELECT
    songId,
    song,
    artist,
    duration
FROM deduplicated_songs;
