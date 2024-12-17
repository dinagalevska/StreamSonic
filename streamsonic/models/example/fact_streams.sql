{{ config(
  materialized = 'table',
  partition_by={
    "field": "ts",
    "data_type": "timestamp",
    "granularity": "hour"  -- You can use 'day' if it's more appropriate for your use case
  }
) }}

WITH enriched_streams AS (
    SELECT
        le.userId AS UserKey,
        s.songId AS SongKey,
        a.artistId AS ArtistKey,
        l.locationId AS LocationKey,
        dt.datetime AS DateKey,
        se.sessionId AS SessionKey,
        le.ts AS Timestamp,
        CAST(le.duration AS DOUBLE) AS duration,
        1 AS streamCount,
        CASE WHEN CAST(le.duration AS DOUBLE) < 30 THEN 1 ELSE 0 END AS isSongSkipped,
        CASE 
            WHEN LAG(le.lat) OVER (PARTITION BY le.userId ORDER BY le.ts) = le.lat AND 
                 LAG(le.lon) OVER (PARTITION BY le.userId ORDER BY le.ts) = le.lon 
            THEN 0 ELSE 1 
        END AS isMoving,
        CASE WHEN LAG(le.ts) OVER (PARTITION BY le.userId ORDER BY le.ts) IS NULL THEN 1 ELSE 0 END AS isFirstListenEvent,
        CASE WHEN LEAD(le.ts) OVER (PARTITION BY le.userId ORDER BY le.ts) IS NULL THEN 1 ELSE 0 END AS isLastListenEvent,
        UNIX_TIMESTAMP(LEAD(le.ts) OVER (PARTITION BY le.userId ORDER BY le.ts)) - UNIX_TIMESTAMP(le.ts) AS nextListenEventTimeGap,
        EXTRACT(YEAR FROM le.ts) AS year,
        EXTRACT(MONTH FROM le.ts) AS month,
        EXTRACT(DAY FROM le.ts) AS day,
        EXTRACT(HOUR FROM le.ts) AS hour,
        EXTRACT(MINUTE FROM le.ts) AS minute,
        EXTRACT(SECOND FROM le.ts) AS second
    FROM 
        {{ source('staging', 'listen_events') }} le
    LEFT JOIN {{ ref('dim_songs') }} s ON s.song = le.song AND s.artist = le.artist
    LEFT JOIN {{ ref('dim_artists') }} a ON a.artist = le.artist
    LEFT JOIN {{ ref('dim_location') }} l ON l.city = le.city AND l.state = le.state
    LEFT JOIN {{ ref('dim_datetime') }} dt ON DATE_TRUNC('hour', le.ts) = dt.datetime
    LEFT JOIN {{ ref('dim_sessions') }} se ON se.sessionId = le.sessionId
)

SELECT DISTINCT
    UserKey,
    SongKey,
    ArtistKey,
    LocationKey,
    DateKey,
    SessionKey,
    Timestamp,
    duration,
    streamCount,
    isSongSkipped,
    isMoving,
    isFirstListenEvent,
    isLastListenEvent,
    nextListenEventTimeGap,
    year,
    month,
    day,
    hour,
    minute,
    second
FROM enriched_streams;
