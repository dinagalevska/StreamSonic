{{ config(
    materialized='table'  -- Create a table for the session dimension
) }}

WITH sessions AS (
    SELECT 
        sessionId,
        MIN(ts) AS startTime,  -- Start time of the session (earliest timestamp)
        MAX(ts) AS endTime     -- End time of the session (latest timestamp)
    FROM {{ source('staging', 'listen_events') }}  -- Replace with your actual source
    GROUP BY sessionId
)

SELECT
    sessionId,
    CAST(startTime AS STRING) AS startTime,  -- Start time as string
    CAST(endTime AS STRING) AS endTime       -- End time as string
FROM sessions
