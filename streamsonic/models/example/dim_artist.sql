{{ config(materialized = 'table') }}

-- This DBT model implements a Slowly Changing Dimension (SCD) Type 2 for tracking artist data (location, name, etc.)
-- The model ensures that changes in an artist's data (like location) are recorded with an active row flag and expiration date.

WITH artist_changes AS (
    -- Step 1: Identify changes in artist data, ordered by the timestamp of events
    SELECT 
        artist AS artistName,
        lat AS latitude,
        lon AS longitude,
        city AS location,
        state,
        ts AS eventTimestamp,
        LEAD(ts, 1, '9999-12-31') OVER (PARTITION BY artist ORDER BY ts) AS nextEventTimestamp,
        -- Create a flag indicating if any change has occurred for the artist
        CASE 
            WHEN LAG(artist, 1, 'NA') OVER (PARTITION BY artist ORDER BY ts) <> artist
               OR LAG(lat, 1, NULL) OVER (PARTITION BY artist ORDER BY ts) <> lat
               OR LAG(lon, 1, NULL) OVER (PARTITION BY artist ORDER BY ts) <> lon
               OR LAG(city, 1, NULL) OVER (PARTITION BY artist ORDER BY ts) <> city
               OR LAG(state, 1, NULL) OVER (PARTITION BY artist ORDER BY ts) <> state THEN 1
            ELSE 0 
        END AS artistChanged
    FROM {{ source('staging', 'listen_events') }}
    WHERE artist IS NOT NULL -- Exclude null artist data, which may be invalid or system data
),

-- Step 2: Aggregate the results and assign rowActivationDate and rowExpirationDate
artist_history AS (
    SELECT 
        artistName,
        latitude,
        longitude,
        location,
        state,
        eventTimestamp AS rowActivationDate,
        nextEventTimestamp AS rowExpirationDate,
        CASE 
            WHEN nextEventTimestamp = '9999-12-31' THEN 1 
            ELSE 0 
        END AS currentRow
    FROM artist_changes
    WHERE artistChanged = 1 -- Only include rows where the artist data has changed
),

-- Step 3: Combine with the initial state (for artists that haven't had any changes)
initial_artist_state AS (
    SELECT 
        artist AS artistName,
        lat AS latitude,
        lon AS longitude,
        city AS location,
        state,
        CAST(MIN(ts) AS DATE) AS rowActivationDate,
        '9999-12-31' AS rowExpirationDate,
        1 AS currentRow
    FROM {{ source('staging', 'listen_events') }}
    WHERE artist NOT IN (SELECT DISTINCT artist FROM artist_changes) -- Artists who haven't changed
    GROUP BY artist, lat, lon, city, state
)

-- Step 4: Combine historical data and initial states, with proper handling of row expiration dates
SELECT 
    {{ dbt_utils.surrogate_key(['artistName', 'rowActivationDate']) }} AS artistKey, 
    artistName,
    latitude,
    longitude,
    location,
    state,
    rowActivationDate,
    rowExpirationDate,
    currentRow
FROM artist_history

UNION ALL

SELECT 
    {{ dbt_utils.surrogate_key(['artistName', 'rowActivationDate']) }} AS artistKey, 
    artistName,
    latitude,
    longitude,
    location,
    state,
    rowActivationDate,
    rowExpirationDate,
    currentRow
FROM initial_artist_state
