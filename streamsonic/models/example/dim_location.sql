{{ config(
    materialized = 'table'
) }}

WITH location_data AS (
    -- Get the raw data and generate unique location keys
    SELECT
        -- Surrogate Key
        {{ dbt_utils.surrogate_key(['city', 'state', 'zip', 'lat', 'lon']) }} as locationKey,
        city,
        zip,
        state,
        lat,
        lon,
        ts
    FROM 
        {{ source('staging', 'listen_events') }}  -- Replace with your source
)

SELECT
    locationKey,
    city,
    zip,
    state,
    lat AS latitude,
    lon AS longitude,
    ts AS timestamp
FROM location_data
