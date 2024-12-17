{{ config(materialized = 'table') }}

-- This DBT model implements a Slowly Changing Dimension (SCD) Type 2 for tracking changes in user level (free to paid and vice versa).
-- The model maintains a history of each user's level and ensures that each level change is recorded with an active row flag and expiration date.

WITH user_level_changes AS (
    -- Step 1: Identify the changes in level for each user, ordered by the timestamp of events
    SELECT 
        userId,
        firstName,
        lastName,
        gender,
        level,
        CAST(registration AS BIGINT) AS registration,
        ts AS eventTimestamp,
        LEAD(ts, 1, '9999-12-31') OVER (PARTITION BY userId ORDER BY ts) AS nextEventTimestamp,
        -- Create a group based on changes in the user's level (e.g., free -> paid, paid -> free)
        CASE 
            WHEN LAG(level, 1, 'NA') OVER (PARTITION BY userId ORDER BY ts) <> level THEN 1 
            ELSE 0 
        END AS levelChanged
    FROM {{ source('staging', 'listen_events') }}
    WHERE userId <> 0 -- Exclude users with userId = 0, which may be invalid or system users
),

-- Step 2: Aggregate the results to assign rowActivationDate and rowExpirationDate
user_level_history AS (
    SELECT 
        userId,
        firstName,
        lastName,
        gender,
        level,
        registration,
        eventTimestamp AS rowActivationDate,
        nextEventTimestamp AS rowExpirationDate,
        -- Create the current row flag to mark the most recent active row for each user
        CASE 
            WHEN nextEventTimestamp = '9999-12-31' THEN 1 
            ELSE 0 
        END AS currentRow
    FROM user_level_changes
    WHERE levelChanged = 1 -- Only include rows where the user's level has changed
),

-- Step 3: Combine with the initial user state (for users that haven't changed their level yet)
initial_user_state AS (
    SELECT 
        userId,
        firstName,
        lastName,
        gender,
        level,
        registration,
        CAST(MIN(ts) AS DATE) AS rowActivationDate,
        '9999-12-31' AS rowExpirationDate,
        1 AS currentRow
    FROM {{ source('staging', 'listen_events') }}
    WHERE userId NOT IN (SELECT DISTINCT userId FROM user_level_changes) -- Users who haven't had any level change
    GROUP BY userId, firstName, lastName, gender, level, registration
)

-- Step 4: Combine historical data and initial states, with proper handling of row expiration dates
SELECT 
    {{ dbt_utils.surrogate_key(['userId', 'rowActivationDate', 'level']) }} AS userKey, 
    userId,
    firstName,
    lastName,
    gender,
    level,
    registration,
    rowActivationDate,
    rowExpirationDate,
    currentRow
FROM user_level_history

UNION ALL

SELECT 
    {{ dbt_utils.surrogate_key(['userId', 'rowActivationDate', 'level']) }} AS userKey, 
    userId,
    firstName,
    lastName,
    gender,
    level,
    registration,
    rowActivationDate,
    rowExpirationDate,
    currentRow
FROM initial_user_state
