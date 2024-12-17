-- This is a dbt model for creating a DateTime Dimension table
{{ config(materialized = 'table') }}

-- First, let's generate a series of hourly timestamps from 2020-01-01 00:00:00 to 2024-12-31 23:59:59
WITH datetime_series AS (
  SELECT
    TIMESTAMP('2020-01-01 00:00:00 UTC') + INTERVAL x HOUR AS datetime
  FROM
    UNNEST(GENERATE_ARRAY(0, 43823)) AS x  -- 43824 hours in 5 years from Jan 1, 2020 to Dec 31, 2024
)

-- Now, let's create the DateTime dimension table with all the required transformations
SELECT
    -- Primary key based on date (DateSK)
    CAST(FORMAT_TIMESTAMP('%Y%m%d', datetime) AS INT64) AS DateSK,
    -- Year
    EXTRACT(YEAR FROM datetime) AS Year,
    -- Quarter
    EXTRACT(QUARTER FROM datetime) AS Quarter,
    -- Month
    EXTRACT(MONTH FROM datetime) AS Month,
    -- Day
    EXTRACT(DAY FROM datetime) AS Day,
    -- Week (week of the year)
    EXTRACT(WEEK FROM datetime) AS Week,
    -- Quarter Name (Long format)
    CASE EXTRACT(QUARTER FROM datetime)
        WHEN 1 THEN 'Q1'
        WHEN 2 THEN 'Q2'
        WHEN 3 THEN 'Q3'
        WHEN 4 THEN 'Q4'
    END AS QuarterNameLong,
    -- Quarter Name (Short format)
    CASE EXTRACT(QUARTER FROM datetime)
        WHEN 1 THEN 'Q1'
        WHEN 2 THEN 'Q2'
        WHEN 3 THEN 'Q3'
        WHEN 4 THEN 'Q4'
    END AS QuarterNameShort,
    -- Quarter Number (String format)
    CAST(EXTRACT(QUARTER FROM datetime) AS STRING) AS QuarterNumberString,
    -- Month Name (Long format)
    FORMAT_TIMESTAMP('%B', datetime) AS MonthNameLong,
    -- Month Name (Short format)
    FORMAT_TIMESTAMP('%b', datetime) AS MonthNameShort,
    -- Month Number (String format)
    FORMAT_TIMESTAMP('%m', datetime) AS MonthNumberString,
    -- Day Number (String format)
    FORMAT_TIMESTAMP('%d', datetime) AS DayNumberString,
    -- Week Name (Long format)
    CONCAT('week', LPAD(CAST(EXTRACT(WEEK FROM datetime) AS STRING), 2, '0')) AS WeekNameLong,
    -- Week Name (Short format)
    CONCAT('w', LPAD(CAST(EXTRACT(WEEK FROM datetime) AS STRING), 2, '0')) AS WeekNameShort,
    -- Week Number (String format)
    LPAD(CAST(EXTRACT(WEEK FROM datetime) AS STRING), 2, '0') AS WeekNumberString,
    -- Day of the Week (1 = Sunday, 7 = Saturday)
    EXTRACT(DAYOFWEEK FROM datetime) AS DayOfWeek,
    -- Year/Month String
    FORMAT_TIMESTAMP('%Y/%m', datetime) AS YearMonthString,
    -- Day of the Week Name (Long format)
    FORMAT_TIMESTAMP('%A', datetime) AS DayOfWeekNameLong,
    -- Day of the Week Name (Short format)
    FORMAT_TIMESTAMP('%a', datetime) AS DayOfWeekNameShort,
    -- Day of the Month
    EXTRACT(DAY FROM datetime) AS DayOfMonth,
    -- Day of the Year
    EXTRACT(DAYOFYEAR FROM datetime) AS DayOfYear,
    -- Hour
    EXTRACT(HOUR FROM datetime) AS Hour,
    -- Minute
    EXTRACT(MINUTE FROM datetime) AS Minute,
    -- Second
    EXTRACT(SECOND FROM datetime) AS Second
FROM
  datetime_series
