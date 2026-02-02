{{
    config(
        materialized='table',
        schema='silver'
    )
}}

/*
    Date Dimension Table
    - Derived from departure dates
    - Includes date attributes for analysis
*/

with departure_dates as (
    select distinct
        -- Parse date from string (format: 'YYYY-MM-DD HH:MM:SS')
        cast(split_part(departure_datetime, ' ', 1) as date) as full_date
    from {{ ref('stg_flight_bookings') }}
    where departure_datetime is not null
      and departure_datetime != ''
),

date_attributes as (
    select
        full_date,
        to_char(full_date, 'YYYYMMDD')::int as date_key,
        extract(year from full_date)::int as year,
        extract(month from full_date)::int as month,
        extract(day from full_date)::int as day,
        extract(quarter from full_date)::int as quarter,
        extract(dow from full_date)::int as day_of_week,
        to_char(full_date, 'Day') as day_name,
        to_char(full_date, 'Month') as month_name,
        
        -- Weekend flag
        case 
            when extract(dow from full_date) in (0, 6) then true 
            else false 
        end as is_weekend,
        
        -- Season classification for Bangladesh
        case
            when extract(month from full_date) in (11, 12, 1, 2) then 'Winter'
            when extract(month from full_date) in (3, 4, 5) then 'Summer'
            when extract(month from full_date) in (6, 7, 8, 9, 10) then 'Monsoon'
            else 'Unknown'
        end as weather_season
        
    from departure_dates
)

select
    date_key,
    full_date,
    year,
    month,
    day,
    quarter,
    day_of_week,
    trim(day_name) as day_name,
    trim(month_name) as month_name,
    is_weekend,
    weather_season,
    current_timestamp as created_at
from date_attributes
order by full_date
