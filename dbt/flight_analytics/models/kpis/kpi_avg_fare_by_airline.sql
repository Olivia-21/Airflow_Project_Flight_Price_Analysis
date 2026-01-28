{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    KPI: Average Fare by Airline
    - Calculates average base fare, tax, and total fare by airline
    - Includes booking counts and fare statistics
*/

with bookings as (
    select * from {{ ref('fct_flight_bookings') }}
),

airlines as (
    select * from {{ ref('dim_airline') }}
),

airline_metrics as (
    select
        a.airline_name as airline,
        
        -- Booking counts
        count(*) as total_bookings,
        
        -- Average fares
        round(avg(b.base_fare_bdt)::numeric, 2) as avg_base_fare_bdt,
        round(avg(b.tax_surcharge_bdt)::numeric, 2) as avg_tax_surcharge_bdt,
        round(avg(b.total_fare_bdt)::numeric, 2) as avg_total_fare_bdt,
        
        -- Fare statistics
        round(min(b.total_fare_bdt)::numeric, 2) as min_fare_bdt,
        round(max(b.total_fare_bdt)::numeric, 2) as max_fare_bdt,
        round(stddev(b.total_fare_bdt)::numeric, 2) as fare_std_dev,
        
        -- Median fare (approximate using percentile)
        round(percentile_cont(0.5) within group (order by b.total_fare_bdt)::numeric, 2) as median_fare_bdt,
        
        -- Average tax percentage
        round(avg(b.tax_percentage)::numeric, 2) as avg_tax_percentage,
        
        -- Average duration
        round(avg(b.duration_hours)::numeric, 2) as avg_duration_hours,
        
        -- Fare per hour (value metric)
        round((avg(b.total_fare_bdt) / nullif(avg(b.duration_hours), 0))::numeric, 2) as avg_fare_per_hour,
        
        -- Market share (will be calculated in outer query)
        count(*)::float as booking_count_for_share

    from bookings b
    join airlines a on b.airline_key = a.airline_key
    group by a.airline_name
)

select
    airline,
    total_bookings,
    avg_base_fare_bdt,
    avg_tax_surcharge_bdt,
    avg_total_fare_bdt,
    min_fare_bdt,
    max_fare_bdt,
    fare_std_dev,
    median_fare_bdt,
    avg_tax_percentage,
    avg_duration_hours,
    avg_fare_per_hour,
    
    -- Market share percentage
    round((booking_count_for_share / sum(booking_count_for_share) over () * 100)::numeric, 2) as market_share_pct,
    
    -- Ranking
    rank() over (order by avg_total_fare_bdt desc) as fare_rank_desc,
    rank() over (order by total_bookings desc) as popularity_rank,
    
    current_timestamp as calculated_at

from airline_metrics
order by total_bookings desc
