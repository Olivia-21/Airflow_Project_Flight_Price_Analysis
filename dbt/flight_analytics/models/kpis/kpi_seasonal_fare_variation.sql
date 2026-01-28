{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    KPI: Seasonal Fare Variation
    - Compares average fares during peak vs non-peak seasons
    - Peak seasons: Eid ul-Fitr, Eid ul-Adha, Winter Holidays
*/

with bookings as (
    select * from {{ ref('fct_flight_bookings') }}
),

-- Classify seasons as peak or non-peak
season_classification as (
    select
        *,
        case 
            when seasonality in {{ var('peak_seasons', "('Eid ul-Fitr', 'Eid ul-Adha', 'Winter Holidays')") }}
            then 'Peak'
            else 'Non-Peak'
        end as season_type
    from bookings
),

-- Aggregate by seasonality
by_seasonality as (
    select
        seasonality,
        season_type,
        
        count(*) as total_bookings,
        
        -- Average fares
        round(avg(total_fare_bdt)::numeric, 2) as avg_total_fare_bdt,
        round(avg(base_fare_bdt)::numeric, 2) as avg_base_fare_bdt,
        round(avg(tax_surcharge_bdt)::numeric, 2) as avg_tax_surcharge_bdt,
        
        -- Fare range
        round(min(total_fare_bdt)::numeric, 2) as min_fare_bdt,
        round(max(total_fare_bdt)::numeric, 2) as max_fare_bdt,
        
        -- Booking lead time
        round(avg(days_before_departure)::numeric, 1) as avg_days_before_departure
        
    from season_classification
    group by seasonality, season_type
),

-- Aggregate by season type (peak vs non-peak)
by_season_type as (
    select
        season_type,
        
        sum(total_bookings) as total_bookings,
        round(avg(avg_total_fare_bdt)::numeric, 2) as weighted_avg_fare,
        round(min(min_fare_bdt)::numeric, 2) as overall_min_fare,
        round(max(max_fare_bdt)::numeric, 2) as overall_max_fare
        
    from by_seasonality
    group by season_type
),

-- Calculate fare variation
fare_comparison as (
    select
        max(case when season_type = 'Peak' then weighted_avg_fare end) as peak_avg_fare,
        max(case when season_type = 'Non-Peak' then weighted_avg_fare end) as non_peak_avg_fare,
        max(case when season_type = 'Peak' then total_bookings end) as peak_bookings,
        max(case when season_type = 'Non-Peak' then total_bookings end) as non_peak_bookings
    from by_season_type
)

-- Final output: detailed seasonal breakdown
select
    s.seasonality,
    s.season_type,
    s.total_bookings,
    s.avg_total_fare_bdt,
    s.avg_base_fare_bdt,
    s.avg_tax_surcharge_bdt,
    s.min_fare_bdt,
    s.max_fare_bdt,
    s.avg_days_before_departure,
    
    -- Comparison to non-peak
    round((s.avg_total_fare_bdt - fc.non_peak_avg_fare)::numeric, 2) as fare_diff_from_non_peak,
    round(((s.avg_total_fare_bdt - fc.non_peak_avg_fare) / nullif(fc.non_peak_avg_fare, 0) * 100)::numeric, 2) as fare_pct_diff_from_non_peak,
    
    -- Overall stats
    fc.peak_avg_fare as overall_peak_avg_fare,
    fc.non_peak_avg_fare as overall_non_peak_avg_fare,
    round(((fc.peak_avg_fare - fc.non_peak_avg_fare) / nullif(fc.non_peak_avg_fare, 0) * 100)::numeric, 2) as peak_vs_non_peak_pct_diff,
    
    current_timestamp as calculated_at

from by_seasonality s
cross join fare_comparison fc
order by s.season_type desc, s.total_bookings desc
