{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    KPI: Booking Count by Airline
    - Total bookings per airline with various breakdowns
    - Includes rankings and market share
*/

with bookings as (
    select * from {{ ref('fct_flight_bookings') }}
),

airlines as (
    select * from {{ ref('dim_airline') }}
),

-- Main booking counts by airline
airline_booking_counts as (
    select
        a.airline_name as airline,
        
        -- Total bookings
        count(*) as total_bookings,
        
        -- Bookings by class
        count(*) filter (where b.booking_class = 'Economy') as economy_bookings,
        count(*) filter (where b.booking_class = 'Business') as business_bookings,
        count(*) filter (where b.booking_class = 'First Class') as first_class_bookings,
        
        -- Bookings by source
        count(*) filter (where b.booking_source = 'Online Website') as online_bookings,
        count(*) filter (where b.booking_source = 'Travel Agency') as agency_bookings,
        count(*) filter (where b.booking_source = 'Direct Booking') as direct_bookings,
        
        -- Bookings by lead time
        count(*) filter (where b.booking_lead_category = 'Last Minute') as last_minute_bookings,
        count(*) filter (where b.booking_lead_category = 'Early Bird') as early_bird_bookings,
        
        -- Bookings by seasonality
        count(*) filter (where b.seasonality in ('Eid ul-Fitr', 'Eid ul-Adha', 'Winter Holidays')) as peak_season_bookings,
        
        -- Revenue metrics
        round(sum(b.total_fare_bdt)::numeric, 2) as total_revenue_bdt,
        round(avg(b.total_fare_bdt)::numeric, 2) as avg_fare_bdt

    from bookings b
    join airlines a on b.airline_key = a.airline_key
    group by a.airline_name
)

select
    airline,
    total_bookings,
    
    -- Booking rank
    rank() over (order by total_bookings desc) as booking_rank,
    
    -- Market share
    round((total_bookings::float / sum(total_bookings) over () * 100)::numeric, 2) as market_share_pct,
    
    -- Class breakdown
    economy_bookings,
    business_bookings,
    first_class_bookings,
    round((economy_bookings::float / nullif(total_bookings, 0) * 100)::numeric, 1) as economy_pct,
    round((business_bookings::float / nullif(total_bookings, 0) * 100)::numeric, 1) as business_pct,
    round((first_class_bookings::float / nullif(total_bookings, 0) * 100)::numeric, 1) as first_class_pct,
    
    -- Source breakdown
    online_bookings,
    agency_bookings,
    direct_bookings,
    
    -- Lead time breakdown
    last_minute_bookings,
    early_bird_bookings,
    
    -- Peak season bookings
    peak_season_bookings,
    round((peak_season_bookings::float / nullif(total_bookings, 0) * 100)::numeric, 1) as peak_season_pct,
    
    -- Revenue metrics
    total_revenue_bdt,
    avg_fare_bdt,
    rank() over (order by total_revenue_bdt desc) as revenue_rank,
    
    current_timestamp as calculated_at

from airline_booking_counts
order by total_bookings desc
