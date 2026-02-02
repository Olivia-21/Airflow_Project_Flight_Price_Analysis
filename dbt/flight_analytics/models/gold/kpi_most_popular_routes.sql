{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    KPI: Most Popular Routes
    - Top source-destination pairs by booking count
    - Includes fare and duration metrics
*/

with bookings as (
    select * from {{ ref('fct_flight_bookings') }}
),

routes as (
    select * from {{ ref('dim_route') }}
),

-- Route metrics
route_metrics as (
    select
        r.source_code,
        r.source_name,
        r.destination_code,
        r.destination_name,
        r.route_code,
        r.route_description,
        
        -- Booking counts
        count(*) as total_bookings,
        
        -- Fare metrics
        round(avg(b.total_fare_bdt)::numeric, 2) as avg_fare_bdt,
        round(min(b.total_fare_bdt)::numeric, 2) as min_fare_bdt,
        round(max(b.total_fare_bdt)::numeric, 2) as max_fare_bdt,
        round(stddev(b.total_fare_bdt)::numeric, 2) as fare_std_dev,
        
        -- Duration metrics
        round(avg(b.duration_hours)::numeric, 2) as avg_duration_hours,
        round(min(b.duration_hours)::numeric, 2) as min_duration_hours,
        round(max(b.duration_hours)::numeric, 2) as max_duration_hours,
        
        -- Stopovers
        count(*) filter (where b.stopovers = 'Direct') as direct_flights,
        count(*) filter (where b.stopovers != 'Direct') as connecting_flights,
        
        -- Class breakdown
        count(*) filter (where b.booking_class = 'Economy') as economy_bookings,
        count(*) filter (where b.booking_class = 'Business') as business_bookings,
        count(*) filter (where b.booking_class = 'First Class') as first_class_bookings,
        
        -- Revenue
        round(sum(b.total_fare_bdt)::numeric, 2) as total_revenue_bdt,
        
        -- Average days before departure
        round(avg(b.days_before_departure)::numeric, 1) as avg_days_before_departure

    from bookings b
    join routes r on b.route_key = r.route_key
    group by r.source_code, r.source_name, r.destination_code, r.destination_name, 
             r.route_code, r.route_description
)

select
    source_code,
    source_name,
    destination_code,
    destination_name,
    route_code,
    route_description,
    
    -- Popularity metrics
    total_bookings,
    rank() over (order by total_bookings desc) as route_rank,
    round((total_bookings::float / sum(total_bookings) over () * 100)::numeric, 2) as market_share_pct,
    
    -- Fare metrics
    avg_fare_bdt,
    min_fare_bdt,
    max_fare_bdt,
    fare_std_dev,
    
    -- Duration metrics
    avg_duration_hours,
    min_duration_hours,
    max_duration_hours,
    
    -- Flight type breakdown
    direct_flights,
    connecting_flights,
    round((direct_flights::float / nullif(total_bookings, 0) * 100)::numeric, 1) as direct_flight_pct,
    
    -- Class breakdown
    economy_bookings,
    business_bookings,
    first_class_bookings,
    
    -- Revenue and business metrics
    total_revenue_bdt,
    rank() over (order by total_revenue_bdt desc) as revenue_rank,
    avg_days_before_departure,
    
    -- Value metrics
    round((avg_fare_bdt / nullif(avg_duration_hours, 0))::numeric, 2) as fare_per_hour,
    
    current_timestamp as calculated_at

from route_metrics
order by total_bookings desc
limit 50  -- Top 50 most popular routes
