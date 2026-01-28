{{
    config(
        materialized='table',
        schema='silver'
    )
}}

/*
    Flight Bookings Fact Table
    - Central fact table joining dimension keys with measures
    - Contains transactional booking data
*/

with staged_bookings as (
    select * from {{ ref('stg_flight_bookings') }}
),

dim_airlines as (
    select * from {{ ref('dim_airline') }}
),

dim_routes as (
    select * from {{ ref('dim_route') }}
),

dim_dates as (
    select * from {{ ref('dim_date') }}
),

-- Join staging data with dimensions
fact_data as (
    select
        -- Surrogate keys
        a.airline_key,
        r.route_key,
        d.date_key,
        
        -- Degenerate dimensions (booking details)
        s.bronze_id as booking_id,
        s.booking_class,
        s.booking_source,
        s.stopovers,
        s.aircraft_type,
        s.departure_datetime,
        s.arrival_datetime,
        
        -- Measures
        s.duration_hours,
        s.base_fare_bdt,
        s.tax_surcharge_bdt,
        s.total_fare_bdt,
        s.days_before_departure,
        
        -- Seasonality (from source data)
        s.seasonality,
        
        -- Derived measures
        case 
            when s.base_fare_bdt > 0 
            then round((s.tax_surcharge_bdt / s.base_fare_bdt * 100)::numeric, 2)
            else 0 
        end as tax_percentage,
        
        -- Booking lead time category
        case
            when s.days_before_departure <= 7 then 'Last Minute'
            when s.days_before_departure <= 14 then 'Short Notice'
            when s.days_before_departure <= 30 then 'Standard'
            when s.days_before_departure <= 60 then 'Advance'
            else 'Early Bird'
        end as booking_lead_category,
        
        -- Metadata
        s.ingested_at,
        s.processed_at,
        current_timestamp as created_at
        
    from staged_bookings s
    left join dim_airlines a on s.airline = a.airline_name
    left join dim_routes r on s.source_code = r.source_code 
                           and s.destination_code = r.destination_code
    left join dim_dates d on cast(split_part(s.departure_datetime, ' ', 1) as date) = d.full_date
)

select * from fact_data
