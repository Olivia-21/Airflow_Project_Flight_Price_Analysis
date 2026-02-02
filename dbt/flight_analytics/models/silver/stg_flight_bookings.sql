{{
    config(
        materialized='view',
        schema='silver'
    )
}}

/*
    Staging model for flight bookings
    - Validates and cleans data from Bronze layer
    - Handles missing/null values
    - Removes duplicates
    - Casts data types appropriately
*/

with bronze_data as (
    select * from {{ source('bronze', 'raw_flight_data') }}
),

-- Data cleaning and validation
cleaned_data as (
    select
        id as bronze_id,
        
        -- String columns - trim and handle nulls
        coalesce(trim(airline), 'Unknown') as airline,
        coalesce(trim(source_code), 'UNK') as source_code,
        coalesce(trim(source_name), 'Unknown Airport') as source_name,
        coalesce(trim(destination_code), 'UNK') as destination_code,
        coalesce(trim(destination_name), 'Unknown Airport') as destination_name,
        
        -- Datetime columns - keep as string for now, parse in transformation
        trim(departure_datetime) as departure_datetime,
        trim(arrival_datetime) as arrival_datetime,
        
        -- Numeric columns - validate and handle nulls
        coalesce(duration_hours, 0) as duration_hours,
        coalesce(trim(stopovers), 'Unknown') as stopovers,
        coalesce(trim(aircraft_type), 'Unknown') as aircraft_type,
        
        -- Booking Class - standardize naming (First Class → First)
        case
            when lower(trim(booking_class)) in ('first class', 'first') then 'First'
            when lower(trim(booking_class)) in ('business class', 'business') then 'Business'
            when lower(trim(booking_class)) in ('economy class', 'economy') then 'Economy'
            else coalesce(trim(booking_class), 'Economy')
        end as booking_class,
        
        coalesce(trim(booking_source), 'Unknown') as booking_source,
        
        -- Fare columns - ensure positive values
        coalesce(base_fare_bdt, 0) as base_fare_bdt,
        coalesce(tax_surcharge_bdt, 0) as tax_surcharge_bdt,
        coalesce(total_fare_bdt, 0) as total_fare_bdt,
        
        -- Seasonality - standardize naming (remove 'Holidays' suffix)
        case
            when lower(trim(seasonality)) like '%winter%' then 'Winter'
            when lower(trim(seasonality)) like '%eid%' then 'Eid'
            when lower(trim(seasonality)) like '%hajj%' then 'Hajj'
            when lower(trim(seasonality)) = 'regular' then 'Regular'
            else coalesce(trim(seasonality), 'Regular')
        end as seasonality,
        
        coalesce(days_before_departure, 0) as days_before_departure,
        
        -- Metadata
        ingested_at,
        current_timestamp as processed_at
        
    from bronze_data
    where 
        -- Basic validation: exclude completely invalid records
        airline is not null
        and source_code is not null
        and destination_code is not null
),

-- Calculate total_fare if missing or incorrect
fare_validated as (
    select
        *,
        -- Recalculate total_fare if it doesn't match base + tax
        case 
            when total_fare_bdt = 0 or total_fare_bdt < base_fare_bdt
            then base_fare_bdt + tax_surcharge_bdt
            else total_fare_bdt
        end as validated_total_fare_bdt
    from cleaned_data
),

-- Remove duplicates based on key columns
deduplicated as (
    select
        *,
        row_number() over (
            partition by 
                airline, 
                source_code, 
                destination_code, 
                departure_datetime,
                booking_class,
                base_fare_bdt
            order by bronze_id
        ) as rn
    from fare_validated
)

select
    bronze_id,
    airline,
    source_code,
    source_name,
    destination_code,
    destination_name,
    departure_datetime,
    arrival_datetime,
    duration_hours,
    stopovers,
    aircraft_type,
    booking_class,
    booking_source,
    base_fare_bdt,
    tax_surcharge_bdt,
    validated_total_fare_bdt as total_fare_bdt,
    seasonality,
    days_before_departure,
    ingested_at,
    processed_at
from deduplicated
where rn = 1
    -- Additional validation: exclude records with invalid values
    and duration_hours >= 0
    and duration_hours <= {{ var('max_duration_threshold', 48) }}
    and base_fare_bdt >= {{ var('min_fare_threshold', 0) }}
    and validated_total_fare_bdt > 0
