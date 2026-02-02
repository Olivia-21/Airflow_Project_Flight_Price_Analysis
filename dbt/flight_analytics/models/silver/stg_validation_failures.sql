{{
    config(
        materialized='table',
        schema='silver'
    )
}}

/*
    Validation failures audit table
    - Logs rows that failed Silver layer validation
    - Useful for data quality monitoring
*/

with bronze_data as (
    select * from {{ source('bronze', 'raw_flight_data') }}
),

-- Identify validation failures
validation_failures as (
    select
        id as bronze_id,
        airline,
        source_code,
        destination_code,
        departure_datetime,
        duration_hours,
        base_fare_bdt,
        tax_surcharge_bdt,
        total_fare_bdt,
        
        -- Failure reasons
        case
            when airline is null then 'missing_airline'
            when source_code is null then 'missing_source'
            when destination_code is null then 'missing_destination'
            when duration_hours < 0 then 'negative_duration'
            when duration_hours > {{ var('max_duration_threshold', 48) }} then 'excessive_duration'
            when base_fare_bdt < 0 then 'negative_base_fare'
            when total_fare_bdt <= 0 and (base_fare_bdt + coalesce(tax_surcharge_bdt, 0)) <= 0 then 'invalid_total_fare'
            else 'unknown_failure'
        end as failure_reason,
        
        current_timestamp as detected_at
        
    from bronze_data
    where
        -- Records that would fail validation
        airline is null
        or source_code is null
        or destination_code is null
        or duration_hours < 0
        or duration_hours > {{ var('max_duration_threshold', 48) }}
        or base_fare_bdt < 0
        or (total_fare_bdt <= 0 and (base_fare_bdt + coalesce(tax_surcharge_bdt, 0)) <= 0)
)

select * from validation_failures
