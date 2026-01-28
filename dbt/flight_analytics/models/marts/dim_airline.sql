{{
    config(
        materialized='table',
        schema='silver'
    )
}}

/*
    Airline Dimension Table
    - Contains unique airlines with surrogate keys
*/

with airlines as (
    select distinct airline
    from {{ ref('stg_flight_bookings') }}
    where airline is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['airline']) }} as airline_key,
    airline as airline_name,
    current_timestamp as created_at,
    current_timestamp as updated_at
from airlines
order by airline
