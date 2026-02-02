{{
    config(
        materialized='table',
        schema='silver'
    )
}}

/*
    Route Dimension Table
    - Contains unique source-destination pairs with surrogate keys
*/

with routes as (
    select distinct
        source_code,
        source_name,
        destination_code,
        destination_name
    from {{ ref('stg_flight_bookings') }}
    where source_code is not null
      and destination_code is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['source_code', 'destination_code']) }} as route_key,
    source_code,
    source_name,
    destination_code,
    destination_name,
    source_code || '-' || destination_code as route_code,
    source_name || ' to ' || destination_name as route_description,
    current_timestamp as created_at,
    current_timestamp as updated_at
from routes
order by source_code, destination_code
