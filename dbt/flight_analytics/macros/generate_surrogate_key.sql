{#
    Custom macro to generate surrogate keys
    Falls back to dbt_utils if available
#}

{% macro generate_surrogate_key(field_list) %}
    {{ return(dbt_utils.generate_surrogate_key(field_list)) }}
{% endmacro %}
