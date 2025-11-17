{# {{config (materialized = 'table')}} #}

{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    schema='bronze',
    description='Bronze layer for raw product categories from ERP system, incrementally loaded'
) }}

SELECT
    *
FROM {{ source('raw_data', 'product_category') }}
{% if is_incremental() %}
  WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}