{# {{config(materialized = 'table')}} #}

{{ config(
    materialized='incremental',
    unique_key='cid',
    incremental_strategy='merge',
    schema='bronze',
    description='Bronze layer for raw customer extras from ERP system, incrementally loaded'
) }}

SELECT
  *
FROM {{ source('raw_data', 'customer_extras') }}
{% if is_incremental() %}
  WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}