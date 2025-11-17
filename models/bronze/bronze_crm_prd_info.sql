{# {{ config(materialized='table') }} #}
{# {{ config(materialized='incremental', unique_key='prd_id', incremental_strategy='merge') }} #}

{{ config(
    materialized='incremental',
    unique_key='prd_id',
    incremental_strategy='merge',
    schema='bronze',
    description='Bronze layer for raw product info from CRM system, incrementally loaded'
) }}

SELECT
  *
FROM {{ source('raw_data', 'product_information') }}
{# WHERE prd_id IS NOT NULL #}
{% if is_incremental() %}
  WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}