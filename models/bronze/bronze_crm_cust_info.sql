{# {{config(materialized = 'table')}} #}
{# {{ config(materialized='incremental', unique_key='cst_id', incremental_strategy='merge') }} #}

{{ config(
    materialized='incremental',
    unique_key='cst_id',
    incremental_strategy='merge',
    schema='bronze',
    description='Bronze layer for raw customer info from CRM system, incrementally loaded'
) }}
-- pre_hook='DROP TABLE IF EXISTS {{ this }}'
SELECT
    *
    --CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM {{source('raw_data', 'customer_information')}}
{# WHERE cst_id IS NOT NULL #}
{% if is_incremental() %}
  WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}