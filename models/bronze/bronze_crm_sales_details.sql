{# {{ config(materialized='table') }} #}
{# {{ config (materialized = 'incremental', unique_key=['sls_ord_num', 'sls_prd_key'], incremental_strategy='merge') }} #}

{{ config(
    materialized='incremental',
    unique_key= ['sls_ord_num', 'sls_prd_key'],
    incremental_strategy='merge',
    schema='bronze',
    description='Bronze layer for raw sales details from CRM system, incrementally loaded'
) }}

SELECT
  *
FROM {{ source('raw_data', 'sales_details') }}
{% if is_incremental() %}
  WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}