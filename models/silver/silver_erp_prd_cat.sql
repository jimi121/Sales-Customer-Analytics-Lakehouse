{{ config(
    materialized='incremental', 
    unique_key='id', 
    incremental_strategy='merge'
) }}

SELECT
  TRIM(id) AS id,
  TRIM(cat) AS cat,
  TRIM(subcat) AS subcat,
  TRIM(maintenance) AS maintenance,
  CURRENT_TIMESTAMP() AS dwh_create_date
FROM {{ ref('bronze_erp_prd_cat') }}
{% if is_incremental() %}
  WHERE ingestion_timestamp > (SELECT MAX(dwh_create_date) FROM {{ this }})
{% endif %}