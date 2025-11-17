{{ config(
    materialized='incremental', 
    unique_key='cid', 
    incremental_strategy='merge'
) }}

SELECT
  REPLACE(cid, '-', '') AS cid,
  CASE WHEN UPPER(TRIM(cntry)) IN ('USA', 'US', 'UNITED STATUS') THEN 'United States' 
       WHEN TRIM(cntry) IN ('DE') THEN 'Germany' 
       WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'Unknown' 
       ELSE cntry 
  END AS cntry,
  CURRENT_TIMESTAMP() AS dwh_create_date
FROM {{ ref('bronze_erp_cust_loc') }}
{% if is_incremental() %}
  WHERE ingestion_timestamp > (SELECT MAX(dwh_create_date) FROM {{ this }})
{% endif %}