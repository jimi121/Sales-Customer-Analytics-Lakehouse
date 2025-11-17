{{ config(
    materialized='incremental',
    unique_key='prd_id', 
    incremental_strategy='merge'
) }}

SELECT 
  prd_id,
  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
  SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS sls_prd_key,
  prd_nm,
  COALESCE(prd_cost, 0) AS prd_cost,
  CASE UPPER(TRIM(prd_line)) WHEN 'M' THEN 'Mountain' 
                             WHEN 'R' THEN 'Road' 
                             WHEN 'S' THEN 'Other Sales' 
                             WHEN 'T' THEN 'Touring' 
                             ELSE 'Unknown' 
  END AS prd_line,
  prd_start_dt,
  LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1' DAY AS prd_end_dt,
  ingestion_timestamp
  --CURRENT_TIMESTAMP() AS dwh_create_date
FROM {{ ref('bronze_crm_prd_info') }}
{% if is_incremental() %}
  WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}
