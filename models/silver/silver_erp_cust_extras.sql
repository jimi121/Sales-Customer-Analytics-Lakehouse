{{ config(
    materialized='incremental',
    unique_key='cid', 
    incremental_strategy='merge'
) }}

SELECT  
  CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) ELSE cid END AS cid,
  CASE WHEN bdate > TO_DATE('2025-05-06', 'yyyy-MM-dd') THEN NULL ELSE bdate END AS bdate,
  CASE UPPER(TRIM(gen)) WHEN 'F' THEN 'Female' WHEN 'M' THEN 'Male' ELSE 'Unknown' END AS gen,
  CURRENT_TIMESTAMP() AS dwh_create_date
FROM {{ ref('bronze_erp_cust_extras') }}
{% if is_incremental() %}
  WHERE ingestion_timestamp > (SELECT MAX(dwh_create_date) FROM {{ this }})
{% endif %}