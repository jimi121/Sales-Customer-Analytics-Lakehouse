{{ config(
    materialized='incremental', 
    unique_key=['sls_ord_num', 'sls_prd_key'], 
    incremental_strategy='merge'
) }}

SELECT  
  TRIM(sls_ord_num) AS sls_ord_num,
  TRIM(sls_prd_key) AS sls_prd_key,
  sls_cust_id,

  CASE 
    WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS STRING)) != 8 
    THEN NULL 
    ELSE TO_DATE(CAST(sls_order_dt AS STRING), 'yyyyMMdd') 
  END AS sls_order_dt,

  CASE 
    WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS STRING)) != 8 
    THEN NULL 
    ELSE TO_DATE(CAST(sls_ship_dt AS STRING), 'yyyyMMdd') 
  END AS sls_ship_dt,

  CASE 
    WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS STRING)) != 8 
    THEN NULL 
    ELSE TO_DATE(CAST(sls_due_dt AS STRING), 'yyyyMMdd') 
  END AS sls_due_dt,

  sls_sales,
  sls_quantity,
  sls_price,
  ingestion_timestamp
  --CURRENT_TIMESTAMP() AS dwh_create_date

FROM {{ ref('bronze_crm_sales_details') }}

{% if is_incremental() %}
  WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}