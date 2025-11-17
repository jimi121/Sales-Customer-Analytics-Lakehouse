{{ config(
    materialized='incremental', 
    unique_key='cst_id', 
    incremental_strategy='merge'
) }}

WITH deduped AS (
  SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE UPPER(TRIM(cst_marital_status)) WHEN 'S' THEN 'Single' WHEN 'M' THEN 'Married' ELSE 'Unknown' END AS cst_marital_status,
    CASE UPPER(TRIM(cst_gndr)) WHEN 'F' THEN 'Female' WHEN 'M' THEN 'Male' ELSE 'Unknown' END AS cst_gndr,
    COALESCE(cst_create_date, CURRENT_DATE()) AS cst_create_date, -- Impute nulls
    ingestion_timestamp,
    --CURRENT_TIMESTAMP() AS dwh_create_date,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rnk
  FROM {{ ref('bronze_crm_cust_info') }}
  WHERE cst_id IS NOT NULL -- Also filter for cst_id
  {% if is_incremental() %}
    AND ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
  {% endif %}
)
SELECT
  cst_id,
  cst_key,
  cst_firstname,
  cst_lastname,
  cst_marital_status,
  cst_gndr,
  cst_create_date,
  ingestion_timestamp
FROM deduped
WHERE rnk = 1