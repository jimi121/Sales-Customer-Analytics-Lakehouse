{# {{ config(materialized='incremental', unique_key='customer_key', incremental_strategy='merge') }} #}

{{ config (materialized = 'table') }}

SELECT
  ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
  ci.cst_id AS customer_id,
  ci.cst_key AS customer_number,
  ci.cst_firstname AS first_name,
  ci.cst_lastname AS last_name,
  la.cntry AS country,
  ci.cst_marital_status AS marital_status,
  CASE WHEN ci.cst_gndr = 'Unknown' THEN COALESCE(ca.gen, 'Unknown') ELSE ci.cst_gndr END AS gender,
  ca.bdate AS birth_date,
  ci.cst_create_date AS create_date
FROM {{ ref('silver_crm_cust_info') }} ci
LEFT JOIN {{ ref('silver_erp_cust_extras') }} ca ON ci.cst_key = ca.cid
LEFT JOIN {{ ref('silver_erp_cust_loc') }} la ON ci.cst_key = la.cid
{# {% if is_incremental() %}
  WHERE ci.dwh_create_date > (SELECT MAX(create_date) FROM {{ this }})
{% endif %} #}