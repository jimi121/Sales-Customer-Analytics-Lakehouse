{# {{ config(materialized='incremental', unique_key='product_key', incremental_strategy='merge') }} #}

{{ config (materialized = 'table') }}

SELECT
  ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.sls_prd_key) AS product_key,
  pn.prd_id AS product_id,
  pn.sls_prd_key AS product_number,
  pn.prd_nm AS product_name,
  pn.cat_id AS category_id,
  pc.cat AS category,
  pc.subcat AS subcategory,
  pc.maintenance AS maintenance,
  pn.prd_cost AS product_cost,
  pn.prd_line AS product_line,
  pn.prd_start_dt AS start_date
FROM {{ ref('silver_crm_prd_info') }} pn
LEFT JOIN {{ ref('silver_erp_prd_cat') }} pc ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL
{# {% if is_incremental() %}
  AND pn.dwh_create_date > (SELECT MAX(start_date) FROM {{ this }})
{% endif %} #}