{# 
{{ config(
    materialized='incremental',
    unique_key=['order_number', 'product_key'],
    incremental_strategy='merge'
) }}
#}

{{ config (materialized = 'table') }}

SELECT
  sd.sls_ord_num AS order_number,
  pr.product_key,
  cu.customer_key,
  sd.sls_cust_id AS customer_id,
  sd.sls_order_dt AS order_date,
  sd.sls_ship_dt AS shipping_date,
  sd.sls_due_dt AS due_date,
  sd.sls_sales AS sales_amount,
  sd.sls_quantity AS quantity,
  sd.sls_price AS price
FROM {{ ref('silver_crm_sales_details') }} sd
LEFT JOIN {{ ref('gold_dim_products') }} pr ON pr.product_number = sd.sls_prd_key
LEFT JOIN {{ ref('gold_dim_customers') }} cu ON cu.customer_id = sd.sls_cust_id

{# -- {{% if is_incremental() %}
--  WHERE sd.dwh_create_date > (SELECT MAX(order_date) FROM {{ this }})
-- {% endif %}} #}