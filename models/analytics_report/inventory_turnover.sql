{{ config(materialized='view') }}

WITH base AS (
    SELECT
        f.product_key,
        p.category,
        p.subcategory,
        p.product_line,
        DATE_TRUNC('month', f.order_date) AS month,
        SUM(f.sales_amount) AS total_sales,
        SUM(f.quantity) AS total_qty,
        AVG(p.product_cost) AS avg_cost
    FROM {{ ref('gold_fact_sales') }} f
    LEFT JOIN {{ ref('gold_dim_products') }} p 
        ON f.product_key = p.product_key
    GROUP BY 1, 2, 3, 4, 5
),

-- Estimate average inventory based on a rolling window of recent months
rolling AS (
    SELECT
        *,
        AVG(total_qty) OVER (
            PARTITION BY product_key ORDER BY month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS avg_inventory
    FROM base
)

SELECT
    *,
    CASE WHEN avg_inventory = 0 THEN 0 
         ELSE total_qty / avg_inventory END AS inventory_turnover,
    CASE WHEN avg_inventory = 0 THEN NULL
         ELSE (30 / (total_qty / avg_inventory)) END AS days_of_supply
FROM rolling;
