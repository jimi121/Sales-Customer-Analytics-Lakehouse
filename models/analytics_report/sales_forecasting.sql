{{ config(materialized='view') }}

WITH base AS (
    SELECT
        f.order_date,
        f.sales_amount,
        f.quantity,
        p.product_key,
        p.category
    FROM {{ ref('gold_fact_sales') }} f
    LEFT JOIN {{ ref('gold_dim_products') }} p 
        ON f.product_key = p.product_key
),

agg AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        category,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity
    FROM base
    GROUP BY 1, 2
),

rolling AS (
    SELECT
        *,
        AVG(total_sales) OVER (
            PARTITION BY category ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3mo_sales,
        LAG(total_sales, 12) OVER (PARTITION BY category ORDER BY month) AS last_year_sales
    FROM agg
)

SELECT
    *,
    CASE WHEN last_year_sales IS NULL THEN NULL
         ELSE ((total_sales - last_year_sales) / last_year_sales) * 100
    END AS yoy_growth_pct
FROM rolling;
