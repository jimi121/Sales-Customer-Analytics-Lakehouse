{{ config(materialized='view') }}

WITH sales AS (
    SELECT
        f.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.product_cost,
        p.product_line,
        SUM(f.sales_amount) AS total_sales,
        SUM(f.quantity) AS total_quantity,
        SUM(f.sales_amount) - SUM(f.quantity * p.product_cost) AS gross_profit,
        (SUM(f.sales_amount) - SUM(f.quantity * p.product_cost)) / COALESCE(SUM(f.sales_amount), 0) AS gross_margin_pct,
        AVG(f.price) AS avg_price,
        CURRENT_DATE() AS snapshot_date
    FROM {{ ref('gold_fact_sales') }} f
    LEFT JOIN {{ ref('gold_dim_products') }} p ON f.product_key = p.product_key
    GROUP BY 
        f.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.product_cost,
        p.product_line
),

category_profit AS (
    SELECT
        category,
        SUM(total_sales) AS category_sales,
        SUM(gross_profit) AS category_profit,
        AVG(gross_margin_pct) AS avg_margin
    FROM sales
    GROUP BY
        category
)

SELECT
    s.*,
    c.category_sales,
    c.category_profit,
    c.avg_margin AS category_avg_margin
FROM sales s
LEFT JOIN category_profit c USING (category);
