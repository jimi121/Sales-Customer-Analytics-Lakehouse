{{ config(materialized='view') }}

WITH base AS (
    SELECT
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        f.price,
        p.product_name,
        p.product_line,
        p.category,
        p.maintenance,
        p.product_cost
    FROM {{ ref('gold_fact_sales') }} f
    LEFT JOIN {{ ref('gold_dim_products') }} p 
        ON f.product_key = p.product_key
),

calc AS (
    SELECT
        product_key,
        product_name,
        product_line,
        category,
        maintenance,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        SUM(quantity * p.product_cost) AS total_cost,
        SUM(sales_amount) - SUM(quantity * p.product_cost) AS gross_profit,
        CASE 
            WHEN SUM(sales_amount) = 0 THEN NULL
            ELSE (SUM(sales_amount) - SUM(quantity * p.product_cost)) / SUM(sales_amount)
        END AS gross_margin_pct
    FROM base p
    GROUP BY 
        product_key, 
        product_name, 
        product_line, 
        category, 
        maintenance
)

SELECT
    *,
    CASE 
        WHEN gross_margin_pct < 0.05 THEN 'Unprofitable'
        WHEN gross_margin_pct BETWEEN 0.05 AND 0.15 THEN 'Low Margin'
        WHEN gross_margin_pct BETWEEN 0.15 AND 0.3 THEN 'Healthy Margin'
        ELSE 'High Margin'
    END AS margin_band
FROM calc;
