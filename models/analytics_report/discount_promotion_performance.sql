{{ config(materialized='view') }}

WITH base AS (
    SELECT
        f.order_number,
        f.order_date,
        f.product_key,
        f.sales_amount,
        f.quantity,
        f.price,
        p.product_cost,
        p.category,
        p.subcategory,
        p.product_line
    FROM {{ ref('gold_fact_sales') }} f
    LEFT JOIN {{ ref('gold_dim_products') }} p 
        ON f.product_key = p.product_key
),

calc AS (
    SELECT
        product_key,
        category,
        subcategory,
        product_line,
        DATE_TRUNC('month', order_date) AS month,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_qty,
        SUM(quantity * p.product_cost) AS total_cost,
        AVG(price) AS avg_selling_price,
        AVG(p.product_cost) AS avg_cost_price,
        (AVG(p.product_cost) * 1.2) AS list_price_estimate, -- assumption: 20% markup base
        (AVG(p.product_cost) * 1.2) - AVG(price) AS discount_amount
    FROM base p
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    *,
    CASE WHEN list_price_estimate = 0 THEN 0 
         ELSE (discount_amount / list_price_estimate) * 100 END AS discount_pct,
    (total_sales - total_cost) AS gross_profit,
    CASE WHEN total_sales = 0 THEN 0 
         ELSE ((total_sales - total_cost) / total_sales) * 100 END AS margin_pct
FROM calc;
