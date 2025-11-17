{{ config(materialized='view') }}

WITH base AS (
    SELECT
        p.product_key,
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        f.order_date,
        f.order_number,
        f.sales_amount,
        f.quantity,
        f.price
    FROM 
        {{ ref("gold_fact_sales") }} f
    JOIN 
        {{ ref("gold_dim_products") }} p ON f.product_key = p.product_key
),

-- Aggregate sales by date and product
agg AS (
    SELECT
        product_key,
        product_name,
        category,
        subcategory,
        DATE_TRUNC('month', order_date) AS order_month,
        SUM(sales_amount) AS monthly_sales,
        SUM(quantity) AS monthly_qty,
        AVG(price) AS avg_price
    FROM base
    GROUP BY 1,2,3,4,5
),

-- Category-level total to compute share and cannibalization
category_sales AS (
    SELECT
        category,
        order_month,
        SUM(monthly_sales) AS total_category_sales
    FROM agg
    GROUP BY 1,2
)

SELECT
    a.product_key,
    a.product_name,
    a.category,
    a.subcategory,
    a.order_month,
    a.monthly_sales,
    a.monthly_qty,
    a.avg_price,
    c.total_category_sales,
    ROUND((a.monthly_sales / NULLIF(c.total_category_sales, 0)) * 100, 2) AS product_sales_share_pct
FROM agg a
JOIN category_sales c
    ON a.category = c.category AND a.order_month = c.order_month;