{{ config(materialized='view') }}

WITH base AS (
    SELECT
        f.order_number,
        f.product_key,
        f.customer_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        f.price,
        p.product_name,
        p.category,
        p.subcategory,
        p.product_cost,
        c.country
    FROM {{ ref('gold_fact_sales') }} f
    LEFT JOIN {{ ref('gold_dim_products') }} p ON f.product_key = p.product_key
    LEFT JOIN {{ ref('gold_dim_customers') }} c ON f.customer_key = c.customer_key
),

calc AS (
    SELECT
        product_key,
        customer_key,
        category,
        subcategory,
        country,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        SUM(quantity * p.product_cost) AS total_cost,
        (SUM(sales_amount) - SUM(quantity * p.product_cost)) AS gross_profit,
        CASE 
            WHEN SUM(sales_amount) = 0 THEN NULL
            ELSE (SUM(sales_amount) - SUM(quantity * p.product_cost)) / SUM(sales_amount)
        END AS gross_margin_pct
    FROM base p
    GROUP BY 
        product_key, 
        customer_key, 
        category, 
        subcategory, 
        country
)

SELECT
    product_key,
    customer_key,
    category,
    subcategory,
    country,
    total_sales,
    total_quantity,
    total_cost,
    gross_profit,
    gross_margin_pct
FROM calc
