{{ config(materialized='view') }}

WITH base AS (
    SELECT
        f.order_number,
        f.order_date,
        f.product_key,
        f.customer_key,
        f.quantity,
        f.sales_amount,
        p.product_cost,
        p.category,
        c.country
    FROM {{ ref('gold_fact_sales') }} f
    LEFT JOIN {{ ref('gold_dim_products') }} p 
        ON f.product_key = p.product_key
    LEFT JOIN {{ ref('gold_dim_customers') }} c 
        ON f.customer_key = c.customer_key
),

calc AS (
    SELECT
        country,
        category,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        SUM(quantity * p.product_cost) AS total_cost,
        (SUM(sales_amount) - SUM(quantity * p.product_cost)) AS gross_profit,
        CASE WHEN SUM(quantity) = 0 THEN 0 ELSE 
            (SUM(sales_amount) - SUM(quantity * p.product_cost)) / SUM(quantity)
        END AS profit_per_unit
    FROM base p
    GROUP BY 
        country, 
        category
)

SELECT
    *,
    CASE 
        WHEN profit_per_unit >= 50 THEN 'High ROI'
        WHEN profit_per_unit BETWEEN 25 AND 50 THEN 'Moderate ROI'
        ELSE 'Low ROI'
    END AS allocation_priority
FROM calc;
