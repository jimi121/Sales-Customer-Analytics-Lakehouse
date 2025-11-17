{{ config(materialized='view') }}

with base AS (
    SELECT
        f.order_date,
        f.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.product_line,
        SUM(f.quantity) AS total_quantity,
        SUM(f.sales_amount) AS total_sales,
        AVG(f.price) AS avg_price,
        AVG(p.product_cost) AS avg_cost
    FROM
        {{ ref('gold_fact_sales') }} f
    JOIN
        {{ ref('gold_dim_products') }} p
    ON f.product_key = p.product_key
    GROUP BY
        f.order_date,
        f.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.product_line
),

promo_detect AS (
    SELECT
        *,
        LAG(avg_price) OVER(PARTITION by product_key ORDER BY order_date) AS prev_price,
        CASE
            WHEN avg_price < prev_price * 0.95 THEN 1 ELSE 0
        END AS promo_flag
    FROM base
),

elasticity AS (
    SELECT
        *,
        LAG(total_quantity) OVER(PARTITION BY product_key ORDER BY order_date) AS prev_qty,
        CASE
            WHEN prev_qty is NULL OR prev_qty = 0 THEN NULL
            ELSE ROUND( ( (total_quantity - prev_qty) / prev_qty ) /
                        ( (avg_price - prev_price) / prev_price ), 2)
        END AS price_elasticity
    FROM promo_detect
)

SELECT
    order_date,
    product_key,
    product_name,
    category,
    subcategory,
    product_line,
    total_sales,
    total_quantity,
    avg_price,
    avg_cost,
    promo_flag,
    price_elasticity
FROM elasticity;