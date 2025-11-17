{{ config(materialized='view') }}

WITH base AS (
    SELECT
        f.order_date,
        year(f.order_date) AS order_year,
        month(f.order_date) AS order_month,
        f.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        SUM(f.quantity) AS total_quantity,
        SUM(f.sales_amount) AS total_sales,
        avg(f.price) AS avg_price,
        COUNT(DISTINCT f.order_number) AS orders_count
    FROM
        {{ref("gold_fact_sales")}} f
    LEFT JOIN
        {{ref("gold_dim_products")}} p
    ON f.product_key = p.product_key
    GROUP BY
        f.order_date,
        year(f.order_date),
        month(f.order_date),
        f.product_key,
        p.product_name,
        p.category,
        p.subcategory
),

rolling_avg AS (
    SELECT
        product_key,
        order_date,
        order_year,
        order_month,
        product_name,
        category,
        subcategory,
        total_quantity,
        total_sales,
        avg_price,
        orders_count,
        AVG(total_quantity) OVER (
            PARTITION BY product_key
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_qty
    FROM base
)

SELECT
    product_key,
    product_name,
    category,
    subcategory,
    order_date,
    order_year,
    order_month,
    total_quantity,
    total_sales,
    avg_price,
    orders_count,
    rolling_7d_qty,
    CASE
        WHEN total_quantity = 0 AND rolling_7d_qty > 0 THEN 1
        ELSE 0
    END AS stockout_flag,
    CASE
        WHEN rolling_7d_qty = 0 THEN NULL
        ELSE ROUND((total_quantity - rolling_7d_qty) / rolling_7d_qty * 100, 2)
    END AS demand_variance_pct
FROM rolling_avg