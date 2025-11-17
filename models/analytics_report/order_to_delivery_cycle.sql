{{ config(materialized='view') }}

WITH base AS (
    SELECT
        f.order_number,
        f.customer_key,
        f.product_key,
        f.order_date,
        f.shipping_date,
        f.due_date,
        f.sales_amount,
        f.quantity,
        p.category,
        p.subcategory,
        c.country
    FROM {{ ref('gold_fact_sales') }} f
    LEFT JOIN {{ ref('gold_dim_products') }} p ON f.product_key = p.product_key
    LEFT JOIN {{ ref('gold_dim_customers') }} c ON f.customer_key = c.customer_key
),

cycle_metrics AS (
    SELECT
        order_number,
        country,
        category,
        subcategory,
        DATEDIFF(order_date, shipping_date) AS order_to_ship_days,
        DATEDIFF(shipping_date, due_date) AS ship_to_due_days,
        CASE WHEN shipping_date <= due_date THEN 1 ELSE 0 END AS on_time_flag,
        sales_amount,
        quantity,
        order_date
    FROM base
)

SELECT
    country,
    category,
    subcategory,
    AVG(order_to_ship_days) AS avg_order_to_ship_days,
    AVG(ship_to_due_days) AS avg_ship_to_due_days,
    SUM(on_time_flag) * 100.0 / COUNT(*) AS on_time_delivery_rate,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(sales_amount) AS total_sales,
    DATE_TRUNC('month', order_date) AS month
FROM cycle_metrics
GROUP BY 1, 2, 3, 9;
