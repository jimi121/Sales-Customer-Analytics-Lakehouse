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
        c.country
    FROM 
        {{ref("gold_fact_sales")}} f
    JOIN
        {{ref("gold_dim_customers")}} c ON f.customer_key = c.customer_key
),

-- Calculate fulfillment metrics
metrics AS (
    SELECT
        order_number,
        country,
        product_key,
        order_date,
        shipping_date,
        due_date,
        sales_amount,
        quantity,
        DATEDIFF(shipping_date, order_date) AS lead_time_days,
        CASE 
            WHEN shipping_date <= due_date THEN 1 
            ELSE 0 
        END AS on_time_flag
    FROM base
)

SELECT
    country,
    product_key,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(on_time_flag) AS on_time_orders,
    ROUND(SUM(on_time_flag) * 100.0 / COUNT(DISTINCT order_number), 2) AS on_time_delivery_rate,
    AVG(lead_time_days) AS avg_lead_time_days,
    STDDEV(lead_time_days) AS lead_time_variance,
    SUM(sales_amount) AS total_sales,
    SUM(quantity) AS total_quantity
FROM 
    metrics
GROUP BY 
    country, 
    product_key