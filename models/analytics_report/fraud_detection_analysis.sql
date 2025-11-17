{{ config(materialized='view') }}

WITH base AS (
    SELECT
        f.order_number,
        f.customer_key,
        f.customer_id,
        f.order_date,
        f.sales_amount,
        f.quantity,
        f.price,
        c.country,
        c.create_date AS customer_create_date
    FROM {{ ref('gold_fact_sales') }} f
    LEFT JOIN {{ ref('gold_dim_customers') }} c 
        ON f.customer_key = c.customer_key
),

-- Calculate average order behavior per customer
customer_baseline AS (
    SELECT
        customer_key,
        AVG(sales_amount) AS avg_sales_per_order,
        AVG(quantity) AS avg_qty_per_order,
        COUNT(DISTINCT order_number) AS total_orders,
        MIN(order_date) AS first_order_date
    FROM base
    GROUP BY customer_key
),

-- Compare each order against its customer baseline
anomaly_calc AS (
    SELECT
        b.order_number,
        b.customer_key,
        b.customer_id,
        b.order_date,
        b.country,
        b.sales_amount,
        b.quantity,
        cb.avg_sales_per_order,
        cb.avg_qty_per_order,
        cb.total_orders,
        DATEDIFF(b.order_date, cb.first_order_date) AS days_since_first_order,

        -- Simple anomaly score (difference ratio)
        ROUND(ABS(b.sales_amount - cb.avg_sales_per_order) / NULLIF(cb.avg_sales_per_order, 0), 2) AS amount_deviation_ratio,
        ROUND(ABS(b.quantity - cb.avg_qty_per_order) / NULLIF(cb.avg_qty_per_order, 0), 2) AS qty_deviation_ratio,

        CASE 
            WHEN cb.total_orders <= 2 AND DATEDIFF(b.order_date, cb.first_order_date) < 7 THEN 1
            ELSE 0
        END AS new_account_flag,

        CASE 
            WHEN ABS(b.sales_amount - cb.avg_sales_per_order) / NULLIF(cb.avg_sales_per_order, 0) > 3 THEN 1
            WHEN ABS(b.quantity - cb.avg_qty_per_order) / NULLIF(cb.avg_qty_per_order, 0) > 3 THEN 1
            ELSE 0
        END AS outlier_flag
    FROM base b
    LEFT JOIN customer_baseline cb ON b.customer_key = cb.customer_key
)

SELECT
    *,
    CASE 
        WHEN outlier_flag = 1 OR new_account_flag = 1 THEN 'High Risk'
        WHEN amount_deviation_ratio > 1.5 OR qty_deviation_ratio > 1.5 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS risk_level
FROM anomaly_calc;
