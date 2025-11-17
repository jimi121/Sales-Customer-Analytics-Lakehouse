{{ config(materialized='view') }}

WITH customer_orders AS (
    SELECT
        f.customer_key,
        MIN(f.order_date) AS first_purchase_date,
        MAX(f.order_date) AS last_purchase_date,
        COUNT(DISTINCT f.order_number) AS total_orders,
        SUM(f.sales_amount) AS total_sales,
        SUM(f.quantity) AS total_quantity
    FROM 
        {{ ref("gold_fact_sales") }} f
    GROUP BY 
        f.customer_key
),

recency_freq_value AS (
    SELECT
        co.customer_key,
        co.total_orders,
        co.total_sales,
        co.total_quantity,
        DATEDIFF(current_date, co.last_purchase_date) AS recency_days,
        co.first_purchase_date,
        co.last_purchase_date
    FROM customer_orders co
),

customer_profile AS (
    SELECT
        c.customer_key,
        c.customer_id,
        c.customer_number,
        c.first_name,
        c.last_name,
        c.country,
        c.gender,
        c.marital_status,
        c.create_date,
        DATEDIFF(current_date(), c.create_date) AS customer_tenure_days
    FROM {{ ref('gold_dim_customers') }} c
)

SELECT
    cp.customer_key,
    cp.first_name,
    cp.last_name,
    cp.country,
    cp.gender,
    cp.marital_status,
    cp.customer_tenure_days,
    rfv.total_orders,
    rfv.total_sales,
    rfv.total_quantity,
    rfv.recency_days,
    rfv.first_purchase_date,
    rfv.last_purchase_date,
    CASE
        WHEN rfv.recency_days <= 30 THEN 'Active'
        WHEN rfv.recency_days BETWEEN 31 AND 90 THEN 'At Risk'
        ELSE 'Dormant'
    END AS customer_segment
FROM recency_freq_value rfv
LEFT JOIN customer_profile cp
    ON rfv.customer_key = cp.customer_key;
