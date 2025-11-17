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
        c.country
    FROM {{ ref('gold_fact_sales') }} f
    LEFT JOIN {{ ref('gold_dim_products') }} p ON f.product_key = p.product_key
    LEFT JOIN {{ ref('gold_dim_customers') }} c ON f.customer_key = c.customer_key
),

-- Identify returns (negative sales or quantity)
returns_flagged AS (
    SELECT
        *,
        CASE 
            WHEN sales_amount < 0 OR quantity < 0 THEN 1 ELSE 0
        END AS is_return
    FROM base
),

-- Aggregate by customer/product/category
return_summary AS (
    SELECT
        product_key,
        customer_key,
        category,
        subcategory,
        country,
        COUNT(DISTINCT order_number) AS total_orders,
        SUM(CASE WHEN is_return = 1 THEN 1 ELSE 0 END) AS total_returns,
        SUM(sales_amount) AS net_sales,
        SUM(CASE WHEN is_return = 1 THEN ABS(sales_amount) ELSE 0 END) AS total_refund_amount,
        SUM(quantity) AS total_quantity,
        CASE 
            WHEN COUNT(DISTINCT order_number) = 0 THEN 0
            ELSE ROUND(SUM(CASE WHEN is_return = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT order_number), 3)
        END AS return_rate
    FROM returns_flagged
    GROUP BY 
        product_key, 
        customer_key, 
        category, 
        subcategory, 
        country
)

SELECT
    *,
    CASE 
        WHEN return_rate > 0.3 THEN 'High Risk'
        WHEN return_rate BETWEEN 0.1 AND 0.3 THEN 'Moderate Risk'
        ELSE 'Low Risk'
    END AS return_risk_level
FROM return_summary;
