{{ config(materialized='view') }}

WITH base AS (
    SELECT
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        p.product_name,
        p.category,
        p.subcategory,
        p.start_date,
        p.product_cost
    FROM {{ ref('gold_fact_sales') }} f
    LEFT JOIN {{ ref('gold_dim_products') }} p 
        ON f.product_key = p.product_key
),

returns_flagged AS (
    SELECT
        *,
        CASE 
            WHEN sales_amount < 0 OR quantity < 0 THEN 1 ELSE 0
        END AS is_return
    FROM base
),

agg AS (
    SELECT
        f.product_key,
        p.category,          
        p.subcategory,        
        p.product_name,     
        p.start_date,         
        SUM(f.quantity) AS total_qty_sold,
        SUM(CASE WHEN is_return = 1 THEN ABS(f.quantity) ELSE 0 END) AS total_qty_returned,
        SUM(f.sales_amount) AS net_sales,
        SUM(CASE WHEN is_return = 1 THEN ABS(f.sales_amount) ELSE 0 END) AS total_refund_amount,
        SUM(f.quantity * p.product_cost) AS total_cost,
        CASE 
            WHEN SUM(f.quantity) = 0 THEN 0
            ELSE ROUND(SUM(CASE WHEN is_return = 1 THEN ABS(f.quantity) ELSE 0 END) / SUM(ABS(f.quantity)), 3)
        END AS return_rate,
        DATEDIFF(CURRENT_DATE, MIN(p.start_date)) AS product_age_days
    FROM returns_flagged f
    LEFT JOIN {{ ref('gold_dim_products') }} p ON f.product_key = p.product_key
    GROUP BY 
        f.product_key, 
        p.category,         
        p.subcategory,        
        p.product_name,     
        p.start_date
)

SELECT
    *,
    (net_sales - total_cost - total_refund_amount) AS adjusted_profit,
    CASE 
        WHEN return_rate > 0.25 THEN 'Critical'
        WHEN return_rate BETWEEN 0.1 AND 0.25 THEN 'Watchlist'
        ELSE 'Healthy'
    END AS quality_flag
FROM agg;
