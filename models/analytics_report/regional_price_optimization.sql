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
        p.category,
        p.subcategory,
        p.product_cost,
        c.country
    FROM {{ ref('gold_fact_sales') }} f
    LEFT JOIN {{ ref('gold_dim_products') }} p ON f.product_key = p.product_key
    LEFT JOIN {{ ref('gold_dim_customers') }} c ON f.customer_key = c.customer_key
),

daily_sales AS (
    -- Aggregate sales and price by day, country, and category to get the average price and total quantity for a consistent time period.
    SELECT
        order_date,
        country,
        category,
        subcategory,
        AVG(price) AS daily_avg_price,
        SUM(quantity) AS daily_quantity
    FROM base
    GROUP BY 1, 2, 3, 4
),

lagged_sales AS (
    -- Calculate previous day's price and quantity for comparison.
    SELECT
        order_date,
        country,
        category,
        subcategory,
        daily_avg_price,
        daily_quantity,
        LAG(daily_avg_price, 1) OVER (
            PARTITION BY country, category ORDER BY order_date) AS previous_day_price,
        LAG(daily_quantity, 1) OVER (
            PARTITION BY country, category ORDER BY order_date) AS previous_day_quantity
    FROM daily_sales
),

elasticity_by_day AS (
    -- Apply the arc elasticity formula: ((Q2-Q1)/(Q2+Q1)) / ((P2-P1)/(P2+P1))
    -- NULLIF is used to prevent division by zero errors.
    SELECT
        order_date,
        country,
        category,
        subcategory,
        (daily_quantity - previous_day_quantity) / NULLIF(daily_quantity + previous_day_quantity, 0) AS percent_change_quantity,
        (daily_avg_price - previous_day_price) / NULLIF(daily_avg_price + previous_day_price, 0) AS percent_change_price
    FROM lagged_sales
    WHERE 
        previous_day_price IS NOT NULL 
    AND previous_day_quantity IS NOT NULL
)

SELECT
    country,
    category,
    AVG(percent_change_quantity / NULLIF(percent_change_price, 0)) AS price_elasticity
FROM elasticity_by_day
GROUP BY 1, 2;
