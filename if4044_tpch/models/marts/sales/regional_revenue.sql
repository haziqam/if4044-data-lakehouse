-- models/marts/sales/regional_revenue.sql

SELECT
    region,
    SUM(line_revenue) AS total_revenue
FROM {{ ref('stg_sales_data') }}
GROUP BY region
