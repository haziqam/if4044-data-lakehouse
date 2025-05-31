-- models/marts/sales/monthly_growth.sql

WITH base AS (
    SELECT
        region,
        {{ month_trunc("order_date") }} AS order_month,
        SUM(line_revenue) AS revenue
    FROM {{ ref('stg_sales_data') }}
    GROUP BY 1, 2
),

growth AS (
    SELECT
        region,
        order_month,
        revenue,
        LAG(revenue) OVER (PARTITION BY region ORDER BY order_month) AS prev_revenue,
        CASE
          WHEN LAG(revenue) OVER (PARTITION BY region ORDER BY order_month) = 0 THEN NULL
          ELSE (revenue - LAG(revenue) OVER (PARTITION BY region ORDER BY order_month))
               / LAG(revenue) OVER (PARTITION BY region ORDER BY order_month)
        END AS growth_rate
    FROM base
)

SELECT * FROM growth
