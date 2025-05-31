-- models/staging/stg_sales_data.sql

WITH raw_joined AS (

    SELECT
        l.l_orderkey                            AS order_key,
        l.l_partkey                             AS part_key,
        {{ calculate_revenue("l.l_extendedprice", "l.l_discount") }} AS line_revenue,
        o.o_orderdate                           AS order_date,
        r.r_name                                AS region,
        p.p_name                                AS product_name
    FROM {{ source('tpch', 'lineitem') }}  AS l

    JOIN {{ source('tpch', 'orders') }}    AS o
      ON l.l_orderkey = o.o_orderkey

    JOIN {{ source('tpch', 'customer') }}  AS c
      ON o.o_custkey = c.c_custkey

    JOIN {{ source('tpch', 'nation') }}    AS n
      ON c.c_nationkey = n.n_nationkey

    JOIN {{ source('tpch', 'region') }}    AS r
      ON n.n_regionkey = r.r_regionkey

    JOIN {{ source('tpch', 'part') }}      AS p
      ON l.l_partkey = p.p_partkey

)

SELECT
    order_key,
    part_key,
    line_revenue,
    order_date,
    region,
    product_name
FROM raw_joined
