-- models/tpch/q03_revenue.sql

SELECT
    c.c_custkey,
    c.c_name,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    o.o_orderdate,
    o.o_shippriority
FROM {{ source('tpch', 'customer') }} AS c
JOIN {{ source('tpch', 'orders') }} AS o
    ON c.c_custkey = o.o_custkey
JOIN {{ source('tpch', 'lineitem') }} AS l
    ON l.l_orderkey = o.o_orderkey
WHERE
    o.o_orderdate < DATE '1995-03-15'
    AND l.l_shipdate > DATE '1995-03-15'
GROUP BY
    c.c_custkey,
    c.c_name,
    o.o_orderdate,
    o.o_shippriority
ORDER BY
    revenue DESC,
    o.o_orderdate