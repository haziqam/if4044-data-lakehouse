-- models/if4044-_tpch/q03_revenue.sql

SELECT
    l.l_orderkey,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    o.o_orderdate,
    o.o_shippriority
FROM
    {{ source('tpch', 'customer') }} AS c,
    {{ source('tpch', 'orders') }} AS o,
    {{ source('tpch', 'lineitem') }} AS l
WHERE
    c.c_mktsegment = 'BUILDING' -- RANDOMLY SELECTED WITHIN THE LIST OF VALUES DEFINED FOR SEGMENTS IN CLAUSE 4.2.2.13
    AND c.c_custkey = o.o_custkey
    AND l.l_orderkey = o.o_orderkey
    AND o.o_orderdate < DATE '1995-03-15' --  RANDOMLY SELECTED DAY WITHIN [1995-03-01 .. 1995-03-31]
    AND l.l_shipdate > DATE '1995-03-15' -- RANDOMLY SELECTED DAY WITHIN [1995-03-01 .. 1995-03-31]
GROUP BY
    l.l_orderkey,
    o.o_orderdate,
    o.o_shippriority
ORDER BY
    revenue DESC,
    o.o_orderdate