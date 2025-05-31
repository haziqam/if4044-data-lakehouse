-- macros/utils.sql

{% macro calculate_revenue(price_col, discount_col) %}
  ( {{ price_col }} * (1 - {{ discount_col }}) )
{% endmacro %}

{% macro month_trunc(date_col) %}
  DATE_TRUNC('month', {{ date_col }})
{% endmacro %}

{% macro top_n_per_group(table, group_cols, order_col, n_var='top_n') %}

  {#— read `top_n` (or whatever var name you passed) with default=1 —#}
  {% set n_value = var(n_var, 1) %}

  WITH ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY {{ group_cols | join(', ') }}
        ORDER BY {{ order_col }}
      ) AS _row_num
    FROM {{ table }}
  )
  SELECT *
  FROM ranked
  WHERE _row_num <= {{ n_value }}
{% endmacro %}
