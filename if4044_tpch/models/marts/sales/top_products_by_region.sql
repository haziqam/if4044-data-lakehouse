-- models/marts/sales/top_products_by_region.sql

{{ top_n_per_group(
     table      = ref('stg_sales_data'),
     group_cols = ["region"],
     order_col  = "line_revenue DESC"
   )
}}
