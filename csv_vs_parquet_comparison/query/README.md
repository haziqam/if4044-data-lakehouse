# CSV vs Parquet Comparison with DuckDB

The purpose of this folder is to explain how you can compare CSV and Parquet performances with DuckDB. The prerequisite to this tutorial is a preinstalled DuckDB-cli and TPC-H dataset with both CSV and Parquet format. Make sure the files are in the same directory as where you run the duckdb.

---

Run DuckDB and create the views with the following commands:

    duckdb
    CREATE OR REPLACE VIEW lineitem  AS read_csv('lineitem.csv');
    CREATE OR REPLACE VIEW customer  AS read_csv('customer.csv');
    CREATE OR REPLACE VIEW orders    AS read_csv('orders.csv');
    CREATE OR REPLACE VIEW supplier  AS read_csv('supplier.csv');
    CREATE OR REPLACE VIEW nation    AS read_csv('nation.csv');
    CREATE OR REPLACE VIEW region    AS read_csv('region.csv');
    CREATE OR REPLACE VIEW part      AS read_csv('part.csv');
    CREATE OR REPLACE VIEW partsupp  AS read_csv('partsupp.csv');

> [!NOTE]
> Since we didn't specify any file for the database, the database is NOT persistent and will vanish the moment you close duckdb. However, in our case, it doesn't matter since we are only creating views, not tables.

Activate the duckdb timer.

    .timer on

Then, run the query from q1, q2, q3, q5, and q12 from the `/query` folder. Log the result somewhere. You can also run other query as you wish. Afterwards, you should repeat all the steps above, except, now create views from the parquet files as displayed below.

    CREATE OR REPLACE VIEW lineitem  AS SELECT * FROM read_parquet('lineitem.parquet');
    CREATE OR REPLACE VIEW customer  AS SELECT * FROM read_parquet('customer.parquet');
    CREATE OR REPLACE VIEW orders    AS SELECT * FROM read_parquet('orders.parquet');
    CREATE OR REPLACE VIEW supplier  AS SELECT * FROM read_parquet('supplier.parquet');
    CREATE OR REPLACE VIEW nation    AS SELECT * FROM read_parquet('nation.parquet');
    CREATE OR REPLACE VIEW region    AS SELECT * FROM read_parquet('region.parquet');
    CREATE OR REPLACE VIEW part      AS SELECT * FROM read_parquet('part.parquet');
    CREATE OR REPLACE VIEW partsupp  AS SELECT * FROM read_parquet('partsupp.parquet');

> [!TIP]
> For more in depth analysis, you can also use other methods of profiling (e.g. using PRAGMA). Kindly read them here https://duckdb.org/docs/stable/configuration/pragmas.