# TPC-H Generator with DuckDB

An alternative way to generate TPC-H benchmark dataset using DuckDB, includes the conversion step to CSV and Parquet.

---

## Dataset Generation
To use this script, make sure you have Duck DB installed by using the following command (ignore if you already have it):

    curl https://install.duckdb.org | sh

Add duckdb to your path:

    export PATH='/root/.duckdb/cli/latest':$PATH

Use DuckDB TPC-H extension with the following commands:

    duckdb <filename>.db
    INSTALL tpch;
    LOAD tpch;
    CALL dbgen(sf = 60);

You can change the size of the dataset by modifying the `sf` parameter. One `scale factor` roughly equals to `1 GB` in size without compression.

> [!NOTE]
> Since the data will be generated in DuckDB database format, the resulting file will be way smaller.

### Resource Usage
Generating TPC-H requires significant amount of time and memory. If you don't have enough memory to generate the dataset with a single step, DuckDB provides parameters called `children` and `step` which allows you to split the dataset generation into two or more partitions.

For example, if you want to generate a TPC-H dataset in 4 partitions:

    CALL dbgen(sf = 50, children = 4, step = 0);
    CALL dbgen(sf = 50, children = 4, step = 1);
    CALL dbgen(sf = 50, children = 4, step = 2);
    CALL dbgen(sf = 50, children = 4, step = 3);

To illustrate how much resource you would need to generate a TPC-H dataset, a `sf = 100` dataset requires `71 GB of memory` (single step) and takes approximately `17 minutes`. The database size (DuckDB format) will roughly amount to `26 GB`.

## Converting to CSV and Parquet
By default, DuckDB will generate TPC-H dataset in the DuckDB database format. If you wish to convert the resulting dataset to CSV and Parquet, follow these command.

Execute the following commands:

    duckdb <filename>.db
    COPY <table_name> TO '<table_name>.csv' (FORMAT CSV, HEADER TRUE);
    COPY <table_name> TO '<table_name>.parquet' (FORMAT PARQUET);

Use the same file you generated in the previous step. There are eight tables in the standard TPC-H dataset, so you need to repeat the command above for all the tables.

For reference, the default tables are: customer, lineitem, nation, orders, part, partsupp, region, and supplier.

> [!TIP]
> For more information, kindly visit the DuckDB official documentation at https://duckdb.org/docs/stable






