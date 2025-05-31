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

## Uploading to R2

To upload your CSV and Parquet files to R2, you will need to setup a remote with RClone. It is advisable to use the latest version of rclone since the earlier version (especially <v1.59) does not fully adhere to the S3 specifications, causing `HTTP 401: Unauthorized` errors.

Make sure to uninstall any old rclone version (if any), then run:

    sudo -v ; curl https://rclone.org/install.sh | sudo bash -s beta

Then, make a new config with:

    rclone config

Afterwards, proceed with the config procedure:
1. Create new remote by selecting n.
2. Select a name for the new remote. For example, use r2.
3. Select the Amazon S3 Compliant Storage Providers storage type (type `4`)
4. Select Cloudflare R2 storage for the provider (type `6`)
5. Select whether you would like to enter AWS credentials manually, or get it from the runtime environment.
6. Enter the Access Key ID.
7. Enter Secret Access Key (password).
8. Select the region to connect to (optional).
9. Select the S3 API endpoint.

> [!TIP]
> You can get the Access Key ID and Secret from the Manage API Token menu in R2 page. As for the S3 API Endpoint, get it from your specific bucket setting.

Once you're done configuring your rclone, run the following command:

    chmod +x upload-to-r2.sh
    ./upload-to-r2.sh

> [!IMPORTANT]
> Make sure all of the CSV and Parquet files are available and belong to the same directory as the script. Configure your remote and bucket name accordingly in the `upload-to-r2.sh` file.


