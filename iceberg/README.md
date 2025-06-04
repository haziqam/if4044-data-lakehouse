## Explanation

The `tbl_to_csv_parquet.py` script will be used by `../tpch-dbgen-to-aws-s3` to convert tbl file into csv and parquet before uploading it to S3. It will not be used by itself, but here's how to test it individually:

1. Install requirements

    ```
    pip install -r requirements.txt
    ```

2. Run the script

    ```
    python3 "tbl_to_csv_parquet.py" \
        --input "../tpc-h-50gb/lineitem.tbl.10" \
        --output_csv "../tpc-h-50gb/lineitem.tbl.10.csv" \
        --output_parquet "../tpc-h-50gb/lineitem.tbl.10.parquet" \
        --table "LINEITEM"

    ```
