## Purpose

Provide a script that makes it easy to generate a TPC-H data set and upload the output files to an AWS S3 bucket.

The generated files are chunked to support the parallel load capabilities provided by Amazon Redshift, AWS Glue, AWS EMR, etc.

The collection of files generated for each table are placed in their own S3 prefix which is necessary for certain services such as Redshift Spectrum.

## Source

https://github.com/zecksr/tpch-dbgen-to-aws-s3

## Instructions

1. Clone the tpc-h dbgen utility

    ```sh
    git clone https://github.com/electrum/tpch-dbgen

    cd tpch-dbgen

    make

    cd..
    ```

2. Ensure that your AWS credentials exist in your machine so that the script can use AWS S3 APIs like `cp`

    ```
    $ ls ~/.aws
    config  credentials
    ```

3. Ensure that the requirements in [this directory](../scripts/) is installed to run the script to convert tbl to csv and parquet. View this [README.md](../scripts/README.md) for more detail.

4. Create an S3 bucket

5. Edit the configuration variables in run.sh ( `SIZE`, `CHUNKS`, `S3_BUCKET`)

6. Run the script

    ```sh
    cd tpch-dbgen-to-aws-s3
    ./run.sh
    ```
