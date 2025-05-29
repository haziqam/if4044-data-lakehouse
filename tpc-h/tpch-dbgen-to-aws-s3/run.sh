#!/bin/bash

# Size, in GBs, of the total dataset to be created
SIZE=50

# For each table, how many files should it be split into?
CHUNKS=10

# The S3 bucket to which the TPC-H files will be sent
S3_BUCKET=if4044-big-data-kel-4

# The top-level working directory (should be one dir above tpch-dbgen)
ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Directory to which you've cloned tpch-dbgen from github
DBGEN_PATH="$ROOT_PATH/tpch-dbgen"

# Python script for converting tbl -> csv + parquet
CONVERTER_SCRIPT="$ROOT_PATH/scripts/tbl_to_csv_parquet.py"

#########################################
# Should not need to edit below this line
#########################################

DIR_NAME="tpc-h-${SIZE}gb"
OUTPUT_DIR="$ROOT_PATH/$DIR_NAME"
mkdir -p "$OUTPUT_DIR"
export DSS_PATH="$OUTPUT_DIR"
cd "$DBGEN_PATH" || exit 1

# Generate output files
for ((i=1; i<=CHUNKS; i++)); do
    echo "./dbgen -v -s $SIZE -C $CHUNKS -S $i -f"
    ./dbgen -v -s "$SIZE" -C "$CHUNKS" -S "$i" -f
done

cd "$ROOT_PATH" || exit 1

# Process each generated .tbl file
for f in "$OUTPUT_DIR"/*.tbl*; do
  # Get filename without path
  FILENAME=$(basename -- "$f")

  # Extract table name before .tbl or .tbl.N (strip numbers)
  TABLE=$(echo "$FILENAME" | sed -E 's/([a-zA-Z]+)\.tbl.*/\1/' | tr '[:lower:]' '[:upper:]')

  echo "Processing $FILENAME (Table: $TABLE)"

  # Define output file paths
  CSV_FILE="${f}.csv"
  PARQUET_FILE="${f}.parquet"

  # Run Python converter
  python3 "$CONVERTER_SCRIPT" \
    --input "$f" \
    --output_csv "$CSV_FILE" \
    --output_parquet "$PARQUET_FILE" \
    --table "$TABLE"

  # Upload CSV to S3
  aws s3 cp "$CSV_FILE" "s3://$S3_BUCKET/$DIR_NAME-csv/$TABLE/$(basename "$CSV_FILE")"

  # Upload Parquet to S3
  aws s3 cp "$PARQUET_FILE" "s3://$S3_BUCKET/$DIR_NAME-parquet/$TABLE/$(basename "$PARQUET_FILE")"

done
