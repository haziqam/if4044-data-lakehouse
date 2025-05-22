#!/bin/bash

# Size, in GBs, of the total dataset to be created
SIZE=1

# For each table, how many files should it be split into?
CHUNKS=10

# The S3 bucket to which the TPC-H files will be sent
S3_BUCKET=if4044-big-data-kel-4

# The top-level working directory (should be one dir above tpch-dbgen)
ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Directory to which you've cloned tpch-dbgen from github
DBGEN_PATH="$ROOT_PATH/tpch-dbgen"

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

# Loop through and upload each file to S3
for f in "$OUTPUT_DIR"/*; do

  # Get filename from full file path
  FILENAME=$(basename -- "$f")

  # Match anything *before* the .tbl or .tbl.X where X is one or more digits
  REGEX='\w+(?=\.tbl)'
  TABLE=$(echo "$FILENAME" | grep -Po "$REGEX")

  # Upload file to S3
  aws s3 cp "$f" "s3://$S3_BUCKET/$DIR_NAME/$TABLE/$FILENAME.csv"

done
