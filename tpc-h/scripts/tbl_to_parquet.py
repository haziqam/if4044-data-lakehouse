import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Define the path to your .tbl file
input_tbl_file = '../tpc-h-1gb/customer.tbl.1'
output_parquet_file = 'customer.parquet'

# Define the column names and their data types
# The .tbl files are pipe-separated and have no header.
# TPC-H customer table schema:
column_names = [
    'C_CUSTKEY',
    'C_NAME',
    'C_ADDRESS',
    'C_NATIONKEY',
    'C_PHONE',
    'C_ACCTBAL',
    'C_MKTSEGMENT',
    'C_COMMENT',
    'C_TRAILING_DELIMITER' # To handle the extra '|' at the end of each line
]

# Infer data types or specify them explicitly if you know them.
# For TPC-H, it's good to specify for better precision.
dtype_mapping = {
    'C_CUSTKEY': int,
    'C_NAME': str,
    'C_ADDRESS': str,
    'C_NATIONKEY': int,
    'C_PHONE': str,
    'C_ACCTBAL': float, # Use float for DECIMAL(15,2) in pandas, or Decimal for exact precision
    'C_MKTSEGMENT': str,
    'C_COMMENT': str,
    'C_TRAILING_DELIMITER': str # This column will be dropped later
}

print(f"Reading {input_tbl_file}...")
# Read the .tbl file into a pandas DataFrame
# - sep='|' for pipe-separated
# - header=None as there's no header row
# - names=column_names to assign column names
# - engine='python' might be needed for skipfooter if you have a malformed last line,
#   but generally 'c' is faster. For clean TPC-H files, 'c' should work.
# - skipinitialspace=False (default for '|')
# - On_bad_lines='warn' or 'error' if you want to handle malformed rows
# - quoting=csv.QUOTE_NONE to prevent issues with quotes within fields if any
#   (though TPC-H .tbl typically doesn't have quoted fields).
df = pd.read_csv(
    input_tbl_file,
    sep='|',
    header=None,
    names=column_names,
    dtype=dtype_mapping,
    skipinitialspace=False,
    keep_default_na=False, # Important to not interpret empty strings as NaN
)

# The last column `C_TRAILING_DELIMITER` is just for the extra '|' and needs to be dropped.
df = df.drop(columns=['C_TRAILING_DELIMITER'])

print(f"Successfully read {len(df)} rows. Converting to Parquet...")

# Convert pandas DataFrame to Apache Arrow Table
table = pa.Table.from_pandas(df)

# Write the Apache Arrow Table to Parquet format
# You can specify compression (e.g., 'snappy', 'gzip', 'brotli', 'zstd')
# 'snappy' is a good default for balanced compression and performance.
pq.write_table(table, output_parquet_file, compression='snappy')

print(f"Successfully converted {input_tbl_file} to {output_parquet_file}")
print(f"Parquet file saved at: {output_parquet_file}")



# Define the path to your Parquet file
parquet_file = 'customer.parquet'

print(f"Reading the Parquet file: {parquet_file}")

try:
    # Read the Parquet file into a pandas DataFrame
    df_read = pd.read_parquet(parquet_file)

    print("\nFirst 5 rows of the Parquet file:")
    print(df_read.head())

except FileNotFoundError:
    print(f"Error: The file '{parquet_file}' was not found. Please ensure it exists.")
except Exception as e:
    print(f"An error occurred while reading the Parquet file: {e}")