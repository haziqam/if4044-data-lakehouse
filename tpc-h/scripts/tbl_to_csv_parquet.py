import logging
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

tpch_schema = {
    "CUSTOMER": {
        "column_names": [
            'C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY',
            'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'
        ],
        "dtypes": {
            'C_CUSTKEY': int,
            'C_NAME': str,
            'C_ADDRESS': str,
            'C_NATIONKEY': int,
            'C_PHONE': str,
            'C_ACCTBAL': float,
            'C_MKTSEGMENT': str,
            'C_COMMENT': str
        }
    },
    "LINEITEM": {
        "column_names": [
            'L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER',
            'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX',
            'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE',
            'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'
        ],
        "dtypes": {
            'L_ORDERKEY': int,
            'L_PARTKEY': int,
            'L_SUPPKEY': int,
            'L_LINENUMBER': int,
            'L_QUANTITY': float,
            'L_EXTENDEDPRICE': float,
            'L_DISCOUNT': float,
            'L_TAX': float,
            'L_RETURNFLAG': str,
            'L_LINESTATUS': str,
            'L_SHIPDATE': 'date',
            'L_COMMITDATE': 'date',
            'L_RECEIPTDATE': 'date',
            'L_SHIPINSTRUCT': str,
            'L_SHIPMODE': str,
            'L_COMMENT': str
        }
    },
    "NATION": {
        "column_names": [
            'N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'
        ],
        "dtypes": {
            'N_NATIONKEY': int,
            'N_NAME': str,
            'N_REGIONKEY': int,
            'N_COMMENT': str
        }
    },
    "ORDERS": {
        "column_names": [
            'O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE',
            'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY',
            'O_COMMENT'
        ],
        "dtypes": {
            'O_ORDERKEY': int,
            'O_CUSTKEY': int,
            'O_ORDERSTATUS': str,
            'O_TOTALPRICE': float,
            'O_ORDERDATE': 'date',
            'O_ORDERPRIORITY': str,
            'O_CLERK': str,
            'O_SHIPPRIORITY': int,
            'O_COMMENT': str
        }
    },
    "PART": {
        "column_names": [
            'P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE',
            'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'
        ],
        "dtypes": {
            'P_PARTKEY': int,
            'P_NAME': str,
            'P_MFGR': str,
            'P_BRAND': str,
            'P_TYPE': str,
            'P_SIZE': int,
            'P_CONTAINER': str,
            'P_RETAILPRICE': float,
            'P_COMMENT': str
        }
    },
    "PARTSUPP": {
        "column_names": [
            'PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'
        ],
        "dtypes": {
            'PS_PARTKEY': int,
            'PS_SUPPKEY': int,
            'PS_AVAILQTY': int,
            'PS_SUPPLYCOST': float,
            'PS_COMMENT': str
        }
    },
    "REGION": {
        "column_names": [
            'R_REGIONKEY', 'R_NAME', 'R_COMMENT'
        ],
        "dtypes": {
            'R_REGIONKEY': int,
            'R_NAME': str,
            'R_COMMENT': str
        }
    },
    "SUPPLIER": {
        "column_names": [
            'S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY',
            'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'
        ],
        "dtypes": {
            'S_SUPPKEY': int,
            'S_NAME': str,
            'S_ADDRESS': str,
            'S_NATIONKEY': int,
            'S_PHONE': str,
            'S_ACCTBAL': float,
            'S_COMMENT': str
        }
    }
}

def setup_logging(log_file=None):
    """Setup logging configuration"""
    if log_file is None:
        log_file = f"logs/tpch_conversion_2.log"
    
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, mode='a'),
                logging.StreamHandler()
            ]
        )
    return logger

def create_spark_compatible_schema(df, table_name):
    """Create a PyArrow schema that's compatible with Spark"""
    schema_fields = []
    dtypes = tpch_schema[table_name]["dtypes"]
    
    for col in df.columns:
        if dtypes[col] == 'date':
            schema_fields.append(pa.field(col, pa.date32()))
        elif dtypes[col] == int:
            schema_fields.append(pa.field(col, pa.int64()))
        elif dtypes[col] == float:
            schema_fields.append(pa.field(col, pa.float64()))
        elif dtypes[col] == str:
            schema_fields.append(pa.field(col, pa.string()))
    
    return pa.schema(schema_fields)

def tbl_to_csv_parquet(
        input_tbl_file: str, 
        output_parquet_file: str, 
        output_csv_file: str,
        table_name: str,
        logger=None):
    
    if logger is None:
        logger = logging.getLogger(__name__)
    
    column_names = tpch_schema[table_name]["column_names"]
    dtypes = tpch_schema[table_name]["dtypes"]

    logger.info(f"Reading {input_tbl_file}...")

    try:
        df = pd.read_csv(
            input_tbl_file,
            sep='|',
            header=None,
            skipinitialspace=False,
            keep_default_na=False,
        )
        
        if df.iloc[:, -1].isna().all() or (df.iloc[:, -1] == '').all():
            df = df.iloc[:, :-1]
            logger.info("Removed trailing empty column")
        
        df.columns = column_names
        
        for col, dtype in dtypes.items():
            if dtype == 'date':
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            else:
                df[col] = df[col].astype(dtype)

        logger.info(f"Successfully read {len(df)} rows from {input_tbl_file}")

        df.to_csv(output_csv_file, sep=";", index=False)
        logger.info(f"Successfully converted {input_tbl_file} to {output_csv_file}")

        schema = create_spark_compatible_schema(df, table_name)
        table = pa.Table.from_pandas(df, schema=schema)
        
        pq.write_table(
            table, 
            output_parquet_file, 
            compression='snappy',
            use_deprecated_int96_timestamps=False,
            coerce_timestamps='ms',
            allow_truncated_timestamps=True
        )
        
        logger.info(f"Successfully converted {input_tbl_file} to {output_parquet_file}")
        logger.info(f"Parquet file saved at: {output_parquet_file}")
        
    except Exception as e:
        logger.error(f"Error processing {input_tbl_file}: {str(e)}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert TPC-H .tbl file to CSV and Parquet")
    parser.add_argument("--input", required=True, help="Input .tbl file path")
    parser.add_argument("--output_csv", required=True, help="Output CSV file path")
    parser.add_argument("--output_parquet", required=True, help="Output Parquet file path")
    parser.add_argument("--table", required=True, choices=tpch_schema.keys(), help="TPC-H table name")
    parser.add_argument("--log_file", help="Log file path (optional)")

    args = parser.parse_args()
    
    logger = setup_logging(args.log_file)
    
    logger.info("Starting TPC-H table conversion")
    logger.info(f"Input file: {args.input}")
    logger.info(f"Output CSV: {args.output_csv}")
    logger.info(f"Output Parquet: {args.output_parquet}")
    logger.info(f"Table: {args.table}")

    tbl_to_csv_parquet(
        input_tbl_file=args.input,
        output_parquet_file=args.output_parquet,
        output_csv_file=args.output_csv,
        table_name=args.table,
        logger=logger
    )
    
    logger.info("Conversion completed successfully")