from datetime import datetime
import logging
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq



def setup_logging(log_file=None):
    """Setup logging configuration"""
    if log_file is None:
        log_file = f"logs/fix_parquet_schema.log"
    
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

def fix_parquet_timestamps(input_file, output_file, table_name, logger):
    """Fix timestamp columns in parquet files for Spark compatibility"""
    
    # Define date columns for each table
    date_columns = {
        'LINEITEM': ['L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE'],
        'ORDERS': ['O_ORDERDATE']
    }
    
    if table_name not in date_columns:
        logger.info(f"No date columns to fix for {table_name}")
        return
    
    try:
        logger.info(f"Reading {input_file}...")
        start_time = datetime.now()
        
        # Read the parquet file
        df = pd.read_parquet(input_file)
        read_time = datetime.now()
        logger.info(f"Read completed in {(read_time - start_time).total_seconds():.2f} seconds")
        logger.info(f"Loaded {len(df):,} rows")
        
        # Convert timestamp columns to date
        cols_to_fix = date_columns[table_name]
        for col in cols_to_fix:
            if col in df.columns:
                logger.info(f"Converting {col} from timestamp to date...")
                # Convert to date type (removes time component)
                df[col] = pd.to_datetime(df[col]).dt.date
        
        # Create explicit schema with date32 for date columns
        schema_fields = []
        for col in df.columns:
            if col in cols_to_fix:
                schema_fields.append(pa.field(col, pa.date32()))
            else:
                # Infer type for other columns
                arrow_type = pa.infer_type(df[col].iloc[:1000])  # Sample first 1k rows for inference
                schema_fields.append(pa.field(col, arrow_type))
        
        schema = pa.schema(schema_fields)
        
        # Convert to PyArrow table with explicit schema
        logger.info("Converting to PyArrow table...")
        table = pa.Table.from_pandas(df, schema=schema)
        
        # Write the fixed parquet file
        logger.info(f"Writing fixed parquet to {output_file}...")
        write_start = datetime.now()
        
        pq.write_table(
            table,
            output_file,
            compression='snappy',
            use_deprecated_int96_timestamps=False,
            coerce_timestamps='ms',
            allow_truncated_timestamps=True
        )
        
        write_time = datetime.now()
        total_time = write_time - start_time
        
        logger.info(f"Write completed in {(write_time - write_start).total_seconds():.2f} seconds")
        logger.info(f"Total processing time: {total_time.total_seconds():.2f} seconds")
        logger.info(f"Successfully fixed {input_file} -> {output_file}")
        
        # Verify the fix by checking schema
        logger.info("Verifying fixed file...")
        fixed_table = pq.read_table(output_file)
        for col in cols_to_fix:
            if col in [field.name for field in fixed_table.schema]:
                field_type = fixed_table.schema.field(col).type
                logger.info(f"{col}: {field_type}")
        
    except Exception as e:
        logger.error(f"Error fixing {input_file}: {str(e)}")
        raise


if __name__ == "__main__":
    import pandas as pd
    logger = setup_logging()

    input_dir = "../tpc-h-50gb"
    output_dir = "../tpc-h-50gb-fixed"

    files = sorted(os.listdir(input_dir))
    files_to_fix = [
        file for file in files 
        if file.endswith(".parquet") 
            and (file.find("lineitem") != -1 or file.find("orders") != -1)
    ]


    for file in files_to_fix:
        file_path = os.path.join(input_dir, file)
        output_path = os.path.join(output_dir, file)
        table_name = "LINEITEM" if file.find("lineitem") != -1 else "ORDERS"
        fix_parquet_timestamps(file_path, output_path, table_name, logger)

    