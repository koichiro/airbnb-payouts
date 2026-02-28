import os
import io
import logging
import hashlib
import pyarrow
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_airbnb_csv(event, context=None):
    """
    Cloud Function to load Airbnb earnings CSV from GCS to BigQuery with Upsert (MERGE) logic.
    Supports both Gen 1 and Gen 2 (CloudEvent) signatures.
    """

    # 1. Handle Event Data based on Function Generation
    if context is None:
        # Gen 2 / CloudRun Environment
        logger.info("Executing as Gen 2")
        data = event.data
    else:
        # Gen 1 Environment
        logger.info("Executing as Gen 1")
        data = event

    bucket_name = data['bucket']
    file_name = data['name']

    if not file_name.lower().endswith('.csv'):
        logger.info(f"Skipping non-CSV file: {file_name}")
        return

    # 2. Get Configuration from Environment Variables
    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset_id = os.environ.get("BQ_DATASET_ID", "airbnb_management")
    table_id = os.environ.get("BQ_TABLE_ID", "earnings_cleaned")
    staging_table_id = f"{table_id}_staging"

    try:
        # 3. Download CSV from Google Cloud Storage
        storage_client = storage.Client()
        blob = storage_client.bucket(bucket_name).blob(file_name)
        content = blob.download_as_bytes()

        # 4. Data Cleansing with Pandas
        # Use utf-8-sig to handle potential BOM in Airbnb CSV
        df = pd.read_csv(io.BytesIO(content), encoding='utf-8-sig')
        df.columns = df.columns.str.strip() # Remove any leading/trailing whitespace from headers

        # Mapping known columns to English for better SQL handling
        # This part ensures specific columns are renamed, while keeping all other columns intact
        COLUMN_MAP = {
            '日付': 'event_date',
            '入金予定日': 'payout_scheduled_date',
            '種別': 'type',
            '確認コード': 'confirmation_code',
            '予約日': 'booking_date',
            '開始日': 'start_date',
            '終了日': 'end_date',
            '泊数': 'number_of_nights',
            'ゲスト': 'guest',
            'リスティング': 'listing_name',
            '詳細': 'details',
            '参照コード': 'reference_code',
            '通貨': 'currency',
            '金額': 'amount',
            '支払い済み': 'paid',
            'サービス料': 'service_fee',
            'スピード送金の手数料': 'express_transfer_fee',
            '清掃料金': 'cleaning_fee',
            'ペット料金': 'pet_fee',
            '総収入': 'total_income',
            '宿泊税': 'accommodation_tax',
            'ホスティング収入年度': 'hosting_revenue_fiscal_year'
        }

        # Identify columns in the CSV that are not in our COLUMN_MAP (unexpected/unknown columns)
        # These columns will remain with their original names after df.rename()
        source_columns = set(df.columns) # Columns in the raw CSV
        mapped_source_columns = set(COLUMN_MAP.keys()) # Japanese names we expect to map

        unmapped_source_columns = [col for col in source_columns if col not in mapped_source_columns]

        if unmapped_source_columns:
            logger.warning(f"Found unmapped columns in the input CSV: {unmapped_source_columns}. "
                           f"These columns will be loaded with their original names into the staging table. "
                           f"Consider updating COLUMN_MAP or the target BigQuery table schema if these columns are important.")

        df.rename(columns=COLUMN_MAP, inplace=True)

        # Explicitly format date columns to ensure BQ recognizes them correctly
        date_cols = ['event_date', 'payout_scheduled_date', 'booking_date', 'start_date', 'end_date']
        for col in date_cols:
            if col in df.columns:
                # Airbnb typically uses MM/DD/YYYY format
                df[col] = pd.to_datetime(df[col], format='%m/%d/%Y', errors='coerce').dt.date

        # Sanitize numeric columns
        num_cols = ['amount', 'service_fee', 'cleaning_fee', 'total_income']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 5. IDEMPOTENCY: Generate a unique hash for each row to serve as a Primary Key (row_id)
        # We use SHA256 on the entire row content to ensure even entries without IDs (like Payouts) are unique
        df['row_id'] = df.apply(
            lambda row: hashlib.sha256(str(tuple(row)).encode('utf-8')).hexdigest(), 
            axis=1
        )

        # 6. BigQuery Operations (Staging -> Merge -> Cleanup)
        bq_client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        staging_ref = f"{project_id}.{dataset_id}.{staging_table_id}"

        # Define explicit schema to ensure financial columns use NUMERIC type
        # instead of FLOAT64 to prevent rounding errors.
        job_schema = [
            bigquery.SchemaField("event_date", "DATE"),
            bigquery.SchemaField("payout_scheduled_date", "DATE"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("confirmation_code", "STRING"),
            bigquery.SchemaField("booking_date", "DATE"),
            bigquery.SchemaField("start_date", "DATE"),
            bigquery.SchemaField("end_date", "DATE"),
            bigquery.SchemaField("number_of_nights", "NUMERIC"),
            bigquery.SchemaField("guest", "STRING"),
            bigquery.SchemaField("listing_name", "STRING"),
            bigquery.SchemaField("details", "STRING"),
            bigquery.SchemaField("reference_code", "STRING"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("amount", "NUMERIC"),
            bigquery.SchemaField("paid", "NUMERIC"),
            bigquery.SchemaField("service_fee", "NUMERIC"),
            bigquery.SchemaField("express_transfer_fee", "NUMERIC"),
            bigquery.SchemaField("cleaning_fee", "NUMERIC"),
            bigquery.SchemaField("pet_fee", "NUMERIC"),
            bigquery.SchemaField("total_income", "NUMERIC"),
            bigquery.SchemaField("accommodation_tax", "NUMERIC"),
            bigquery.SchemaField("hosting_revenue_fiscal_year", "NUMERIC"),
            bigquery.SchemaField("row_id", "STRING", "REQUIRED"),
        ]

        # A. Load to Staging Table (Overwrite)
        # Using autodetect=True allows the schema to adapt to "all columns" provided in the CSV
        load_job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=job_schema, # Use explicit schema
            autodetect=True
        )

        # --- Start of temporary debugging code ---
        import pyarrow
        logger.info("Starting diagnostic check: Verifying DataFrame columns against BigQuery schema...")
        bq_schema_to_check = getattr(load_job_config, 'schema', None)
        if bq_schema_to_check:
            for field in bq_schema_to_check:
                col_name = field.name
                if col_name not in df.columns:
                    # This case can happen if a column in the schema is not in the CSV. This is usually fine.
                    logger.warning(f"Column '{col_name}' from schema not found in DataFrame. Skipping check.")
                    continue
                try:
                    series = df[col_name]
                    # This mapping is simplified for debugging but covers common cases.
                    arrow_type = None
                    if field.field_type in ('STRING', 'GEOGRAPHY'):
                        arrow_type = pyarrow.string()
                    elif field.field_type == 'DATE':
                        arrow_type = pyarrow.date32()
                    elif field.field_type == 'BYTES':
                        arrow_type = pyarrow.binary(field.max_length) if field.max_length else pyarrow.binary()
                    elif field.field_type in ('INTEGER', 'INT64'):
                        arrow_type = pyarrow.int64()
                    elif field.field_type in ('FLOAT', 'FLOAT64', 'NUMERIC', 'BIGNUMERIC'):
                        # NUMERIC is tricky as pandas doesn't have a native high-precision decimal.
                        # We test as float64, as that's how pandas stores it.
                        arrow_type = pyarrow.float64()
                    elif field.field_type == 'BOOLEAN':
                        arrow_type = pyarrow.bool_()
                    elif field.field_type == 'TIMESTAMP':
                        arrow_type = pyarrow.timestamp('us', tz='UTC') # BQ Timestamps are UTC

                    if arrow_type:
                        logger.info(f"Checking column '{col_name}' for conversion to Arrow type '{arrow_type}'...")
                        # Attempt the conversion to see if it fails
                        pyarrow.Array.from_pandas(series, type=arrow_type)
                        logger.info(f"Column '{col_name}' PASSED check.")
                    else:
                        logger.info(f"Skipping check for unhandled BQ type '{field.field_type}' in column '{col_name}'.")

                except (pyarrow.lib.ArrowInvalid, pyarrow.lib.ArrowTypeError) as e:
                    logger.error(f"!!! >>> COLUMN '{col_name}' FAILED conversion to Arrow array! <<< !!!")
                    logger.error(f"BigQuery Type specified: {field.field_type}")
                    logger.error(f"Pandas Series dtype: {series.dtype}")
                    logger.error(f"First 5 values of '{col_name}':\n{series.head().to_string()}")
                    logger.error(f"Arrow Conversion Error: {e}")
                    # Re-raise a clearer error to stop execution.
                    raise ValueError(f"Data in column '{col_name}' is incompatible with the specified BigQuery schema.") from e
                except Exception as e:
                    logger.error(f"An unexpected error occurred while checking column '{col_name}': {e}")
                    raise
            logger.info("All columns in the explicit schema passed the diagnostic check.")
        else:
            logger.warning("No explicit schema found in load_job_config. Skipping diagnostic check.")
        # --- End of temporary debugging code ---

        # --- Start of V2 temporary debugging code ---
        logger.info("Starting diagnostic check V2...")
        
        # Part 1: Check columns defined in the explicit schema
        logger.info("Part 1: Verifying columns defined in the explicit schema...")
        bq_schema_to_check = getattr(load_job_config, 'schema', [])
        schema_cols = {field.name for field in bq_schema_to_check}
        passed_schema_check = True

        for field in bq_schema_to_check:
            col_name = field.name
            if col_name not in df.columns:
                logger.warning(f"Schema column '{col_name}' not in DataFrame. Skipping check.")
                continue
            try:
                series = df[col_name]
                arrow_type = None # Simplified type mapping for debug
                if field.field_type in ('STRING', 'GEOGRAPHY'): arrow_type = pyarrow.string()
                elif field.field_type == 'DATE': arrow_type = pyarrow.date32()
                elif field.field_type == 'BYTES': arrow_type = pyarrow.binary(field.max_length) if field.max_length else pyarrow.binary()
                elif field.field_type in ('INTEGER', 'INT64'): arrow_type = pyarrow.int64()
                elif field.field_type in ('FLOAT', 'FLOAT64', 'NUMERIC', 'BIGNUMERIC'): arrow_type = pyarrow.float64()
                elif field.field_type == 'BOOLEAN': arrow_type = pyarrow.bool_()
                elif field.field_type == 'TIMESTAMP': arrow_type = pyarrow.timestamp('us', tz='UTC')
                
                if arrow_type:
                    pyarrow.Array.from_pandas(series, type=arrow_type)

            except (pyarrow.lib.ArrowInvalid, pyarrow.lib.ArrowTypeError) as e:
                logger.error(f"!!! >>> SCHEMA COLUMN '{col_name}' FAILED conversion! <<< !!!")
                logger.error(f"BigQuery Type specified: {field.field_type}")
                logger.error(f"Pandas Series dtype: {series.dtype}")
                logger.error(f"First 5 values of '{col_name}':\n{series.head().to_string()}")
                logger.error(f"Arrow Conversion Error: {e}")
                passed_schema_check = False
                raise ValueError(f"Data in schema column '{col_name}' is incompatible.") from e
        
        if passed_schema_check:
             logger.info("Part 1: All columns in the explicit schema passed the check.")

        # Part 2: Check extra columns handled by autodetect
        logger.info("Part 2: Verifying extra columns not in schema (handled by autodetect)...")
        extra_cols = [col for col in df.columns if col not in schema_cols]
        
        if not extra_cols:
            logger.info("Part 2: No extra columns found. The error source is still a mystery if failure occurs.")
        else:
            logger.warning(f"Found extra columns not in schema: {extra_cols}")
            for col_name in extra_cols:
                try:
                    series = df[col_name]
                    logger.info(f"Checking extra column '{col_name}'...")
                    # Let pyarrow infer type, simulating autodetect
                    pyarrow.Array.from_pandas(series)
                    logger.info(f"Extra column '{col_name}' PASSED basic conversion.")
                except (pyarrow.lib.ArrowInvalid, pyarrow.lib.ArrowTypeError) as e:
                    logger.error(f"!!! >>> EXTRA COLUMN '{col_name}' FAILED Arrow conversion! <<< !!!")
                    logger.error("This column is processed by 'autodetect=True' and is the likely source of the error.")
                    logger.error(f"Pandas Series dtype: {series.dtype}")
                    logger.error(f"First 5 values of '{col_name}':\n{series.head().to_string()}")
                    logger.error(f"Arrow Conversion Error: {e}")
                    raise ValueError(f"Data in extra column '{col_name}' failed during autodetect processing.") from e
        # --- End of temporary debugging code ---

        load_job = bq_client.load_table_from_dataframe(df, staging_ref, job_config=load_job_config)
        load_job.result() # Wait for the load to complete
        logger.info(f"Loaded {len(df)} rows to staging table.")

        # B. Check if target table exists
        try:
            bq_client.get_table(table_ref)
            table_exists = True
        except NotFound:
            table_exists = False

        if not table_exists:
            # First run: Copy staging table to target table
            logger.info(f"Target table {table_ref} not found. Creating it for the first time.")
            copy_job_config = bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE")
            copy_job = bq_client.copy_table(staging_ref, table_ref, job_config=copy_job_config)
            copy_job.result()
            logger.info("Target table created successfully.")
        else:
            # Subsequent runs: Perform MERGE (Upsert)
            logger.info(f"Target table {table_ref} exists. Performing MERGE.")
            columns_list = ", ".join([f"`{c}`" for c in df.columns])
            source_columns_list = ", ".join([f"S.`{c}`" for c in df.columns])

            merge_query = f"""
            MERGE `{table_ref}` T
            USING `{staging_ref}` S
            ON T.row_id = S.row_id
            WHEN NOT MATCHED THEN
              INSERT ({columns_list}) VALUES ({source_columns_list})
            """
            query_job = bq_client.query(merge_query)
            query_job.result()
            logger.info("MERGE operation completed.")

        # C. Cleanup: Delete Staging Table
        bq_client.delete_table(staging_ref, not_found_ok=True)
        logger.info("Staging table cleaned up.")

    except Exception as e:
        logger.error(f"Failed to process Airbnb CSV: {str(e)}", exc_info=True)
        raise e
