import os
import io
import logging
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage

# Logging configuration
logging.basicConfig(level=logging.INFO)

def load_airbnb_csv(event, context=None):
    """
    event: GCS event data
    context: Event metadata (None if Gen 2)
    """
    if context is None:
        logging.info("DEBUG: Executing as Gen 2")
        data = event.data
    else:
        logging.info("DEBUG: Executing as Gen 1")
        data = event

    bucket_name = data['bucket']
    file_name = data['name']

    logging.info(f"Processing file: {file_name} from bucket: {bucket_name}")

    if not file_name.lower().endswith('.csv'):
        return

    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset_id = os.environ.get("BQ_DATASET_ID", "airbnb_management")
    table_id = os.environ.get("BQ_TABLE_ID", "earnings_cleaned")

    try:
        # Read from GCS
        storage_client = storage.Client()
        blob = storage_client.bucket(bucket_name).blob(file_name)
        content = blob.download_as_bytes()

        # Strip any invisible whitespace from headers after loading
        df = pd.read_csv(io.BytesIO(content), encoding='utf-8-sig')
        df.columns = df.columns.str.strip()

        # Mapping
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
            '総収入': 'total_income',
            '宿泊税': 'accommodation_tax',
            'ホスティング収入年度': 'hosting_revenue_fiscal_year'
        }
        
        # Extract only existing columns
        existing_cols = [c for c in COLUMN_MAP.keys() if c in df.columns]
        df = df[existing_cols].rename(columns=COLUMN_MAP)


        # 【NULL Prevention】Explicitly specify date format (Airbnb uses MM/DD/YYYY)
        # Failure to specify format causes inference failure, resulting in NULL (NaN)
        date_cols = ['event_date', 'payout_scheduled_date', 'booking_date', 'start_date', 'end_date']
        for col in date_cols:
            if col in df.columns:
                # Airbnb format “12/28/2025” aligned
                df[col] = pd.to_datetime(df[col], format='%m/%d/%Y', errors='coerce').dt.date

        # Cleaning Numeric Columns (Preventing Mixing of Strings That Cause NULL Values)
        num_cols = ['amount', 'service_fee', 'cleaning_fee', 'total_income']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Load to BigQuery
        bq_client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        
        # Job settings for loading directly from DataFrames to save memory
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            # First time: Create tables using autodetect; subsequent times: Maintain schema
            autodetect=True 
        )

        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

        logging.info(f"Successfully loaded {len(df)} rows to {table_id}.")

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise e
