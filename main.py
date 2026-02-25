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
        # Running as Gen 2 (CloudEvent)
        logging.info("DEBUG: Executing as Gen 2")
        data = event.data
    else:
        # Running as Gen 1
        logging.info("DEBUG: Executing as Gen 1")
        data = event

    bucket_name = data['bucket']
    file_name = data['name']
    
    logging.info(f"Processing file: {file_name} from bucket: {bucket_name}")

    if not file_name.endswith('.csv'):
        logging.info("Not a CSV file. Skipping.")
        return

    # Environment variables
    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset_id = os.environ.get("BQ_DATASET_ID", "airbnb_management")
    table_id = os.environ.get("BQ_TABLE_ID", "earnings_cleaned")

    try:
        # 1. Read from GCS
        storage_client = storage.Client()
        blob = storage_client.bucket(bucket_name).blob(file_name)
        content = blob.download_as_bytes()

        # 2. Data cleansing with Pandas
        df = pd.read_csv(io.BytesIO(content), encoding='utf-8-sig')

        # Mapping
        COLUMN_MAP = {
            '日付': 'event_date',
            '入金予定日': 'payout_scheduled_date',
            '種別': 'type',
            '確認コード': 'confirmation_code',
            '予約日': 'booking_date',
            '開始日': 'start_date',
            '終了日': 'end_date',
            'リスティング': 'listing_name',
            '金額': 'amount',
            'サービス料': 'service_fee',
            '清掃料金': 'cleaning_fee',
            '総収入': 'total_income'
        }
        
        df = df[[c for c in COLUMN_MAP.keys() if c in df.columns]].rename(columns=COLUMN_MAP)

        # Date conversion (MM/DD/YYYY -> YYYY-MM-DD)
        date_cols = ['event_date', 'payout_scheduled_date', 'booking_date', 'start_date', 'end_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        # 3. Load to BigQuery
        bq_client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        
        # Configration for LoadJob
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND", # Append
            autodetect=True                   # If it dosen't exist, create table
        )
        
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

        logging.info(f"Successfully loaded {len(df)} rows.")

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise e
