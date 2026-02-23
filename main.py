import os
import io
import logging
import functions_framework
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage

# ロギング設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 環境変数（deploy.shまたはコンソールで設定）
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "airbnb_management")
TABLE_ID = os.environ.get("BQ_TABLE_ID", "earnings_cleaned")

@functions_framework.cloud_event
def load_airbnb_csv_improved(cloud_event):
    """GCSにアップロードされたAirbnbのCSVをBQへロードする"""
    data = cloud_event.data
    bucket_name = data['bucket']
    file_name = data['name']
    
    logger.info(f"Processing: gs://{bucket_name}/{file_name}")

    if not file_name.endswith('.csv'):
        logger.info("Skipping non-CSV file.")
        return

    try:
        # 1. GCSからファイル取得
        storage_client = storage.Client()
        blob = storage_client.bucket(bucket_name).blob(file_name)
        content = blob.download_as_bytes()

        # 2. Pandasでデータ整形
        df = pd.read_csv(io.BytesIO(content), encoding='utf-8-sig')
        
        # カラムマッピング
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
            '総収入': 'total_income'
        }
        
        available_cols = [c for c in COLUMN_MAP.keys() if c in df.columns]
        df = df[available_cols].rename(columns=COLUMN_MAP)

        # 日付型変換
        date_cols = ['event_date', 'payout_scheduled_date', 'booking_date', 'start_date', 'end_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        # 3. BigQueryロード
        bq_client = bigquery.Client()
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

        logger.info(f"Success: Loaded {len(df)} rows to {table_ref}")

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        raise e
