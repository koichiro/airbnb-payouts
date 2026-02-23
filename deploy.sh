#!/bin/bash

# 変数の設定（ご自身の環境に合わせて書き換えてください） 
FUNCTION_NAME="airbnb-payouts-import"
REGION="asia-northeast1" 
TRIGGER_BUCKET="[あなたのバケット名]" # 例: airbnb-payouts-raw 
GCP_PROJECT_ID="airbnb-management-488303"

echo "Deploying ${FUNCTION_NAME}..." gcloud functions 

deploy ${FUNCTION_NAME} \
  --gen2 \ --runtime python310 \ --entry-point load_airbnb_csv_improved \ --region ${REGION} \ 
  --trigger-event-type google.cloud.storage.object.v1.finalized \ --trigger-location ${REGION} \ 
  --trigger-bucket ${TRIGGER_BUCKET} \ --set-env-vars 
  GCP_PROJECT_ID=${GCP_PROJECT_ID},BQ_DATASET_ID=airbnb_management,BQ_TABLE_ID=earnings_cleaned \ 
  --source .

echo "Deployment complete."

