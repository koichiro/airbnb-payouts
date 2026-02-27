#!/bin/bash

# Configuration (Please update these values for your environment)
FUNCTION_NAME="airbnb-payouts-import"
REGION="asia-northeast1"
TRIGGER_BUCKET="YOUR_BUCKET_NAME" # e.g., airbnb-payouts-raw
GCP_PROJECT_ID="YOUR_PROJECT_ID"

echo "Deploying ${FUNCTION_NAME}..."

gcloud functions deploy ${FUNCTION_NAME} \
  --gen2 \
  --runtime python312 \
  --memory 512MB
  --entry-point process_airbnb_csv \
  --region ${REGION} \
  --trigger-event-type google.cloud.storage.object.v1.finalized \
  --trigger-location ${REGION} \
  --trigger-bucket ${TRIGGER_BUCKET} \
  --set-env-vars GCP_PROJECT_ID=${GCP_PROJECT_ID},BQ_DATASET_ID=airbnb_management,BQ_TABLE_ID=earnings_cleaned \
  --source .

echo "Deployment complete."

