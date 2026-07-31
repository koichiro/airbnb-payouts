#!/bin/bash

set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-airbnb-payouts-import}"
REGION="${REGION:-asia-northeast1}"
PROJECT_ID="${PROJECT_ID:-YOUR_PROJECT_ID}"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_EMAIL:-YOUR_SERVICE_ACCOUNT_EMAIL}"
BQ_DATASET_ID="${BQ_DATASET_ID:-airbnb_management}"
BQ_TABLE_ID="${BQ_TABLE_ID:-earnings_cleaned}"

echo "Deploying Cloud Run service ${SERVICE_NAME} to ${REGION}..."

gcloud run deploy "${SERVICE_NAME}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --source . \
  --memory 512Mi \
  --service-account "${SERVICE_ACCOUNT_EMAIL}" \
  --no-allow-unauthenticated \
  --update-env-vars "GCP_PROJECT_ID=${PROJECT_ID},BQ_DATASET_ID=${BQ_DATASET_ID},BQ_TABLE_ID=${BQ_TABLE_ID}"

echo "Cloud Run deployment complete."
