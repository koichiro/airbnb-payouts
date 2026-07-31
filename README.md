# Airbnb Earnings to BigQuery Pipeline

[![Ruby](https://img.shields.io/badge/ruby-4.0.6-red.svg)](https://www.ruby-lang.org/)
[![GCP](https://img.shields.io/badge/GCP-Cloud%20Run-orange.svg)](https://cloud.google.com/run)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Stop manual copy-pasting. Transition your Airbnb management from messy spreadsheets to a robust data warehouse.**

This project provides an automated ETL pipeline that triggers when an Airbnb earnings CSV is uploaded to Google Cloud Storage (GCS). It cleanses the data and performs an **UPSERT (MERGE)** into BigQuery, ensuring your financial records are always up-to-date and free of duplicates.

This implementation is written in **Ruby** and is designed to run on **Cloud Run** with **Eventarc**. It preserves the behavior of the original Python pipeline as closely as possible, except for unavoidable language-level differences.

---

## 🚀 Key Features

* **Automated ETL**: Fully event-driven. Upload a file to GCS, and your data appears in BigQuery seconds later.
* **Idempotency (Smart Upsert)**: Implements SHA256 row hashing. It uniquely identifies every entry, including payouts without confirmation codes, preventing duplicate rows when the same file is uploaded multiple times.
* **Data Cleansing & Normalization**:
    * Maps Japanese headers to standardized English column names.
    * Converts US-style dates (`MM/DD/YYYY`) to ISO-compatible values for BigQuery.
    * Normalizes financial columns with `BigDecimal` for BigQuery `NUMERIC` compatibility.
    * Preserves unmapped source columns and emits actionable warnings when Airbnb changes the export format.
* **Cloud Run Ready**: Accepts HTTP requests from Eventarc and supports both structured CloudEvent payloads and raw event-style payloads.
* **BI & Analytics Ready**: Query with SQL or connect BigQuery to Google Sheets or Looker Studio for financial dashboards.
* **Tested for OSS Use**: Includes a `Minitest` suite with `SimpleCov` coverage enforcement. Current coverage is above 80%.

## 🏗 Architecture

1. **Storage**: Google Cloud Storage (trigger bucket).
2. **Routing**: Eventarc (object finalized trigger).
3. **Compute**: Cloud Run (Ruby + Rack/Puma).
4. **Warehouse**: Google BigQuery.
5. **Interface**: Google Sheets (Connected Sheets) or Looker Studio.

## ⚙️ Configuration

The pipeline is controlled via environment variables in Cloud Run.

| Variable | Description | Default |
| :--- | :--- | :--- |
| `GCP_PROJECT_ID` | Your Google Cloud Project ID. | - |
| `BQ_DATASET_ID` | Destination BigQuery dataset ID. | `airbnb_management` |
| `BQ_TABLE_ID` | Destination BigQuery table ID. | `earnings_cleaned` |
| `PORT` | HTTP port used by Cloud Run. | `8080` |

## 🛠 Setup & Deployment

### 1. Prerequisites (IAM Roles)
Ensure the Cloud Run service account has the following permissions:
* `Storage Object Viewer`: To read CSV files from GCS.
* `BigQuery Data Editor`: To insert, copy, and merge data into tables.
* `BigQuery Job User`: To run query and load jobs.

If you deploy the Eventarc trigger from CI/CD or the command line, that identity also needs the relevant Eventarc and Cloud Run administration permissions.

### 2. Local Setup

```bash
bundle install
bundle exec puma -C config/puma.rb
```

Health check:

```bash
curl http://localhost:8080/up
```

### 3. Testing

```bash
bundle exec rake test
```

### 4. Dependency Auditing

Dependabot checks for Bundler and GitHub Actions updates every week. GitHub Actions also checks `Gemfile.lock` against the [Ruby Advisory Database](https://github.com/rubysec/ruby-advisory-db) on relevant pull requests, pushes to `main`, and a weekly schedule.

Run the same vulnerability audit locally with:

```bash
gem install bundler-audit --no-document
bundle-audit check --update
```

### 5. Deployment
Deploy the Cloud Run service with `deploy.sh` or through Cloud Build (`cloudbuild.yaml`). Create the Eventarc trigger separately with `scripts/create_trigger.sh`.

```bash
chmod +x deploy.sh scripts/create_trigger.sh

SERVICE_NAME=airbnb-payouts-import \
REGION=asia-northeast1 \
PROJECT_ID=your-project-id \
SERVICE_ACCOUNT_EMAIL=etl-runner@your-project-id.iam.gserviceaccount.com \
BQ_DATASET_ID=airbnb_management \
BQ_TABLE_ID=earnings_cleaned \
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/... \
./deploy.sh
```

Create or update the Eventarc trigger separately:

```bash
SERVICE_NAME=airbnb-payouts-import \
TRIGGER_NAME=airbnb-payouts-import-gcs-finalized \
REGION=asia-northeast1 \
PROJECT_ID=your-project-id \
TRIGGER_BUCKET=your-bucket \
SERVICE_ACCOUNT_EMAIL=etl-runner@your-project-id.iam.gserviceaccount.com \
./scripts/create_trigger.sh
```

## 📊 Usage

1. Export your **Transaction History** CSV from the Airbnb hosting dashboard.
2. Upload the CSV to your designated GCS bucket.
3. Eventarc sends the object-finalized event to Cloud Run.
4. The service cleans, stages, and merges the data into BigQuery.
5. Receive a notification in Slack (if configured).
6. Analyze your data in BigQuery, Google Sheets, or Looker Studio.

### Slack Notifications

When `SLACK_WEBHOOK_URL` is provided, the pipeline sends a rich attachment message to your channel:

*   **Success**: Shows the filename, import mode (Full Import for new tables vs. Merge Import for existing tables), and the count of inserted and updated rows.
*   **Failure**: Sends an alert with the error message and filename to help you troubleshoot quickly (e.g., schema mismatches or permission issues).

## Notes

* Like the original implementation, this project uses a staging table and then performs a `MERGE` into the target table.
* Like the original implementation, if Airbnb introduces a new column and your target BigQuery table schema is not updated, the merge can fail until the schema is aligned.
* The service entrypoint is HTTP-based because Cloud Run receives events through Eventarc rather than Cloud Functions-style `event, context` handlers.

## 🤝 Contributing
Contributions are welcome. Feel free to open an issue or submit a pull request if you have ideas for improvement.

## 📝 License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
