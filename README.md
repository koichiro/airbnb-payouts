# Airbnb Earnings to BigQuery Pipeline

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![GCP](https://img.shields.io/badge/GCP-Cloud%20Functions-orange.svg)](https://cloud.google.com/functions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Stop manual copy-pasting. Transition your Airbnb management from messy spreadsheets to a robust data warehouse.**

This project provides an automated ETL pipeline that triggers when an Airbnb earnings CSV is uploaded to Google Cloud Storage (GCS). It cleanses the data and performs an **UPSERT (MERGE)** into BigQuery, ensuring your financial records are always up-to-date and free of duplicates.



---

## 🚀 Key Features

* **Automated ETL**: Fully event-driven. Upload a file to GCS, and your data appears in BigQuery seconds later.
* **Idempotency (Smart Upsert)**: Implements a SHA256 row-hashing logic. It uniquely identifies every entry (including Payouts without confirmation codes), preventing duplicate records even if the same file is uploaded multiple times.
* **Data Cleansing & Normalization**:
    * Maps Japanese headers to standardized English column names.
    * Converts US-style dates (`MM/DD/YYYY`) to ISO format (`YYYY-MM-DD`).
    * Sanitizes numeric fields and handles missing values (NaN to 0).
* **Hybrid Runtime Support**: Compatible with both **Cloud Functions Gen 1** and **Gen 2** (CloudEvent) signatures.
* **BI & Analytics Ready**: Query via SQL or use "Connected Sheets" in Google Sheets for real-time financial dashboards.

## 🏗 Architecture

1.  **Storage**: Google Cloud Storage (Trigger Bucket).
2.  **Compute**: Google Cloud Functions (Python + Pandas).
3.  **Warehouse**: Google BigQuery.
4.  **Interface**: Google Sheets (Connected Sheets) or Looker Studio.

## ⚙️ Configuration

The pipeline is controlled via environment variables in Cloud Functions.

| Variable | Description | Default |
| :--- | :--- | :--- |
| `GCP_PROJECT_ID` | Your Google Cloud Project ID. | - |
| `BQ_DATASET_ID` | Destination BigQuery Dataset ID. | `airbnb_management` |
| `BQ_TABLE_ID` | Destination BigQuery Table ID. | `earnings_cleaned` |

## 🛠 Setup & Deployment

### 1. Prerequisites (IAM Roles)
Ensure the Cloud Functions Service Account has the following permissions:
* `Storage Object Viewer`: To read CSV files from GCS.
* `BigQuery Data Editor`: To insert/merge data into tables.
* `BigQuery Job User`: To run query and load jobs.

### 2. Deployment
Deploy using the provided `deploy.sh` or through a CI/CD pipeline like Cloud Build (see `cloudbuild.yaml`).

```bash
chmod +x deploy.sh
./deploy.sh
```

## 📊Usage

1. Export your **"Transaction History"** CSV from the Airbnb Hosting Dashboard.
2. Upload the CSV to your designated GCS bucket.
3. The Cloud Function will automatically process and merge the data into BigQuery.
4. Analyze your data: Open Google Sheets > **Data > Data connectors > Connect to BigQuery**.


## 🤝 Contributing
Contributions are welcome! Feel free to open an issue or submit a pull request if you have ideas for improvement.

## 📝 License
This project is licensed under the MIT License - see the  [LICENSE](LICENSE) file for details.
