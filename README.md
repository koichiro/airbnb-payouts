# Airbnb Earnings Automation

A data pipeline that automatically loads Airbnb earnings CSV files into BigQuery when uploaded to Google Cloud Storage, enabling analysis via Google Sheets.

## Architecture

- **Cloud Storage**: For CSV uploads
- **Cloud Functions (Gen 2)**: Data processing and loading
- **BigQuery**: Data warehouse
- **Connected Sheets**: Visualization and analysis

## Setup

1. Update the bucket name in `deploy.sh`.
2. Run `./deploy.sh`.

## Notes

- Automatically converts date format from `MM/DD/YYYY` to `YYYY-MM-DD`.
- Renames Japanese column headers to English for easier analysis.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
