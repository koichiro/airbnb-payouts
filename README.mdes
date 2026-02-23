# Airbnb Earnings Automation

Airbnbの収支CSVをGoogle Cloud Storageにアップロードすると、自動的にBigQueryへロードし、スプレッドシートで分析可能にするパイプライン。 

## 構成

- **Cloud Storage**: CSVアップロード用
- **Cloud Functions (Gen 2)**: データ加工・ロード
- **BigQuery**: データ蓄積 - **Connected Sheets**: 可視化・分析

## セットアップ

1. `deploy.sh` 内のバケット名を修正
2. `./deploy.sh`を実行

## 備考

- 日付形式 `MM/DD/YYYY` を `YYYY-MM-DD` に自動変換します。
- 日本語カラム名を分析しやすい英名に変換します。

