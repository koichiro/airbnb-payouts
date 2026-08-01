# Specification

## Reference Architecture

- Source: Airbnb からダウンロードした earnings CSV
- Optional Intake: Google Drive + Google Apps Script
- Landing: Google Cloud Storage
- Trigger: Eventarc の `google.cloud.storage.object.v1.finalized`
- Compute: Ruby 製 Cloud Run サービス
- Warehouse: BigQuery
- Optional Notification: Slack Incoming Webhook

処理シーケンス:

1. CSV を GCS バケットへ直接アップロードする、または Google Drive input folder から GAS で転送する
2. Eventarc が Cloud Run にイベント配送
3. Cloud Run が対象バケットとオブジェクト名を取得
4. GCS から CSV 本文をダウンロード
5. CSV を正規化し `row_id` を付与
6. BigQuery staging table へ JSON Lines として load
7. 本番テーブルへ `MERGE`
8. staging table を削除
9. 必要に応じて Slack 通知

## Application Shape

初版は以下の責務に分ける。

- `App`: Eventarc からの HTTP リクエスト受付、health check、payload parse
- `Processor`: イベント解釈、CSV 対象判定、変換と保存のオーケストレーション
- `CsvTransformer`: ヘッダマッピング、前処理、型変換、未知列警告、`row_id` 生成
- `BigqueryGateway`: GCS download、BigQuery load、table create、merge、staging cleanup
- `Schema`: 列マッピング、型定義、BigQuery schema 定義
- `SlackNotifier`: success / failure 通知の送信

## Event Payload Handling

- structured CloudEvent と raw event payload の両方を受け付ける
- structured の場合は `payload["data"]` を使う
- raw の場合は payload 自体から `bucket` と `name` を読む
- ファイル名が `.csv` でない場合は処理せず終了する

## CSV Parsing Rules

- 文字コードは UTF-8 を前提にし、BOM を除去する
- 不正バイト列がある場合は UTF-8 に変換しつつ置換する
- ヘッダ名の前後空白は除去する
- セル値の前後空白は除去する
- 空文字は `NULL` 扱いにする
- 既知ヘッダは `Schema::COLUMN_MAP` に従って英語カラム名へ変換する
- 未知ヘッダは raw 名のまま保持し warning を出す

## Type Conversion Rules

- `Schema::DATE_COLUMNS` は `Date.strptime(value, "%m/%d/%Y")`
- `Schema::NUMERIC_COLUMNS` は `BigDecimal`
- `Schema::INTEGER_COLUMNS` は `Integer(Float(value))`
- 変換失敗時は例外を握り潰して `NULL` を返す

## Deduplication Strategy

自然キーが不足する行を含むため、`row_id` は正規化後行データのハッシュで生成する。

現行方針:

- schema 補完後の row 値列から SHA256 を計算する
- 本番テーブルへの `MERGE` 条件は `T.row_id = S.row_id`

初版の `MERGE` 動作:

- `WHEN NOT MATCHED THEN INSERT`

つまり insert-only merge とする。

Google Drive intake を使用する場合は、行単位の `row_id` に加えて object 単位の冪等性を持たせる。

- `LockService` で GAS の並行実行を避ける
- `drive/<Drive file ID>/<CSV SHA-256>/<sanitized filename>.csv` を object 名とする
- upload に `ifGenerationMatch=0` を指定し、HTTP 412 を保存済みとして扱う
- upload 成功または 412 確認後だけ Drive file を processed folder へ移動する

詳細は [google-drive-sync.md](./google-drive-sync.md) を参照する。

## BigQuery Schema Policy

BigQuery schema は `Schema::JOB_SCHEMA` に定義する。

現行列:

- Date
  - `event_date`
  - `payout_scheduled_date`
  - `booking_date`
  - `start_date`
  - `end_date`
- String
  - `type`
  - `confirmation_code`
  - `guest`
  - `listing_name`
  - `details`
  - `reference_code`
  - `currency`
  - `row_id`
- Numeric
  - `amount`
  - `paid`
  - `service_fee`
  - `express_transfer_fee`
  - `cleaning_fee`
  - `pet_fee`
  - `total_income`
  - `accommodation_tax`
  - `airbnb_remitted_tax`
- Integer
  - `number_of_nights`
  - `hosting_revenue_fiscal_year`

`row_id` は required とする。

## Header Mapping Guidelines

少なくとも以下の対応を維持する。

- `日付` -> `event_date`
- `入金予定日` -> `payout_scheduled_date`
- `種別` -> `type`
- `確認コード` -> `confirmation_code`
- `予約日` -> `booking_date`
- `開始日` -> `start_date`
- `終了日` -> `end_date`
- `泊数` -> `number_of_nights`
- `ゲスト` -> `guest`
- `リスティング` -> `listing_name`
- `詳細` -> `details`
- `参照コード` -> `reference_code`
- `通貨` -> `currency`
- `金額` -> `amount`
- `支払い済み` -> `paid`
- `サービス料` -> `service_fee`
- `スピード送金の手数料` -> `express_transfer_fee`
- `清掃料金` -> `cleaning_fee`
- `ペット料金` -> `pet_fee`
- `総収入` -> `total_income`
- `宿泊税` -> `accommodation_tax`
- `Airbnb remitted tax` -> `airbnb_remitted_tax`
- `Airbnbが納税する自動設定された税金` -> `airbnb_remitted_tax`
- `ホスティング収入年度` -> `hosting_revenue_fiscal_year`

## Environment Variables

初版で想定する環境変数:

- `GCP_PROJECT_ID`
- `BQ_DATASET_ID`
- `BQ_TABLE_ID`
- `PORT`
- `SLACK_WEBHOOK_URL` optional

## IAM Requirements

Cloud Run サービスアカウント:

- `Storage Object Viewer`
- `BigQuery Data Editor`
- `BigQuery Job User`

デプロイ実行主体:

- Cloud Run, Eventarc, IAM 関連の必要権限

## Deployment Shape

デプロイは `deploy.sh` / `scripts/deploy.sh` と `cloudbuild.yaml` を基準にする。

- `gcloud run deploy --source .`
- Eventarc trigger 作成は `scripts/create_trigger.sh` で別実行
- Cloud Run 環境変数注入
- 必要に応じて Slack webhook を設定

## Testing Strategy

- テストフレームワークは `Minitest`
- フルテストは `bundle exec rake test`
- `SimpleCov` によりカバレッジ 80% 未満で失敗させる
- 外部依存は fake / stub で置き換える

推奨テスト構成:

- `test/app_test.rb`
- `test/processor_test.rb`
- `test/csv_transformer_test.rb`
- `test/bigquery_gateway_test.rb`
- `test/slack_notifier_test.rb`

重点テストケース:

- BOM 付き CSV を読める
- ヘッダマッピングが正しい
- 新旧 2 種類の Airbnb tax 列を同一カラムへ吸収できる
- 日付、数値、整数の型変換
- 未知ヘッダの warning
- `.csv` 以外のファイルをスキップする
- 初回 copy path と既存 table merge path
- Slack 通知の success / failure payload

## Planned Follow-Up Documents

- `plan/data-quality.md`: null 正規化や変換失敗時の扱いを詰める場合に追加
- `plan/deployment.md`: 実運用向けのデプロイ手順を詳細化する場合に追加
- `plan/schema-evolution.md`: Airbnb の列変更への追従方針を詳細化する場合に追加
