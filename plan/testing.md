# Testing Plan

## Goal

Airbnb CSV インポーターの主要ロジックを、ローカルで安定再現できる単体テスト中心で検証する。

初版のテスト方針:

- テストフレームワークは `Minitest`
- フルテストは `bundle exec rake test`
- カバレッジ計測は `SimpleCov`
- 目標カバレッジは 80% 以上
- ネットワークや実 BigQuery には接続しない

## Test Scope

対象コンポーネント:

- `App`
- `Processor`
- `CsvTransformer`
- `BigqueryGateway`
- `Schema`
- `SlackNotifier`
- Apps Script deployment artifacts（manifest、冪等性に必要な設定と処理順）

対象外:

- 実 GCP 環境への疎通確認
- Eventarc 自体の動作保証
- 実 BigQuery job の統合検証
- 実 Slack webhook 送信確認
- 実 Google Drive / Apps Script / GCS 間の疎通確認

## Directory Layout

現行ディレクトリ:

- `test/test_helper.rb`
- `test/app_test.rb`
- `test/processor_test.rb`
- `test/csv_transformer_test.rb`
- `test/bigquery_gateway_test.rb`
- `test/slack_notifier_test.rb`
- `test/google_drive_sync_artifacts_test.rb`

必要に応じて今後追加する候補:

- `test/schema_test.rb`
- `test/fixtures/airbnb/`
- `test/support/`

## Fixture Policy

### Basic Rules

- fixture は実サンプルに近い形を保つ
- ただし個人情報や予約情報は残さない
- fixture はテスト目的ごとに最小化し、1 ファイル 1 意図を原則とする
- 文字列の揺れ、未知ヘッダ、BOM など論点ごとに分ける

### Recommended Fixtures

将来 `test/fixtures/airbnb/` を導入する場合の候補:

- `valid_minimal.csv`
  - 1 行だけの最小正常系
- `valid_with_bom.csv`
  - BOM 付き正常系
- `valid_with_unknown_headers.csv`
  - 未知ヘッダ混在時の warning 確認用
- `valid_with_legacy_tax_header.csv`
  - `Airbnb remitted tax` を含む
- `valid_with_new_tax_header.csv`
  - `Airbnbが納税する自動設定された税金` を含む
- `invalid_bad_dates_and_numbers.csv`
  - 型変換失敗時の `NULL` フォールバック確認用

### Source Sample Handling

実運用で取得した CSV は参照用サンプルとして扱い、fixture にそのままコピーしない。

理由:

- 実データのままでは公開リポジトリ向けの匿名化が不十分になりうる
- 行数が多いと失敗原因の切り分けが遅くなる
- テストの意図がぼやける

## Stub And Fake Policy

### Principle

外部依存は stub または fake object で閉じ込め、テストの失敗原因をアプリケーションコードに限定する。

方針:

- HTTP 通信は発生させない
- GCS は fake storage client を使う
- BigQuery は fake bigquery client を使う
- Slack 通知は `Net::HTTP` を stub する
- 環境変数依存はテスト内で明示設定する

### BigqueryGateway Tests

`BigqueryGateway` のテストでは Google Cloud SDK を実際には呼ばない。

既存 fake:

- `FakeStorage`
- `FakeBucket`
- `FakeFile`
- `FakeSchema`
- `FakeLoadJob`
- `FakeCopyJob`
- `FakeQueryJob`
- `FakeTable`
- `FakeDataset`
- `FakeBigquery`

検証対象:

- dataset 未存在時の作成
- target table 未存在時の copy path
- target table 既存時の merge path
- `MERGE` SQL の生成
- DML 件数の抽出
- staging table cleanup
- job failure 時の例外伝播

### Processor Tests

`Processor` は gateway、transformer、notifier を dependency injection で差し替える。

検証対象:

- structured CloudEvent payload
- raw payload
- `.csv` 以外のスキップ
- 成功時の notifier 呼び出し
- 失敗時の notifier 呼び出し
- エラー再送出

### App Tests

`App` は Rack 経由でテストし、processor は fake object に置き換える。

検証対象:

- `GET /up`
- `POST /` 正常系
- 不正 JSON
- 未定義 route

### SlackNotifier Tests

`SlackNotifier` は `Net::HTTP` を stub し、payload 形成と失敗時ログを確認する。

検証対象:

- webhook URL の有無による `enabled?`
- success payload
- failure payload
- HTTP エラー時のログ出力

## Assertion Policy

- 文字列完全一致よりも、意味のある構造単位で検証する
- `MERGE` SQL は重要句を含むことを検証する
- ログ検証は全文一致ではなく warning/error を含むことを確認する
- `row_id` は固定値完全一致より、形式と安定性を優先して検証する

## Coverage Policy

### Target

- ラインカバレッジ 80% 以上

### Measurement

- `test/test_helper.rb` で `SimpleCov` を起動する
- `bundle exec rake test` 実行時に coverage レポートを生成する
- 80% 未満は hard fail とする

### Priorities

優先度が高い箇所:

- CSV 正規化
- `row_id` 生成
- BigQuery load / merge 分岐
- エラー処理
- 非 CSV スキップ

優先度が低い箇所:

- 単純な schema 定数
- ログ文言の細かな違い

## CI Readiness

将来 CI に載せるときの前提:

- 追加セットアップなしで `bundle exec rake test` が動く
- fake / stub のみで完結する
- GCP 認証情報を必要としない

## Open Decisions

- `Schema` 単体テストを追加するか
- fixture を本格導入するか、当面は inline CSV のまま進めるか
- 壊れた CSV を扱う場合に parser の寛容設定をどこまで許容するか
