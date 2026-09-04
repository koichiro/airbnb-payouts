# Requirements

## Objective

Airbnb の earnings CSV を GCS アップロード契機で自動処理し、分析可能な形で BigQuery に蓄積する。

## Business Requirements

- 手作業での CSV 整形や BigQuery 手動 import をなくす
- Airbnb の入金・予約関連データを継続的に BigQuery へ蓄積できる
- 同じ CSV を再投入しても重複しない
- 月次収支やリスティング単位の分析に使える列構造にする
- 将来の CSV フォーマット変化を検知しやすくする

## Functional Requirements

### Ingestion

- GCS の object finalized イベントを受けて Cloud Run が起動すること
- structured CloudEvent と raw event payload の両方を処理できること
- 対象は `.csv` 拡張子ファイルのみとすること
- GCS 上の CSV ファイルをサービスが読み込めること

### Parsing

- UTF-8 BOM 付き CSV を処理できること
- 日本語ヘッダを内部の英語カラム名にマッピングできること
- 未知のヘッダを検知して warning ログを出せること
- 空セルや前後空白を正規化できること

### Normalization

- `MM/DD/YYYY` 形式の日付を BigQuery `DATE` 向けに変換できること
- 金額列を BigQuery `NUMERIC` 互換の値へ正規化できること
- 泊数や年度などの整数列を `INTEGER` として扱えること
- 変換不能な日付や数値は `NULL` として扱えること
- `row_id` を各行から安定的に生成できること

### Loading

- 正規化データを BigQuery staging table にロードできること
- dataset が未作成なら初回作成できること
- 本番テーブルが未作成なら staging table から作成できること
- 本番テーブルが既存なら `row_id` をキーに `MERGE` できること
- 同じ行を再投入しても重複行が増えないこと
- 監査用の本番テーブルは insert-only のまま保持すること
- 年次累積CSVは対象年の current-state table を置換できること
- 古いスナップショットの再配送で current-state table が巻き戻らないこと
- 自然キーを推測して異なる取引を統合しないこと
- 並行取り込みが同じ staging table を共有しないこと

### Observability

- 成功時に件数やファイル名をログ出力できること
- 失敗時にエラー内容と対象ファイルをログ出力できること
- Slack webhook が設定されている場合のみ通知できること

## Data Requirements

- 初版では `Schema::JOB_SCHEMA` に定義された列を BigQuery 管理対象とする
- BigQuery テーブルは earnings CSV の 1 行粒度とする
- 一意キーは自然キー依存ではなく、正規化後の行内容から計算したハッシュを採用する
- 予約確認コードが空でも重複排除できること
- 既知ヘッダ以外の列は raw 名で保持し、将来の schema 更新判断に使えること

## Security And Privacy Requirements

- Cloud Run は未認証公開しない
- サービスアカウントには最小権限を付与する
- BigQuery へのアクセス権は必要な利用者だけに制限する
- ログに不要な個人情報を過剰出力しない
- Slack 通知には調査に必要な最小限の情報だけを載せる

## Operational Requirements

- 環境変数で GCP project、dataset、table、port を切り替えられること
- ローカル実行と自動テストが可能であること
- 失敗したファイルを再投入してリカバリできること
- current-state reconciliation は `off` / `dry_run` / `apply` を切り替えられること
- `bundle exec rake test` でフルテストを実行できること
- テストフレームワークは `Minitest` を採用すること
- コードカバレッジは 80% 以上を維持すること

## Test Requirements

- `App` の HTTP エントリポイントをテストすること
- `Processor` のイベント処理と通知分岐をテストすること
- `CsvTransformer` のヘッダ変換、型変換、未知列警告をテストすること
- `BigqueryGateway` の load / copy / merge / cleanup を fake で検証すること
- `SlackNotifier` の payload 形成とエラーログをテストすること

## Out Of Scope

- Airbnb API からの自動ダウンロード
- CSV 以外のデータソース
- Looker Studio や Google Sheets の自動セットアップ
- 会計ロジックや仕訳作成

## Open Questions

- `N/A` やローカライズ差分など、空文字以外の null 相当値を追加で吸収するか
- 壊れた CSV への耐性を高めるため `liberal_parsing` 相当を導入するか
- 監査列として `source_file_name` や `imported_at` を本体 schema に含めるか
- insert-only merge のままで十分か、将来 update を許可するか
