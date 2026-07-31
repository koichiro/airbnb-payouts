# Airbnb Payout Importer

## Goal

Airbnb の earnings / transaction history CSV を Google Cloud Storage 経由で受け取り、Cloud Run 上の Ruby サービスで BigQuery に継続取り込みする。

このリポジトリでは、CSV の正規化、重複排除、BigQuery への staging + `MERGE`、任意の Slack 通知までを扱う。

## Current Inputs

想定入力と現実装から読み取れる前提:

- 日本語ヘッダを英語のスネークケースへマッピングする
- `Airbnb remitted tax` と `Airbnbが納税する自動設定された税金` は同一列として扱う
- 日付は `MM/DD/YYYY` 形式を前提に `DATE` へ変換する
- 金額列は `BigDecimal` で BigQuery `NUMERIC` 互換へ正規化する
- 空文字は `NULL` として扱う
- 未知ヘッダは raw 名のまま保持しつつ warning を出す
- 各行の内容から `row_id` を計算し、同一ファイル再投入時の重複を防ぐ

## Delivery Scope

このリポジトリで継続的に扱う対象:

1. Airbnb CSV の取り込み要件整理
2. Cloud Run サービスの HTTP エントリポイント
3. CSV 正規化ロジック
4. BigQuery 取り込みと `MERGE`
5. デプロイ手順
6. テストと運用ドキュメント

## Design Principles

- Ruby + Rack/Puma + Cloud Run の小さな構成を維持する
- CSV の元情報を落としすぎず、分析しやすい英語カラムへ正規化する
- BigQuery では staging table 経由でロードし、本番テーブルへ `MERGE` する
- 重複排除は自然キーではなく `row_id` ベースで行う
- 未知ヘッダや失敗は握り潰さず、ログで検知できるようにする
- Slack 通知は optional とし、未設定でも本体処理は動くようにする
- テストは `Minitest` で維持し、`bundle exec rake test` を標準コマンドにする
- カバレッジは 80% 以上を維持目標とする

## Expected Runtime Flow

1. Airbnb から earnings CSV をダウンロードする
2. CSV を GCS バケットへアップロードする
3. Eventarc が Cloud Run サービスへ object finalized イベントを配送する
4. サービスが GCS から CSV を取得する
5. CSV を正規化し `row_id` を付与する
6. BigQuery staging table へ JSON Lines としてロードする
7. 本番テーブルに対して `row_id` ベースの `MERGE` を実行する
8. 成功時または失敗時にログを残し、必要に応じて Slack 通知する

## Non-Goals For Initial Version

- Airbnb API からの自動ダウンロード
- CSV 以外の入力形式への対応
- ダッシュボード自動作成
- 会計システムや仕訳システムとの直接連携
- BigQuery 集計ビューの自動生成

## Known Risks

- Airbnb の CSV 列構成は予告なく変わる可能性がある
- 現実装は空文字を `NULL` 化するが、`N/A` 相当値の追加正規化は今後の余地がある
- `CSV.parse` は現状 `liberal_parsing` を使っていないため、崩れた CSV には弱い可能性がある
- 未知列を raw 名のまま保持すると、既存 BigQuery schema と不一致になりうる
- ゲスト名や予約関連情報など、取り扱いに注意が必要なデータを含む

## Decisions To Preserve

- `row_id` を重複排除キーとして維持する
- `MERGE` は初版では insert-only とし、既存行の更新は行わない
- structured CloudEvent と raw payload の両方を受け付ける
- BigQuery へのロード前に schema で型を明示する
- optional な通知機構は本体ロジックから分離したまま保つ

## Tooling Requirements

- `gh` コマンドは必ずネットワーク権限付きで実行する
- 公開 OSS として、GitHub の Issue と Pull Request はタイトル・本文ともに英語で作成する
