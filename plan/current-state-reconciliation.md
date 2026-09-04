# Current-state reconciliation

## Purpose

`row_id` ベースの insert-only `MERGE` は、完全に同じ行の再投入を防ぎつつ監査履歴を残せる。一方、Airbnb の累積CSVで既存行が訂正または削除された場合、旧版も残るため分析時に二重計上され得る。

自然キー候補を本番データで集計した結果、Payout の `reference_code` は一意だったが、予約・解決金・調整金では `confirmation_code` と `event_date` の組み合わせも一意ではなかった。このため、自然キーを推測した update/delete は行わない。

## Authority model

- `${BQ_TABLE_ID}` は従来どおり append-only の監査履歴とする。
- `${BQ_TABLE_ID}_current` は分析用の現行状態とする。
- Airbnb の年次累積CSV 1ファイルを、そのファイルが示す対象年の完全なスナップショットとして扱う。
- current table は論理キー単位ではなく対象年の集合全体を置換する。これにより、原本内の正当な複数行を維持し、原本から削除された行だけを現行状態から除外できる。
- current table の利用開始後も監査履歴は削除しない。

## Eligible files

current-state reconciliation の対象は、次の命名規則に合うCSVだけとする。

```text
airbnb_01_<start-year>-<end-month>_<end-year>[ (<revision>)].csv
```

追加条件:

- `start-year` と全行の `event_date` 年が一致する
- 全行に `event_date` がある
- 全行が単一年に属する
- `end-year` は `start-year` と同じ、または翌年1月である
- 全行がファイル名から計算した対象期間内にある
- GCS の generation と作成時刻を取得できる

条件に合わない通常CSVは監査テーブルへ取り込むが、current table は変更しない。不正な累積CSVは誤った年次置換を防ぐため import を失敗させる。

## Snapshot ordering

`${BQ_TABLE_ID}_snapshot_state` は年ごとに、次を保存する。

- CSV本文のSHA-256 (`snapshot_id`)
- ファイル名から得た対象期間末日 (`snapshot_through`)
- GCS object の作成時刻
- GCS generation
- 行数と適用時刻

同一 `snapshot_id` かつ同一または古い対象期間の再配送は適用しない。ただし、取引がない月などでCSV本文が同一でも対象期間が先へ進んだ場合は、古いスナップショットへの巻き戻しを防ぐためsnapshot stateを進める。対象期間末日が古いスナップショットも適用しない。同じ対象期間では、GCS作成時刻、次にgenerationが新しいものだけを適用する。

## Modes

| Mode | Behavior |
| --- | --- |
| `off` | 監査テーブルだけを更新する |
| `dry_run` | 対象年、期間末日、行数、snapshot ID prefixをログへ出すがcurrent tableを変更しない |
| `apply` | snapshot orderingを満たす場合だけcurrent tableを年単位で置換する |

初期値は `dry_run` とする。ログにはCSV行、ゲスト名、確認コード、金額を出さない。

## Runtime flow

1. 実行ごとに一意な staging table を作成する。
2. 従来の `row_id` MERGEで監査テーブルへ追加する。
3. 対象ファイルならsnapshot metadataを検証する。
4. `dry_run` は候補情報だけをログに残す。
5. `apply` はcurrent tableとsnapshot state tableを必要に応じて作成する。
6. BigQuery transaction内で対象年を削除し、stagingの全行を挿入し、snapshot stateを更新する。
7. staging tableを削除する。

## Initial migration

1. 監査テーブルのexpiring snapshotを作成する。
2. `CURRENT_STATE_MODE=dry_run` でデプロイする。
3. 各年の最新累積CSVを再投入し、対象年・期間末日・行数が期待どおりであることをログで確認する。
4. `CURRENT_STATE_MODE=apply` に変更する。
5. 各年の最新累積CSVを再投入する。年の順序は問わない。
6. snapshot stateが各年1行であることを確認する。
7. current tableの年別件数と各最新原本の年別件数が一致することを確認する。
8. current tableに、対応する最新原本に存在しない `row_id` がないことを確認する。
9. Connected Sheets、Looker Studio、集計SQLの参照先をcurrent tableへ切り替える。

## Dry-run report

Cloud Runログで `Current-state reconciliation candidate` を検索する。確認対象は `event_year`、`through_date`、`row_count`、`snapshot_id` prefix、`mode` のみとする。個別行や金額をログへ追加しない。

## Rollback

1. `CURRENT_STATE_MODE=off` に変更してcurrent table更新を停止する。
2. 分析参照先を従来の監査テーブルへ戻す。
3. current tableの内容に問題がある場合は、正しい年次累積CSVを新しいGCS objectとして再投入する。
4. 監査テーブルはcurrent reconciliationで削除・更新しないため、履歴復旧は不要である。
5. snapshot stateを手動変更する場合は、事前に両current関連tableのsnapshotを作成する。
