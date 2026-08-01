# Google Drive to Cloud Storage Sync

## Purpose

Airbnb の earnings / transaction history CSV を指定 Google Drive フォルダへ配置すると、Google Apps Script（GAS）が定期的に検出し、既存 Eventarc trigger の対象 GCS bucket へ転送する。

```text
Google Drive input folder
  -> GAS time-driven trigger
  -> gs://<AIRBNB_CSV_BUCKET>/drive/<file-id>/<sha256>/<filename>.csv
  -> Google Drive processed folder
  -> Eventarc
  -> Cloud Run airbnb-payouts-import
  -> BigQuery airbnb_management.earnings_cleaned
```

GAS は [apps-script/Code.gs](../apps-script/Code.gs) と [apps-script/appsscript.json](../apps-script/appsscript.json) を Apps Script editor へ登録して運用する。GCS 保存後の Eventarc、Cloud Run、BigQuery 処理は既存構成を変更しない。

参考実装: [amazon-purchasing-importer Google Drive sync](https://github.com/koichiro/amazon-purchasing-importer/blob/main/plan/google-drive-sync.md)

## Design invariants

### Layered idempotency

二重処理を次の三層で防止する。

1. `LockService` で同一 GAS project の並行実行を避ける
2. Drive file ID と CSV 本文 SHA-256 から決定した object 名、および `ifGenerationMatch=0` で同一 revision の GCS object 再作成を防ぐ
3. BigQuery の `row_id` をキーにした insert-only `MERGE` で同一行の再投入を防ぐ

GCS object 名は次の形式とする。

```text
drive/<Drive file ID>/<CSV bytes SHA-256>/<sanitized original filename>.csv
```

同じ Drive file ID と同じ本文を再検出した場合、GCS は `412 Precondition Failed` を返す。GAS はこれを失敗ではなく保存済み確認として扱う。Drive の更新日時や Script Properties の状態は重複判定に使用しない。

同じ Drive file の本文が更新されると SHA-256 が変わり、別 object になる。BigQuery の `row_id` は行内容から生成されるため、完全に同じ行は増えない。一方、修正された行は旧行と新行が共存し得る。訂正版は元 CSV を上書きせず、新しいファイル名で配置する。

### Move after durable storage

Drive file を processed folder へ移動する条件は、次のいずれかに限定する。

- GCS object の新規作成に成功した
- HTTP 412 により同じ immutable object がすでに存在すると確認できた

upload に失敗した場合、file は input folder に残り、次回実行で再試行される。upload 成功後の folder move に失敗した場合も input folder に残る。次回は 412 を確認して upload を増やさず、move だけを再試行できる。

### Data protection

Airbnb CSV はゲスト名、予約情報、リスティング情報を含み得る。ログと通知へ次を出さない。

- CSV 本文、セル値、GCS response body
- Drive filename または GCS object 名
- ゲスト名、予約コード、リスティング名

ログには Drive file ID、本文 hash、件数、処理時間、HTTP status から抽象化した error type のみを残す。failure email は Apps Script の Executions 画面へ誘導する汎用文面とする。

## Preconditions

設定前に次を確定する。

- [ ] GCP project ID
- [ ] Eventarc trigger 対象の GCS bucket
- [ ] Drive input folder ID
- [ ] Drive processed folder ID
- [ ] My Drive / shared drive の別
- [ ] GAS owner および trigger 作成者
- [ ] 失敗通知先 email（optional）

input folder と processed folder は別 ID とし、互いを入れ子にしない。GAS owner は両方の folder を編集し、file を移動できる必要がある。shared drive を使う場合は両 folder を同じ shared drive 内に配置して move 権限を事前検証する。

## GCP setup

### Enable APIs

```bash
PROJECT_ID="<PROJECT_ID>"

gcloud services enable \
  drive.googleapis.com \
  storage.googleapis.com \
  --project="${PROJECT_ID}"
```

### Grant create-only bucket access

時間主導 trigger は作成者の Google account で実行される。その account に対象 bucket 単位の `Storage Object Creator` を付与する。

```bash
BUCKET_NAME="<AIRBNB_CSV_BUCKET>"
GAS_EXECUTOR_EMAIL="<GAS_TRIGGER_OWNER_EMAIL>"

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
  --member="user:${GAS_EXECUTOR_EMAIL}" \
  --role="roles/storage.objectCreator"
```

project 全体の Editor、Storage Admin、object delete 権限は付与しない。GAS 用の service account key、OAuth client secret、refresh token は作成しない。

## Apps Script setup

### Create the folders and project

1. Drive に Airbnb CSV input folder と processed folder を作成する
2. GAS executor が両 folder を編集・移動できることを確認する
3. 新しい Apps Script project を作成する
4. time zone を `Asia/Tokyo` に設定する
5. Apps Script project を対象の標準 GCP project に関連付ける
6. [Code.gs](../apps-script/Code.gs) と [appsscript.json](../apps-script/appsscript.json) の内容を editor へ登録する

推奨 folder 名:

```text
Airbnb CSV - Input
Airbnb CSV - Processed
```

### Configure Script Properties

Apps Script の Project Settings から次を登録する。

| Property | Value | Required |
| :--- | :--- | :---: |
| `DRIVE_FOLDER_ID` | input folder ID | yes |
| `PROCESSED_FOLDER_ID` | processed folder ID | yes |
| `GCS_BUCKET_NAME` | Eventarc 対象 bucket 名 | yes |
| `GCS_OBJECT_PREFIX` | `drive` | yes |
| `MAX_FILE_SIZE_BYTES` | `20971520`（20 MiB） | yes |
| `ALERT_EMAIL` | 汎用 failure 通知先 | no |

認証情報や CSV の値を Script Properties に保存しない。

### OAuth scopes

manifest は次の操作に必要な scope を明示する。

- Drive file の取得と processed folder への移動
- GCS JSON API への upload
- `UrlFetchApp` による HTTP request
- installable trigger 作成
- optional failure email

OAuth token の scope が広くても、実際の GCS resource 操作は bucket 単位 IAM の create-only 権限で制限する。

## Initial smoke test

### Prepare a safe fixture

実データをそのまま使用せず、ゲスト名、予約コード、リスティング名を架空値へ置換した最小 CSV を一意な名前で input folder へ置く。

```text
drive-sync-smoke-<UTC timestamp>.csv
```

### Run manually

1. Apps Script editor で `syncAirbnbCsv` を選択する
2. Run を押して全 scope を認可する
3. Executions で実行成功を確認する
4. `drive_sync_completed` の `syncedCount: 1`、`movedCount: 1`、`failedCount: 0` を確認する
5. fixture が processed folder へ移動したことを確認する
6. GCS の object path が `drive/<file-id>/<64-char-sha256>/...csv` であることを operator 権限の端末から確認する
7. Cloud Run と BigQuery の import 成功を確認する

```bash
gcloud storage ls --recursive "gs://<AIRBNB_CSV_BUCKET>/drive/"

gcloud run services logs read airbnb-payouts-import \
  --project="<PROJECT_ID>" \
  --region="asia-northeast1" \
  --limit=100
```

## Idempotency test

1. smoke test の file を内容変更せず input folder へ戻す
2. `syncAirbnbCsv` を手動実行する
3. `drive_file_already_synced` と `movedCount: 1` を確認する
4. GCS object 数が増えていないことを確認する
5. BigQuery の対象 `row_id` 件数が増えていないことを確認する
6. 同じ Drive file の本文を変更した場合は別 hash path が作成されることを確認する

追加で次を確認する。

- `.txt`、Google Sheets、subfolder 内 CSV は upload されない
- `MAX_FILE_SIZE_BYTES` 超過 file は本文をログへ出さず skip される
- upload failure と move failure では file が input folder に残る
- trigger が重複せず 1 個だけ存在する

## Install the scheduled trigger

手動 smoke test と idempotency test の成功後、Apps Script editor から `installSyncTrigger` を一度だけ実行する。

trigger 設定:

| Setting | Value |
| :--- | :--- |
| Handler | `syncAirbnbCsv` |
| Event source | Time-driven |
| Interval | Every 5 minutes |
| Failure notification | Notify immediately |

installable trigger は作成者の account で実行され、別 user から見えない場合がある。作成者を運用記録へ残す。

## Normal operations

利用者:

1. Airbnb から CSV をダウンロードする
2. 内容を編集せず、input folder 直下へ配置する
3. processed folder への移動と import 結果を確認する
4. 失敗連絡では CSV や filename を転記せず、配置時刻だけを運用担当へ伝える

運用担当は Apps Script Executions で次を確認する。

- `scannedCount`, `syncedCount`, `skippedCount`, `movedCount`, `failedCount`
- `durationMs`
- Drive file ID と content hash

一時障害の解消後は `syncAirbnbCsv` を手動実行する。Script Properties に処理済み state はないため、input folder に残った対象 file がすべて再評価される。GCS precondition が最終的な object-level 重複防止になる。

## Troubleshooting

### Drive access denied

- trigger 作成者が両 folder を編集できるか確認する
- shared drive の member と move 権限を確認する
- Drive OAuth scope の認可を確認する
- input / processed folder ID の誤りと同一 ID 指定を確認する

### GCS HTTP 401 / 403

- Apps Script が対象の標準 GCP project に関連付いているか確認する
- Cloud Storage API が有効か確認する
- trigger 作成者に対象 bucket の `roles/storage.objectCreator` があるか確認する
- `devstorage.read_write` scope の認可を確認する

### GCS HTTP 412

同じ Drive file ID と同じ本文がすでに保存済みであることを示す正常な skip である。`drive_file_already_synced` の後に processed folder への move が行われることを確認する。

### GAS execution timeout

- input folder から対象外 file を移す
- 1 回の upload 数を減らす
- file size 上限を安易に増やさない
- 大量 file が常態化する場合は Drive API change tracking や queue-based architecture を別 Issue で検討する

### GCS object exists but importer did not run

- object 名が `.csv` で終わるか確認する
- bucket が Eventarc filter と一致するか確認する
- Eventarc trigger、Pub/Sub backlog、Cloud Run logs を確認する

Eventarc は at-least-once 配送である。同じ CloudEvent が再配送されても BigQuery `row_id` MERGE により同じ行は増えない。再配送と別 generation を区別する必要がある場合は、CloudEvent ID、source、bucket、object、generation の構造化ログ追加を別 Issue とする。

## Stop and rollback

緊急停止時は Apps Script project の `syncAirbnbCsv` trigger を削除する。コード、Script Properties、Drive file、GCS object、BigQuery row は削除しない。

再開手順:

1. `syncAirbnbCsv` を手動実行して成功を確認する
2. `installSyncTrigger` を実行する
3. trigger が 1 個だけ存在することを確認する

コードを戻す場合は Apps Script project history から正常 version の `Code.gs` と manifest へ戻す。rollback 中は trigger を停止し、手動 smoke test 後に再作成する。作成済み GCS object と BigQuery row は自動削除しない。

## Completion checklist

- [ ] input / processed folder、GAS executor、bucket を確定した
- [ ] GAS executor の GCS 権限が対象 bucket の create-only である
- [ ] service account key、OAuth client secret、refresh token を作成していない
- [ ] Script Properties と OAuth scopes を設定した
- [ ] PII-free fixture の manual sync が成功した
- [ ] 同一 revision の再検出で GCS object と BigQuery row が増えない
- [ ] upload 成功または 412 確認後だけ processed folder へ移動する
- [ ] upload / move failure file が input folder に残る
- [ ] 対象外 file が転送されない
- [ ] logs と notifications に CSV data や filename がない
- [ ] scheduled trigger が 1 個だけ存在する
- [ ] Drive to GCS to Eventarc to Cloud Run to BigQuery の E2E が成功した
- [ ] trigger 停止、手動再実行、rollback 手順を確認した

## Official references

- [Apps Script Lock service](https://developers.google.com/apps-script/reference/lock)
- [Apps Script File.moveTo](https://developers.google.com/apps-script/reference/drive/file#movetodestination)
- [Apps Script ClockTriggerBuilder](https://developers.google.com/apps-script/reference/script/clock-trigger-builder)
- [Cloud Storage request preconditions](https://cloud.google.com/storage/docs/request-preconditions)
- [Cloud Storage JSON API objects.insert](https://cloud.google.com/storage/docs/json_api/v1/objects/insert)
