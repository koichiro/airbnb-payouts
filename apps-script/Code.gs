const REQUIRED_PROPERTIES = [
  'DRIVE_FOLDER_ID',
  'PROCESSED_FOLDER_ID',
  'GCS_BUCKET_NAME',
  'GCS_OBJECT_PREFIX',
  'MAX_FILE_SIZE_BYTES',
];

/**
 * Polls the configured Drive folder and creates immutable CSV objects in GCS.
 * This function is the entrypoint for both manual runs and the time-driven trigger.
 */
function syncAirbnbCsv() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(1000)) {
    logEvent_('WARNING', 'drive_sync_skipped', {
      reason: 'another_execution_is_running',
    });
    return;
  }

  const startedAt = Date.now();
  const counts = { scanned: 0, synced: 0, skipped: 0, moved: 0, failed: 0 };

  try {
    const config = loadConfig_();
    const inputFolder = DriveApp.getFolderById(config.driveFolderId);
    const processedFolder = DriveApp.getFolderById(config.processedFolderId);
    const files = inputFolder.getFiles();

    while (files.hasNext()) {
      const file = files.next();
      counts.scanned += 1;

      if (!isTargetCsv_(file, config)) {
        counts.skipped += 1;
        continue;
      }

      try {
        const uploadResult = uploadFileToGcs_(file, config);
        counts[uploadResult] += 1;

        // A successful create or a 412 proves that the immutable object exists.
        // If this move fails, the file remains available for a safe retry. The
        // retry receives 412 and attempts the move again without another object.
        file.moveTo(processedFolder);
        counts.moved += 1;
        logEvent_('INFO', 'drive_file_moved', {
          driveFileId: file.getId(),
        });
      } catch (error) {
        counts.failed += 1;
        logEvent_('ERROR', 'drive_file_sync_failed', {
          driveFileId: file.getId(),
          errorType: error.name || 'Error',
        });
      }
    }

    logEvent_(counts.failed > 0 ? 'ERROR' : 'INFO', 'drive_sync_completed', {
      scannedCount: counts.scanned,
      syncedCount: counts.synced,
      skippedCount: counts.skipped,
      movedCount: counts.moved,
      failedCount: counts.failed,
      durationMs: Date.now() - startedAt,
    });

    if (counts.failed > 0) {
      throw new Error('One or more Drive files failed to sync');
    }
  } catch (error) {
    notifyFailure_();
    throw error;
  } finally {
    lock.releaseLock();
  }
}

/**
 * Installs one five-minute trigger. Run this once from the Apps Script editor
 * after the initial manual smoke test succeeds.
 */
function installSyncTrigger() {
  ScriptApp.requireAllScopes(ScriptApp.AuthMode.FULL);

  const existing = ScriptApp.getProjectTriggers()
    .filter(trigger => trigger.getHandlerFunction() === 'syncAirbnbCsv');

  if (existing.length > 0) {
    throw new Error('syncAirbnbCsv trigger already exists');
  }

  ScriptApp.newTrigger('syncAirbnbCsv')
    .timeBased()
    .everyMinutes(5)
    .create();
}

function loadConfig_() {
  const values = PropertiesService.getScriptProperties().getProperties();
  const missing = REQUIRED_PROPERTIES.filter(key => !values[key]);

  if (missing.length > 0) {
    throw new Error(`Missing Script Properties: ${missing.join(', ')}`);
  }

  if (values.DRIVE_FOLDER_ID === values.PROCESSED_FOLDER_ID) {
    throw new Error('DRIVE_FOLDER_ID and PROCESSED_FOLDER_ID must be different');
  }

  const maxFileSizeBytes = Number(values.MAX_FILE_SIZE_BYTES);
  if (!Number.isFinite(maxFileSizeBytes) || maxFileSizeBytes <= 0) {
    throw new Error('MAX_FILE_SIZE_BYTES must be a positive number');
  }

  const objectPrefix = values.GCS_OBJECT_PREFIX.replace(/^\/+|\/+$/g, '');
  if (!objectPrefix) {
    throw new Error('GCS_OBJECT_PREFIX must not be empty');
  }

  return {
    driveFolderId: values.DRIVE_FOLDER_ID,
    processedFolderId: values.PROCESSED_FOLDER_ID,
    bucketName: values.GCS_BUCKET_NAME,
    objectPrefix,
    maxFileSizeBytes,
  };
}

function isTargetCsv_(file, config) {
  if (!file.getName().toLowerCase().endsWith('.csv')) {
    return false;
  }

  if (file.getMimeType() === MimeType.GOOGLE_SHEETS) {
    return false;
  }

  if (file.getSize() > config.maxFileSizeBytes) {
    logEvent_('WARNING', 'drive_file_skipped', {
      driveFileId: file.getId(),
      reason: 'file_too_large',
      size: file.getSize(),
    });
    return false;
  }

  return true;
}

function uploadFileToGcs_(file, config) {
  const bytes = file.getBlob().getBytes();
  const contentHash = sha256Hex_(bytes);
  const safeName = sanitizeFileName_(file.getName());
  const objectName = [
    config.objectPrefix,
    file.getId(),
    contentHash,
    safeName,
  ].join('/');

  const url = [
    'https://storage.googleapis.com/upload/storage/v1/b/',
    encodeURIComponent(config.bucketName),
    '/o?uploadType=media',
    '&ifGenerationMatch=0',
    '&name=',
    encodeURIComponent(objectName),
  ].join('');

  const response = UrlFetchApp.fetch(url, {
    method: 'post',
    headers: {
      Authorization: `Bearer ${ScriptApp.getOAuthToken()}`,
    },
    contentType: 'text/csv; charset=utf-8',
    payload: bytes,
    muteHttpExceptions: true,
  });

  const status = response.getResponseCode();
  if (status >= 200 && status < 300) {
    logEvent_('INFO', 'drive_file_synced', {
      driveFileId: file.getId(),
      contentHash,
    });
    return 'synced';
  }

  if (status === 412) {
    logEvent_('INFO', 'drive_file_already_synced', {
      driveFileId: file.getId(),
      contentHash,
    });
    return 'skipped';
  }

  throw new Error(`GCS upload failed with HTTP ${status}`);
}

function sha256Hex_(bytes) {
  return Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, bytes)
    .map(byte => ((byte + 256) % 256).toString(16).padStart(2, '0'))
    .join('');
}

function sanitizeFileName_(name) {
  const normalized = name
    .normalize('NFKC')
    .replace(/[\/\\\u0000-\u001f\u007f]/g, '_')
    .replace(/^\.+/, '_')
    .slice(0, 180);
  const stem = normalized.replace(/\.csv$/i, '').replace(/\.+$/, '');

  return `${stem || 'file'}.csv`;
}

function notifyFailure_() {
  const alertEmail = PropertiesService.getScriptProperties()
    .getProperty('ALERT_EMAIL');
  if (!alertEmail) {
    return;
  }

  MailApp.sendEmail({
    to: alertEmail,
    subject: '[Airbnb Payouts] Drive to GCS sync failed',
    body: [
      'The Google Drive to GCS sync failed.',
      'Open the Apps Script Executions page to inspect the failed run.',
      'Do not copy CSV contents or guest/reservation data into email or tickets.',
    ].join('\n'),
  });
}

function logEvent_(severity, event, fields) {
  const message = JSON.stringify(Object.assign({ severity, event }, fields || {}));

  if (severity === 'ERROR') {
    console.error(message);
  } else if (severity === 'WARNING') {
    console.warn(message);
  } else {
    console.log(message);
  }
}
