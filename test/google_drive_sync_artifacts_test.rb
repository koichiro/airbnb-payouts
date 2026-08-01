# frozen_string_literal: true

require_relative "test_helper"
require "json"

class GoogleDriveSyncArtifactsTest < Minitest::Test
  CODE_PATH = File.expand_path("../apps-script/Code.gs", __dir__)
  MANIFEST_PATH = File.expand_path("../apps-script/appsscript.json", __dir__)

  def setup
    @code = File.read(CODE_PATH)
    @manifest = JSON.parse(File.read(MANIFEST_PATH))
  end

  def test_manifest_uses_v8_tokyo_and_explicit_scopes
    assert_equal "Asia/Tokyo", @manifest.fetch("timeZone")
    assert_equal "V8", @manifest.fetch("runtimeVersion")
    assert_equal "STACKDRIVER", @manifest.fetch("exceptionLogging")
    assert_equal expected_scopes.sort, @manifest.fetch("oauthScopes").sort
  end

  def test_code_requires_all_durable_sync_configuration
    %w[
      DRIVE_FOLDER_ID
      PROCESSED_FOLDER_ID
      GCS_BUCKET_NAME
      GCS_OBJECT_PREFIX
      MAX_FILE_SIZE_BYTES
    ].each do |property|
      assert_includes @code, "'#{property}'"
    end

    assert_includes @code, "DRIVE_FOLDER_ID and PROCESSED_FOLDER_ID must be different"
  end

  def test_object_name_is_revision_specific_and_upload_is_create_only
    object_components = <<~JAVASCRIPT
      const objectName = [
          config.objectPrefix,
          file.getId(),
          contentHash,
          safeName,
        ].join('/');
    JAVASCRIPT

    assert_includes normalize_indentation(@code), normalize_indentation(object_components)
    assert_includes @code, "'&ifGenerationMatch=0'"
    assert_match(/if \(status === 412\).*?return 'skipped';/m, @code)
  end

  def test_file_is_moved_only_after_upload_result_is_known
    upload_position = @code.index("const uploadResult = uploadFileToGcs_")
    move_position = @code.index("file.moveTo(processedFolder)")

    refute_nil upload_position
    refute_nil move_position
    assert_operator upload_position, :<, move_position
    assert_match(/uploadFileToGcs_.*?file\.moveTo\(processedFolder\).*?catch \(error\)/m, @code)
  end

  def test_script_uses_lock_and_does_not_persist_processed_state
    assert_includes @code, "LockService.getScriptLock()"
    assert_includes @code, "lock.tryLock(1000)"
    assert_includes @code, "lock.releaseLock()"
    refute_match(/setPropert(?:y|ies)\(/, @code)
  end

  def test_logs_do_not_include_filename_object_name_or_csv_payload
    logging_section = @code[@code.index("function logEvent_")..]
    upload_logging = @code[@code.index("if (status >= 200")...@code.index("throw new Error(`GCS upload failed")]

    refute_includes upload_logging, "objectName"
    refute_includes upload_logging, "safeName"
    refute_includes logging_section, "payload"
  end

  private

  def expected_scopes
    %w[
      https://www.googleapis.com/auth/drive
      https://www.googleapis.com/auth/devstorage.read_write
      https://www.googleapis.com/auth/script.external_request
      https://www.googleapis.com/auth/script.scriptapp
      https://www.googleapis.com/auth/script.send_mail
    ]
  end

  def normalize_indentation(value)
    value.lines.map(&:strip).join("\n")
  end
end
