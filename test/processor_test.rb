# frozen_string_literal: true

require "logger"
require "stringio"

require_relative "test_helper"
require_relative "../lib/airbnb_payous/processor"

class ProcessorTest < Minitest::Test
  FakeTransformer = Struct.new(:rows, :received_csv) do
    def call(csv)
      self.received_csv = csv
      rows
    end
  end

  class FakeGateway
    attr_reader :download_calls, :load_calls

    def initialize(csv: "csv", error: nil, result: { mode: :merge, inserted_count: 1, updated_count: 2 })
      @csv = csv
      @error = error
      @result = result
      @download_calls = []
      @load_calls = []
    end

    def download(bucket_name:, file_name:)
      raise @error if @error

      @download_calls << { bucket_name:, file_name: }
      @csv
    end

    def load_and_merge!(rows:, snapshot: nil)
      @load_calls << { rows: rows, snapshot: snapshot }
      @result
    end
  end

  class FakeNotifier
    attr_reader :success_calls, :failure_calls

    def initialize
      @success_calls = []
      @failure_calls = []
    end

    def notify_success(file_name:, mode:, inserted_count:, updated_count:)
      @success_calls << { file_name:, mode:, inserted_count:, updated_count: }
    end

    def notify_failure(file_name:, error_message:)
      @failure_calls << { file_name:, error_message: }
    end
  end

  def setup
    @rows = [{ "event_date" => Date.new(2026, 3, 12), "row_id" => "abc" }]
    @transformer = FakeTransformer.new(@rows)
    @gateway = FakeGateway.new
    @notifier = FakeNotifier.new
    @log_output = StringIO.new
    @logger = Logger.new(@log_output)
    @processor = AirbnbPayous::Processor.new(
      transformer: @transformer,
      gateway: @gateway,
      notifier: @notifier,
      logger: @logger
    )
  end

  def test_downloads_transforms_and_loads_csv_from_raw_payload
    assert_nil @processor.call({ "bucket" => "bucket", "name" => "file.csv" })

    assert_equal [{ bucket_name: "bucket", file_name: "file.csv" }], @gateway.download_calls
    assert_equal "csv", @transformer.received_csv
    assert_equal [{ rows: @rows, snapshot: nil }], @gateway.load_calls

    assert_equal 1, @notifier.success_calls.length
    assert_equal "file.csv", @notifier.success_calls.first[:file_name]
    assert_equal :merge, @notifier.success_calls.first[:mode]
  end

  def test_extracts_cloudevent_data_payloads
    @processor.call({ "data" => { "bucket" => "bucket", "name" => "file.csv" } })

    assert_includes @log_output.string, "Executing from structured CloudEvent payload"
    assert_equal [{ bucket_name: "bucket", file_name: "file.csv" }], @gateway.download_calls
    assert_equal 1, @notifier.success_calls.length
  end

  def test_passes_cumulative_snapshot_metadata_to_the_gateway
    downloaded_csv = AirbnbPayous::BigqueryGateway::DownloadedCsv.new(
      content: "csv",
      generation: 123,
      created_at: Time.utc(2026, 8, 29, 4, 12, 44)
    )
    gateway = FakeGateway.new(csv: downloaded_csv)
    processor = AirbnbPayous::Processor.new(
      transformer: @transformer,
      gateway:,
      notifier: @notifier,
      logger: @logger
    )

    processor.call({ "bucket" => "bucket", "name" => "airbnb_01_2026-08_2026.csv" })

    snapshot = gateway.load_calls.first[:snapshot]
    assert_equal 2026, snapshot.event_year
    assert_equal Date.new(2026, 8, 31), snapshot.through_date
    assert_equal 123, snapshot.source_generation
  end

  def test_skips_non_csv_files
    assert_nil @processor.call({ "bucket" => "bucket", "name" => "file.txt" })
    assert_empty @gateway.download_calls
    assert_empty @gateway.load_calls
    assert_empty @notifier.success_calls
  end

  def test_notifies_failure_on_error
    processor = AirbnbPayous::Processor.new(
      transformer: @transformer,
      gateway: FakeGateway.new(error: RuntimeError.new("boom")),
      notifier: @notifier,
      logger: @logger
    )

    assert_raises(RuntimeError) do
      processor.call({ "bucket" => "bucket", "name" => "file.csv" })
    end

    assert_equal 1, @notifier.failure_calls.length
    assert_equal "file.csv", @notifier.failure_calls.first[:file_name]
    assert_equal "boom", @notifier.failure_calls.first[:error_message]
  end
end
