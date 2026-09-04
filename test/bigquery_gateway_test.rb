# frozen_string_literal: true

require "bigdecimal"
require "date"
require "logger"
require "stringio"

require_relative "test_helper"
require_relative "../lib/airbnb_payous/bigquery_gateway"

class BigqueryGatewayTest < Minitest::Test
  class FakeStorage
    attr_reader :bucket_calls

    def initialize(content: "csv")
      @content = content
      @bucket_calls = []
    end

    def bucket(name)
      @bucket_calls << name
      FakeBucket.new(@content)
    end
  end

  class FakeBucket
    attr_reader :file_calls

    def initialize(content)
      @content = content
      @file_calls = []
    end

    def file(name)
      @file_calls << name
      FakeFile.new(@content)
    end
  end

  class FakeFile
    def initialize(content)
      @content = content
    end

    def download
      StringIO.new(@content)
    end

    def generation = 123
    def created_at = Time.utc(2026, 8, 29, 4, 12, 44)
  end

  class FakeSchema
    attr_reader :fields

    def initialize
      @fields = []
    end

    def date(name, mode:) = @fields << [:date, name, mode]
    def string(name, mode:) = @fields << [:string, name, mode]
    def integer(name, mode:) = @fields << [:integer, name, mode]
    def numeric(name, mode:) = @fields << [:numeric, name, mode]
  end

  class FakeLoadJob
    attr_reader :waited, :job_id

    def initialize(output_rows: 1, job_id: "job_123")
      @waited = false
      @output_rows = output_rows
      @job_id = job_id
    end

    def wait_until_done!
      @waited = true
    end

    def failed?
      false
    end

    def output_rows
      @output_rows
    end

    def error; nil; end
  end

  class FakeCopyJob < FakeLoadJob
    def error; nil; end
  end

  class FakeQueryJob < FakeLoadJob
    attr_reader :sql

    def initialize(
      inserted: 1,
      updated: 0,
      supports_dml_stats: true,
      supports_row_counts: true,
      supports_affected_rows: true,
      reconciliation_applied: true
    )
      super()
      @inserted = inserted
      @updated = updated
      @supports_dml_stats = supports_dml_stats
      @supports_row_counts = supports_row_counts
      @supports_affected_rows = supports_affected_rows
      @reconciliation_applied = reconciliation_applied
      @dml_stats = Struct.new(:inserted_row_count, :updated_row_count).new(inserted, updated)
    end

    def dml_stats
      raise NoMethodError, "undefined method 'dml_stats'" unless @supports_dml_stats

      @dml_stats
    end

    def num_dml_affected_rows
      raise NoMethodError, "undefined method 'num_dml_affected_rows'" unless @supports_affected_rows

      @inserted + @updated
    end

    def inserted_row_count
      raise NoMethodError, "undefined method 'inserted_row_count'" unless @supports_row_counts

      @inserted
    end

    def updated_row_count
      raise NoMethodError, "undefined method 'updated_row_count'" unless @supports_row_counts

      @updated
    end

    def error; nil; end

    def data
      [{ applied: @reconciliation_applied }]
    end

    def respond_to?(method_name, include_private = false)
      return @supports_dml_stats if method_name == :dml_stats
      return @supports_row_counts if [:inserted_row_count, :updated_row_count].include?(method_name)
      return @supports_affected_rows if method_name == :num_dml_affected_rows

      super
    end
  end

  class FakeTable
    attr_reader :deleted

    def initialize(raises_on_delete: false)
      @raises_on_delete = raises_on_delete
      @deleted = false
    end

    def delete
      raise "cannot delete" if @raises_on_delete

      @deleted = true
    end
  end

  class FakeDataset
    attr_reader :load_job_calls, :table_requests, :loaded_json, :schema_fields

    def initialize(target_table:, staging_table:, output_rows: 1)
      @target_table = target_table
      @staging_table = staging_table
      @output_rows = output_rows
      @load_job_calls = []
      @table_requests = []
      @loaded_json = []
      @schema_fields = []
    end

    def load_job(table_id, path, **kwargs)
      @load_job_calls << [table_id, kwargs]
      @loaded_json << File.read(path)
      schema = FakeSchema.new
      updater = Object.new
      updater.define_singleton_method(:schema) do |&block|
        block.call(schema)
      end
      yield updater
      @schema_fields = schema.fields
      FakeLoadJob.new(output_rows: @output_rows)
    end

    def table(name)
      @table_requests << name
      return @target_table if name == "table"
      return @staging_table if name.start_with?("table_staging_")

      nil
    end
  end

  class FakeBigquery
    attr_reader :dataset_calls, :create_dataset_calls, :copy_job_calls, :query_job_calls

    def initialize(dataset:, created_dataset: nil, query_job: nil)
      @dataset = dataset
      @created_dataset = created_dataset || dataset
      @query_job = query_job || FakeQueryJob.new
      @dataset_calls = []
      @create_dataset_calls = []
      @copy_job_calls = []
      @query_job_calls = []
    end

    def dataset(name)
      @dataset_calls << name
      @dataset
    end

    def create_dataset(name)
      @create_dataset_calls << name
      @created_dataset
    end

    def copy_job(source, destination, write:)
      @copy_job_calls << { source:, destination:, write: }
      FakeCopyJob.new
    end

    def query_job(sql)
      @query_job_calls << sql
      @query_job
    end
  end

  def setup
    @logger = Logger.new(StringIO.new)
    @staging_table = FakeTable.new
    @target_table = Object.new
    @dataset = FakeDataset.new(target_table: @target_table, staging_table: @staging_table)
    @bigquery = FakeBigquery.new(dataset: @dataset)
    @storage = FakeStorage.new(content: "csv")
    @gateway = AirbnbPayous::BigqueryGateway.new(
      project_id: "project",
      dataset_id: "dataset",
      table_id: "table",
      logger: @logger,
      bigquery: @bigquery,
      storage: @storage
    )
    @rows = [
      {
        "event_date" => Date.new(2026, 3, 12),
        "amount" => BigDecimal("150.50"),
        "row_id" => "abc123"
      }
    ]
  end

  def test_downloads_csv_bytes_from_cloud_storage
    downloaded_csv = @gateway.download(bucket_name: "bucket", file_name: "file.csv")

    assert_equal "csv", downloaded_csv.content
    assert_equal 123, downloaded_csv.generation
    assert_equal Time.utc(2026, 8, 29, 4, 12, 44), downloaded_csv.created_at
    assert_equal ["bucket"], @storage.bucket_calls
  end

  def test_copies_the_staging_table_on_the_first_run
    dataset = FakeDataset.new(target_table: nil, staging_table: FakeTable.new)
    bigquery = FakeBigquery.new(dataset: dataset)
    gateway = AirbnbPayous::BigqueryGateway.new(
      project_id: "project",
      dataset_id: "dataset",
      table_id: "table",
      logger: @logger,
      bigquery: bigquery,
      storage: @storage
    )

    result = gateway.load_and_merge!(rows: @rows)

    assert_equal :create_table, result[:mode]
    assert_equal @rows.length, result[:inserted_count]
    assert_equal 0, result[:updated_count]

    assert_equal 1, bigquery.copy_job_calls.length
    staging_table_id = dataset.load_job_calls.first.first
    assert_match(/\Atable_staging_\h{16}\z/, staging_table_id)
    assert_equal "project.dataset.#{staging_table_id}", bigquery.copy_job_calls.first[:source]
    assert_equal "project.dataset.table", bigquery.copy_job_calls.first[:destination]
    assert_empty bigquery.query_job_calls
    assert dataset.table(staging_table_id).deleted
  end

  def test_merges_into_an_existing_target_table
    result = @gateway.load_and_merge!(rows: @rows)

    assert_equal :merge, result[:mode]
    assert_equal 1, result[:inserted_count]
    assert_equal 0, result[:updated_count]

    assert_equal 1, @bigquery.query_job_calls.length
    assert_includes @bigquery.query_job_calls.first, "MERGE `project.dataset.table`"
    assert_empty @bigquery.copy_job_calls
  end

  def test_merges_with_query_job_that_exposes_row_counts_but_not_dml_stats
    bigquery = FakeBigquery.new(
      dataset: @dataset,
      query_job: FakeQueryJob.new(inserted: 2, updated: 3, supports_dml_stats: false, supports_row_counts: true)
    )
    gateway = AirbnbPayous::BigqueryGateway.new(
      project_id: "project",
      dataset_id: "dataset",
      table_id: "table",
      logger: @logger,
      bigquery: bigquery,
      storage: @storage
    )

    result = gateway.load_and_merge!(rows: @rows)

    assert_equal :merge, result[:mode]
    assert_equal 2, result[:inserted_count]
    assert_equal 3, result[:updated_count]
  end

  def test_merges_with_query_job_that_only_exposes_affected_rows
    bigquery = FakeBigquery.new(
      dataset: @dataset,
      query_job: FakeQueryJob.new(inserted: 4, updated: 0, supports_dml_stats: false, supports_row_counts: false, supports_affected_rows: true)
    )
    gateway = AirbnbPayous::BigqueryGateway.new(
      project_id: "project",
      dataset_id: "dataset",
      table_id: "table",
      logger: @logger,
      bigquery: bigquery,
      storage: @storage
    )

    result = gateway.load_and_merge!(rows: @rows)

    assert_equal :merge, result[:mode]
    assert_equal 4, result[:inserted_count]
    assert_equal 0, result[:updated_count]
  end

  def test_serializes_dates_and_decimals_for_bigquery_load_jobs
    @gateway.load_and_merge!(rows: @rows)

    json = @dataset.loaded_json.last
    assert_includes json, "\"event_date\":\"2026-03-12\""
    assert_includes json, "\"amount\":\"150.5\""
    assert_includes json, "\"row_id\":\"abc123\""
  end

  def test_uses_a_unique_staging_table_for_each_import
    @gateway.load_and_merge!(rows: @rows)
    @gateway.load_and_merge!(rows: @rows)

    staging_table_ids = @dataset.load_job_calls.map(&:first)
    assert_equal 2, staging_table_ids.uniq.length
    assert staging_table_ids.all? { |name| name.match?(/\Atable_staging_\h{16}\z/) }
  end

  def test_reports_current_snapshot_changes_without_applying_them_in_dry_run_mode
    snapshot = build_snapshot

    result = @gateway.load_and_merge!(rows: @rows, snapshot:)

    assert_equal :dry_run, result[:current_state]
    assert_equal 1, @bigquery.query_job_calls.length
  end

  def test_reconciles_the_current_table_by_event_year_in_apply_mode
    gateway = AirbnbPayous::BigqueryGateway.new(
      project_id: "project",
      dataset_id: "dataset",
      table_id: "table",
      logger: @logger,
      bigquery: @bigquery,
      storage: @storage,
      reconciliation_mode: "apply"
    )
    snapshot = build_snapshot

    result = gateway.load_and_merge!(rows: @rows, snapshot:)

    assert_equal :applied, result[:current_state]
    assert_equal 2, @bigquery.query_job_calls.length

    reconciliation_sql = @bigquery.query_job_calls.last
    assert_includes reconciliation_sql, "CREATE TABLE IF NOT EXISTS `project.dataset.table_current`"
    assert_includes reconciliation_sql, "CREATE TABLE IF NOT EXISTS `project.dataset.table_snapshot_state`"
    assert_includes reconciliation_sql, "EXTRACT(YEAR FROM event_date) = 2026"
    assert_includes reconciliation_sql, "snapshot_id = '#{"a" * 64}'"
    assert_includes reconciliation_sql, "snapshot_through >= DATE '2026-03-12'"
    assert_includes reconciliation_sql, "snapshot_through > DATE '2026-03-12'"
    assert_includes reconciliation_sql, "source_created_at > TIMESTAMP '2026-03-13T04:05:06.000000Z'"
    assert_includes reconciliation_sql, "source_generation >= 123"
    assert_includes reconciliation_sql, "INSERT INTO `project.dataset.table_current`"
  end

  def test_reports_when_an_older_snapshot_is_skipped
    bigquery = FakeBigquery.new(
      dataset: @dataset,
      query_job: FakeQueryJob.new(reconciliation_applied: false)
    )
    gateway = AirbnbPayous::BigqueryGateway.new(
      project_id: "project",
      dataset_id: "dataset",
      table_id: "table",
      logger: @logger,
      bigquery:,
      storage: @storage,
      reconciliation_mode: "apply"
    )

    result = gateway.load_and_merge!(rows: @rows, snapshot: build_snapshot)

    assert_equal :skipped, result[:current_state]
  end

  def test_rejects_an_unknown_reconciliation_mode
    error = assert_raises(ArgumentError) do
      AirbnbPayous::BigqueryGateway.new(
        project_id: "project",
        dataset_id: "dataset",
        table_id: "table",
        logger: @logger,
        bigquery: @bigquery,
        storage: @storage,
        reconciliation_mode: "unknown"
      )
    end

    assert_includes error.message, "CURRENT_STATE_MODE"
  end

  def test_creates_dataset_when_it_does_not_already_exist
    created_dataset = FakeDataset.new(target_table: @target_table, staging_table: FakeTable.new)
    bigquery = FakeBigquery.new(dataset: nil, created_dataset: created_dataset)
    gateway = AirbnbPayous::BigqueryGateway.new(
      project_id: "project",
      dataset_id: "dataset",
      table_id: "table",
      logger: @logger,
      bigquery: bigquery,
      storage: @storage
    )

    gateway.load_and_merge!(rows: @rows)

    assert_equal ["dataset"], bigquery.create_dataset_calls
    assert_equal 1, bigquery.query_job_calls.length
  end

  def test_cleans_up_gracefully_when_staging_table_deletion_fails
    dataset = FakeDataset.new(target_table: @target_table, staging_table: FakeTable.new(raises_on_delete: true))
    bigquery = FakeBigquery.new(dataset: dataset)
    gateway = AirbnbPayous::BigqueryGateway.new(
      project_id: "project",
      dataset_id: "dataset",
      table_id: "table",
      logger: @logger,
      bigquery: bigquery,
      storage: @storage
    )

    gateway.load_and_merge!(rows: @rows)

    assert_equal 1, bigquery.query_job_calls.length
  end

  def test_returns_fully_qualified_table_names
    assert_equal "project.dataset.table", @gateway.qualified_table_name("table")
  end

  private

  def build_snapshot
    Struct.new(
      :id,
      :event_year,
      :through_date,
      :source_generation,
      :source_created_at,
      :row_count,
      keyword_init: true
    ).new(
      id: "a" * 64,
      event_year: 2026,
      through_date: Date.new(2026, 3, 12),
      source_generation: 123,
      source_created_at: Time.utc(2026, 3, 13, 4, 5, 6),
      row_count: 1
    )
  end
end
