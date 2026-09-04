# frozen_string_literal: true

require "json"
require "securerandom"
require "tempfile"

require "google/cloud/bigquery"
require "google/cloud/storage"

require_relative "schema"

module AirbnbPayous
  class BigqueryGateway
    DownloadedCsv = Data.define(:content, :generation, :created_at)

    RECONCILIATION_MODES = %w[off dry_run apply].freeze

    attr_reader :project_id, :dataset_id, :table_id, :current_table_id, :snapshot_state_table_id,
      :reconciliation_mode

    def initialize(
      project_id:,
      dataset_id:,
      table_id:,
      logger: Logger.new($stdout),
      bigquery: nil,
      storage: nil,
      reconciliation_mode: ENV.fetch("CURRENT_STATE_MODE", "dry_run")
    )
      @logger = logger
      @project_id = project_id
      @dataset_id = dataset_id
      @table_id = table_id
      @current_table_id = "#{table_id}_current"
      @snapshot_state_table_id = "#{table_id}_snapshot_state"
      @reconciliation_mode = reconciliation_mode

      @logger.info("Initializing BigqueryGateway with project_id: #{@project_id.inspect}, dataset_id: #{@dataset_id.inspect}, table_id: #{@table_id.inspect}")

      if @project_id.nil? || @project_id.empty?
        raise ArgumentError, "project_id is required"
      end

      if @dataset_id.nil? || @dataset_id.empty?
        raise ArgumentError, "dataset_id is required"
      end

      if @table_id.nil? || @table_id.empty?
        raise ArgumentError, "table_id is required"
      end

      unless RECONCILIATION_MODES.include?(@reconciliation_mode)
        raise ArgumentError, "CURRENT_STATE_MODE must be one of: #{RECONCILIATION_MODES.join(", ")}"
      end

      @bigquery = bigquery || Google::Cloud::Bigquery.new(project_id: @project_id)
      @storage = storage || Google::Cloud::Storage.new(project_id: @project_id)
    end

    def download(bucket_name:, file_name:)
      bucket = @storage.bucket(bucket_name)
      file = bucket.file(file_name)
      DownloadedCsv.new(
        content: file.download.string,
        generation: file.generation,
        created_at: file.created_at
      )
    end

    def load_and_merge!(rows:, snapshot: nil)
      dataset = @bigquery.dataset(dataset_id) || @bigquery.create_dataset(dataset_id)
      temp_file = build_tempfile(rows)
      staging_table_id = unique_staging_table_id

      load_job = dataset.load_job(
        staging_table_id,
        temp_file.path,
        format: "json",
        write: "truncate",
        autodetect: true
      ) do |job|
        job.schema do |schema|
          Schema::JOB_SCHEMA.each do |name, type, mode|
            schema_field_type = map_schema_type(type)
            schema.public_send(schema_field_type, name, mode: map_schema_mode(mode))
          end
        end
      end
      load_job.wait_until_done!
      raise load_job.error if load_job.failed?

      # Use job output rows, fallback to input rows count if zero (sometimes stats are delayed)
      total_rows_loaded = load_job.output_rows.to_i
      total_rows_loaded = rows.length if total_rows_loaded.zero?
      
      @logger.info("Load job finished. output_rows: #{load_job.output_rows}, rows.length: #{rows.length}. Using #{total_rows_loaded} as reference.")

      # Fetch a fresh table reference to avoid caching issues
      target_table = dataset.table(table_id)
      result = { inserted_count: 0, updated_count: 0 }

      if target_table.nil?
        @logger.info("Target table #{qualified_table_name(table_id)} does not exist. Path: :create_table")
        copy_job = @bigquery.copy_job qualified_table_name(staging_table_id), qualified_table_name(table_id), write: "truncate"
        @logger.info("Started copy_job (ID: #{copy_job.job_id}). Waiting for completion...")
        copy_job.wait_until_done!
        raise copy_job.error if copy_job.failed?
        @logger.info("Target table created successfully via copy_job.")
        
        result[:mode] = :create_table
        result[:inserted_count] = total_rows_loaded
      else
        @logger.info("Target table #{qualified_table_name(table_id)} exists. Path: :merge")
        merge_sql = build_merge_query(rows.first.keys, staging_table_id:)
        query_job = @bigquery.query_job(merge_sql)
        @logger.info("Started query_job (ID: #{query_job.job_id}). Waiting for completion...")
        query_job.wait_until_done!
        raise query_job.error if query_job.failed?

        result.merge!(extract_dml_counts(query_job))

        result[:mode] = :merge
      end

      result[:current_state] = reconcile_current_snapshot(
        staging_table_id:,
        columns: rows.first.keys,
        snapshot:
      )

      @logger.info("Final result for notification: #{result.inspect}")
      result
    ensure
      temp_file&.close!
      delete_staging_table(dataset, staging_table_id)
    end

    def qualified_table_name(table_name)
      "#{project_id}.#{dataset_id}.#{table_name}"
    end

    private

    def to_newline_delimited_json(rows)
      rows.map { |row| JSON.generate(serialize_row(row)) }.join("\n")
    end

    def build_tempfile(rows)
      file = Tempfile.new(["airbnb-payous", ".json"])
      file.binmode
      file.write(to_newline_delimited_json(rows))
      file.flush
      file
    end

    def serialize_row(row)
      row.transform_values do |value|
        case value
        when Date
          value.iso8601
        when BigDecimal
          value.to_s("F")
        else
          value
        end
      end
    end

    def map_schema_type(type)
      {
        date: :date,
        string: :string,
        integer: :integer,
        numeric: :numeric
      }.fetch(type)
    end

    def map_schema_mode(mode)
      mode == :required ? :required : :nullable
    end

    def build_merge_query(columns, staging_table_id:)
      columns_list = columns.map { |column| "`#{column}`" }.join(", ")
      source_columns_list = columns.map { |column| "S.`#{column}`" }.join(", ")

      <<~SQL
        MERGE `#{qualified_table_name(table_id)}` T
        USING `#{qualified_table_name(staging_table_id)}` S
        ON T.row_id = S.row_id
        WHEN NOT MATCHED THEN
          INSERT (#{columns_list}) VALUES (#{source_columns_list})
      SQL
    end

    def reconcile_current_snapshot(staging_table_id:, columns:, snapshot:)
      return :off if snapshot.nil? || reconciliation_mode == "off"

      @logger.info(
        "Current-state reconciliation candidate: event_year=#{snapshot.event_year}, " \
        "through_date=#{snapshot.through_date}, row_count=#{snapshot.row_count}, " \
        "snapshot_id=#{snapshot.id[0, 12]}, mode=#{reconciliation_mode}."
      )
      return :dry_run if reconciliation_mode == "dry_run"

      query_job = @bigquery.query_job(
        build_current_reconciliation_query(columns, staging_table_id:, snapshot:)
      )
      query_job.wait_until_done!
      raise query_job.error if query_job.failed?

      result_row = query_job.data&.first
      applied = result_row && (result_row[:applied] || result_row["applied"])
      status = applied ? :applied : :skipped
      @logger.info(
        "Current-state reconciliation #{status} for event_year=#{snapshot.event_year}."
      )
      status
    end

    def build_current_reconciliation_query(columns, staging_table_id:, snapshot:)
      columns_list = columns.map { |column| "`#{column}`" }.join(", ")
      generation = Integer(snapshot.source_generation)
      created_at = snapshot.source_created_at.utc.iso8601(6)

      <<~SQL
        DECLARE should_apply BOOL;

        CREATE TABLE IF NOT EXISTS `#{qualified_table_name(current_table_id)}`
        LIKE `#{qualified_table_name(table_id)}`;

        CREATE TABLE IF NOT EXISTS `#{qualified_table_name(snapshot_state_table_id)}` (
          event_year INT64,
          snapshot_id STRING,
          snapshot_through DATE,
          source_created_at TIMESTAMP,
          source_generation INT64,
          row_count INT64,
          applied_at TIMESTAMP
        );

        SET should_apply = NOT EXISTS (
          SELECT 1
          FROM `#{qualified_table_name(snapshot_state_table_id)}`
          WHERE event_year = #{snapshot.event_year}
            AND (
              (
                snapshot_id = '#{snapshot.id}'
                AND snapshot_through >= DATE '#{snapshot.through_date.iso8601}'
              )
              OR snapshot_through > DATE '#{snapshot.through_date.iso8601}'
              OR (
                snapshot_through = DATE '#{snapshot.through_date.iso8601}'
                AND source_created_at > TIMESTAMP '#{created_at}'
              )
              OR (
                snapshot_through = DATE '#{snapshot.through_date.iso8601}'
                AND source_created_at = TIMESTAMP '#{created_at}'
                AND source_generation >= #{generation}
              )
            )
        );

        IF should_apply THEN
          BEGIN TRANSACTION;

          DELETE FROM `#{qualified_table_name(current_table_id)}`
          WHERE EXTRACT(YEAR FROM event_date) = #{snapshot.event_year};

          INSERT INTO `#{qualified_table_name(current_table_id)}` (#{columns_list})
          SELECT #{columns_list}
          FROM `#{qualified_table_name(staging_table_id)}`;

          DELETE FROM `#{qualified_table_name(snapshot_state_table_id)}`
          WHERE event_year = #{snapshot.event_year};

          INSERT INTO `#{qualified_table_name(snapshot_state_table_id)}` (
            event_year,
            snapshot_id,
            snapshot_through,
            source_created_at,
            source_generation,
            row_count,
            applied_at
          ) VALUES (
            #{snapshot.event_year},
            '#{snapshot.id}',
            DATE '#{snapshot.through_date.iso8601}',
            TIMESTAMP '#{created_at}',
            #{generation},
            #{snapshot.row_count},
            CURRENT_TIMESTAMP()
          );

          COMMIT TRANSACTION;
        END IF;

        SELECT should_apply AS applied;
      SQL
    end

    def unique_staging_table_id
      "#{table_id}_staging_#{SecureRandom.hex(8)}"
    end

    def extract_dml_counts(query_job)
      inserted_count = extract_query_job_metric(query_job, :inserted_row_count)
      updated_count = extract_query_job_metric(query_job, :updated_row_count)

      if inserted_count || updated_count
        inserted_count ||= 0
        updated_count ||= 0
        @logger.info("DML row counts - Inserted: #{inserted_count}, Updated: #{updated_count}")
        return { inserted_count:, updated_count: }
      end

      affected = extract_query_job_metric(query_job, :num_dml_affected_rows)
      if affected
        # Current MERGE only inserts new rows, so affected rows can be treated as inserts.
        @logger.info("DML row counts unavailable; using num_dml_affected_rows=#{affected} as inserted count.")
        return { inserted_count: affected, updated_count: 0 }
      end

      @logger.warn("No DML statistics available for query job #{query_job.job_id}.")
      { inserted_count: 0, updated_count: 0 }
    end

    def extract_query_job_metric(query_job, method_name)
      return nil unless query_job.respond_to?(method_name)

      value = query_job.public_send(method_name)
      value.nil? ? nil : value.to_i
    rescue StandardError => e
      @logger.warn("Failed to read #{method_name} from query job #{query_job.job_id}: #{e.message}")
      nil
    end

    def delete_staging_table(dataset, staging_table_id)
      return if dataset.nil? || staging_table_id.nil?

      staging_table = dataset.table(staging_table_id)
      staging_table&.delete
      @logger.info("Staging table cleaned up.")
    rescue StandardError => e
      @logger.warn("Failed to delete staging table #{qualified_table_name(staging_table_id)}: #{e.message}")
    end
  end
end
