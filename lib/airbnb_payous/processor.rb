# frozen_string_literal: true

require "logger"

require_relative "bigquery_gateway"
require_relative "cumulative_snapshot"
require_relative "csv_transformer"
require_relative "slack_notifier"

module AirbnbPayous
  class Processor
    def initialize(
      transformer: CsvTransformer.new,
      gateway: nil,
      notifier: SlackNotifier.new,
      logger: Logger.new($stdout)
    )
      @transformer = transformer
      @gateway = gateway || default_gateway
      @notifier = notifier
      @logger = logger
    end

    def call(event_payload)
      data = extract_event_data(event_payload)
      bucket_name = data.fetch("bucket")
      file_name = data.fetch("name")

      unless file_name.downcase.end_with?(".csv")
        @logger.info("Skipping non-CSV file: #{file_name}")
        return nil
      end

      downloaded_csv = @gateway.download(bucket_name:, file_name:)
      csv_content = downloaded_csv.respond_to?(:content) ? downloaded_csv.content : downloaded_csv
      rows = @transformer.call(csv_content)
      snapshot = build_cumulative_snapshot(
        file_name:,
        csv_content:,
        rows:,
        downloaded_csv:
      )
      result = @gateway.load_and_merge!(rows:, snapshot:)

      @notifier.notify_success(
        file_name: file_name,
        mode: result[:mode],
        inserted_count: result[:inserted_count],
        updated_count: result[:updated_count]
      )

      nil
    rescue StandardError => e
      @logger.error("Failed to process Airbnb CSV: #{e.message}")
      @notifier.notify_failure(file_name: file_name || "unknown", error_message: e.message)
      raise e
    end

    private

    def build_cumulative_snapshot(file_name:, csv_content:, rows:, downloaded_csv:)
      return nil unless downloaded_csv.respond_to?(:generation) && downloaded_csv.respond_to?(:created_at)

      CumulativeSnapshot.build(
        file_name:,
        content: csv_content,
        rows:,
        source_generation: downloaded_csv.generation,
        source_created_at: downloaded_csv.created_at
      )
    end

    def extract_event_data(event_payload)
      if event_payload.key?("data") && event_payload["data"].is_a?(Hash)
        @logger.info("Executing from structured CloudEvent payload")
        event_payload["data"]
      else
        @logger.info("Executing from raw event payload")
        event_payload
      end
    end

    def default_gateway
      BigqueryGateway.new(
        project_id: ENV["GCP_PROJECT_ID"],
        dataset_id: ENV["BQ_DATASET_ID"],
        table_id: ENV["BQ_TABLE_ID"]
      )
    end
  end
end
