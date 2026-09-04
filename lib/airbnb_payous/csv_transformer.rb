# frozen_string_literal: true

require "bigdecimal"
require "csv"
require "date"
require "digest"

require_relative "schema"

module AirbnbPayous
  class CsvTransformer
    attr_reader :unmapped_source_columns

    def initialize(logger: Logger.new($stdout))
      @logger = logger
      @unmapped_source_columns = []
    end

    def call(csv_content)
      normalized_content = normalize_encoding(csv_content)
      rows = CSV.parse(normalized_content, headers: true)
      normalized_headers = rows.headers.map { |header| header.to_s.strip }
      @unmapped_source_columns = normalized_headers.reject { |header| Schema::COLUMN_MAP.key?(header) }

      warn_unmapped_columns if unmapped_source_columns.any?

      raw_rows = rows.map.with_index(2) do |row, csv_row_number|
        normalized_row = {}

        normalized_headers.each do |header|
          value = row[header]
          normalized_key = Schema::COLUMN_MAP.fetch(header, header)
          normalized_row[normalized_key] = normalize_cell(value)
        end

        normalize_types!(normalized_row, csv_row_number:)
        normalized_row
      end

      raw_rows.each do |row|
        ensure_schema_columns!(row)
        row["row_id"] = build_row_id(row)
      end

      raw_rows
    end

    private

    def normalize_encoding(csv_content)
      # Force to UTF-8 and handle invalid sequences (common in CSV exports)
      content = csv_content.to_s.dup.force_encoding("UTF-8")
      unless content.valid_encoding?
        # If not valid UTF-8, try common encodings or replace invalid bytes
        content = csv_content.to_s.encode("UTF-8", invalid: :replace, undef: :replace, replace: "")
      end

      # Remove UTF-8 Byte Order Mark (BOM) if present
      content.sub!("\xEF\xBB\xBF", "")
      content
    end

    def normalize_cell(value)
      return nil if value.nil?

      stripped = value.strip
      stripped.empty? ? nil : stripped
    end

    def normalize_types!(row, csv_row_number:)
      Schema::DATE_COLUMNS.each do |column|
        row[column] = parse_date(row[column]) if row.key?(column)
      end

      Schema::NUMERIC_COLUMNS.each do |column|
        row[column] = parse_decimal(row[column], column:, csv_row_number:) if row.key?(column)
      end

      Schema::INTEGER_COLUMNS.each do |column|
        row[column] = parse_integer(row[column]) if row.key?(column)
      end
    end

    def parse_date(value)
      return nil if value.nil?

      Date.strptime(value, "%m/%d/%Y")
    rescue ArgumentError
      nil
    end

    def parse_decimal(value, column:, csv_row_number:)
      return nil if value.nil?

      normalized_value = normalize_grouping_separators(value.to_s)
      decimal = BigDecimal(normalized_value)
      return decimal if decimal.finite?

      warn_invalid_numeric(column:, csv_row_number:)
      nil
    rescue ArgumentError
      warn_invalid_numeric(column:, csv_row_number:)
      nil
    end

    def normalize_grouping_separators(value)
      return value unless value.include?(",")

      unless value.match?(/\A[+-]?\d{1,3}(?:,\d{3})+(?:\.\d+)?\z/)
        raise ArgumentError, "invalid numeric grouping"
      end

      value.delete(",")
    end

    def warn_invalid_numeric(column:, csv_row_number:)
      @logger.warn(
        "Failed to parse non-empty numeric value; " \
        "column=#{column}, csv_row=#{csv_row_number}. Value was replaced with NULL."
      )
    end

    def parse_integer(value)
      return nil if value.nil?

      Integer(Float(value))
    rescue ArgumentError, TypeError
      nil
    end

    def ensure_schema_columns!(row)
      Schema::JOB_SCHEMA.each do |name, type,|
        row[name] = Schema.default_value(type) unless row.key?(name)
      end
    end

    def build_row_id(row)
      values = row.values_at(*row.keys)
      Digest::SHA256.hexdigest(values.inspect)
    end

    def warn_unmapped_columns
      @logger.warn(
        "Detected unmapped Airbnb CSV columns: #{unmapped_source_columns.sort}. " \
        "These columns are not part of COLUMN_MAP/job_schema and will keep their raw names. " \
        "If this is a new Airbnb export format, add explicit mappings and schema fields before the next production import. " \
        "If the target BigQuery table was created from the old schema, recreate it or update its schema to match."
      )
    end
  end
end
