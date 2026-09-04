# frozen_string_literal: true

require "date"
require "digest"

module AirbnbPayous
  class CumulativeSnapshot
    FILE_PATTERN = /\Aairbnb_01_(?<start_year>\d{4})-(?<end_month>\d{2})_(?<end_year>\d{4})(?: \(\d+\))?\.csv\z/i

    attr_reader :id, :event_year, :through_date, :source_generation, :source_created_at, :row_count

    def self.build(file_name:, content:, rows:, source_generation:, source_created_at:)
      match = File.basename(file_name).match(FILE_PATTERN)
      return nil unless match
      return nil if source_generation.nil? || source_created_at.nil?

      dates = rows.map { |row| row["event_date"] }
      raise ArgumentError, "cumulative snapshots must contain at least one row" if dates.empty?
      raise ArgumentError, "cumulative snapshots require event_date on every row" if dates.any?(&:nil?)

      years = dates.map(&:year).uniq
      unless years.one?
        raise ArgumentError, "cumulative snapshots must contain exactly one event year"
      end

      event_year = years.first
      start_year = Integer(match[:start_year], 10)
      unless event_year == start_year
        raise ArgumentError, "cumulative snapshot rows must match the filename start year"
      end

      coverage_end = coverage_end_date(
        start_year:,
        end_month: Integer(match[:end_month], 10),
        end_year: Integer(match[:end_year], 10)
      )
      if dates.any? { |date| date > coverage_end }
        raise ArgumentError, "cumulative snapshot rows exceed the filename coverage"
      end

      new(
        id: Digest::SHA256.hexdigest(content),
        event_year:,
        through_date: coverage_end,
        source_generation: Integer(source_generation),
        source_created_at: source_created_at.to_time.utc,
        row_count: rows.length
      )
    end

    def self.coverage_end_date(start_year:, end_month:, end_year:)
      if end_year == start_year && (1..12).cover?(end_month)
        return Date.new(end_year, end_month, -1)
      end

      if end_year == start_year + 1 && end_month == 1
        return Date.new(start_year, 12, 31)
      end

      raise ArgumentError, "unsupported cumulative snapshot filename range"
    end

    private_class_method :coverage_end_date

    def initialize(id:, event_year:, through_date:, source_generation:, source_created_at:, row_count:)
      @id = id
      @event_year = event_year
      @through_date = through_date
      @source_generation = source_generation
      @source_created_at = source_created_at
      @row_count = row_count
    end
  end
end
