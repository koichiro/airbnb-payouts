# frozen_string_literal: true

require "date"
require "time"

require_relative "test_helper"
require_relative "../lib/airbnb_payous/cumulative_snapshot"

class CumulativeSnapshotTest < Minitest::Test
  def test_builds_metadata_for_a_single_year_cumulative_export
    snapshot = AirbnbPayous::CumulativeSnapshot.build(
      file_name: "drive/id/hash/airbnb_01_2026-08_2026 (2).csv",
      content: "csv-content",
      rows: rows_for(Date.new(2026, 1, 4), Date.new(2026, 8, 27)),
      source_generation: "123",
      source_created_at: Time.utc(2026, 8, 29, 4, 12, 44)
    )

    assert_equal Digest::SHA256.hexdigest("csv-content"), snapshot.id
    assert_equal 2026, snapshot.event_year
    assert_equal Date.new(2026, 8, 31), snapshot.through_date
    assert_equal 123, snapshot.source_generation
    assert_equal 2, snapshot.row_count
  end

  def test_ignores_files_that_do_not_follow_the_cumulative_export_name
    snapshot = AirbnbPayous::CumulativeSnapshot.build(
      file_name: "manual-adjustment.csv",
      content: "csv-content",
      rows: rows_for(Date.new(2026, 8, 27)),
      source_generation: 123,
      source_created_at: Time.now.utc
    )

    assert_nil snapshot
  end

  def test_requires_storage_metadata_before_reconciliation
    snapshot = AirbnbPayous::CumulativeSnapshot.build(
      file_name: "airbnb_01_2026-08_2026.csv",
      content: "csv-content",
      rows: rows_for(Date.new(2026, 8, 27)),
      source_generation: nil,
      source_created_at: nil
    )

    assert_nil snapshot
  end

  def test_rejects_multiple_event_years
    error = assert_raises(ArgumentError) do
      AirbnbPayous::CumulativeSnapshot.build(
        file_name: "airbnb_01_2025-01_2026.csv",
        content: "csv-content",
        rows: rows_for(Date.new(2025, 12, 31), Date.new(2026, 1, 1)),
        source_generation: 123,
        source_created_at: Time.now.utc
      )
    end

    assert_includes error.message, "exactly one event year"
  end

  def test_accepts_a_full_year_export_ending_in_january_of_the_next_year
    snapshot = AirbnbPayous::CumulativeSnapshot.build(
      file_name: "airbnb_01_2025-01_2026.csv",
      content: "csv-content",
      rows: rows_for(Date.new(2025, 1, 2), Date.new(2025, 12, 29)),
      source_generation: 123,
      source_created_at: Time.now.utc
    )

    assert_equal 2025, snapshot.event_year
    assert_equal Date.new(2025, 12, 31), snapshot.through_date
  end

  def test_rejects_rows_that_do_not_match_the_filename_start_year
    error = assert_raises(ArgumentError) do
      AirbnbPayous::CumulativeSnapshot.build(
        file_name: "airbnb_01_2026-08_2026.csv",
        content: "csv-content",
        rows: rows_for(Date.new(2025, 8, 27)),
        source_generation: 123,
        source_created_at: Time.now.utc
      )
    end

    assert_includes error.message, "filename start year"
  end

  def test_rejects_unsupported_filename_ranges
    error = assert_raises(ArgumentError) do
      AirbnbPayous::CumulativeSnapshot.build(
        file_name: "airbnb_01_2026-02_2027.csv",
        content: "csv-content",
        rows: rows_for(Date.new(2026, 8, 27)),
        source_generation: 123,
        source_created_at: Time.now.utc
      )
    end

    assert_includes error.message, "unsupported cumulative snapshot filename range"
  end

  def test_rejects_missing_event_dates
    error = assert_raises(ArgumentError) do
      AirbnbPayous::CumulativeSnapshot.build(
        file_name: "airbnb_01_2026-08_2026.csv",
        content: "csv-content",
        rows: [{ "event_date" => nil }],
        source_generation: 123,
        source_created_at: Time.now.utc
      )
    end

    assert_includes error.message, "event_date on every row"
  end

  private

  def rows_for(*dates)
    dates.map { |date| { "event_date" => date } }
  end
end
