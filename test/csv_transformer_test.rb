# frozen_string_literal: true

require "bigdecimal"
require "logger"
require "stringio"

require_relative "test_helper"
require_relative "../lib/airbnb_payous/csv_transformer"

class CsvTransformerTest < Minitest::Test
  def setup
    @log_output = StringIO.new
    @logger = Logger.new(@log_output)
    @transformer = AirbnbPayous::CsvTransformer.new(logger: @logger)
  end

  def test_normalizes_mapped_headers_and_types
    csv = <<~CSV
      日付,種別,泊数,金額,Airbnb remitted tax
      03/12/2026,予約,2,150.50,12.34
    CSV

    rows = @transformer.call(csv)

    assert_equal 1, rows.length
    assert_equal Date.new(2026, 3, 12), rows.first["event_date"]
    assert_equal "予約", rows.first["type"]
    assert_equal 2, rows.first["number_of_nights"]
    assert_equal BigDecimal("150.50"), rows.first["amount"]
    assert_equal BigDecimal("12.34"), rows.first["airbnb_remitted_tax"]
    assert_nil rows.first["pet_fee"]
    assert_match(/\A\h{64}\z/, rows.first["row_id"])
  end

  def test_warns_and_preserves_unmapped_columns
    csv = <<~CSV
      日付,Unexpected Airbnb Column,金額
      03/12/2026,foo,100.00
    CSV

    rows = @transformer.call(csv)

    assert_equal "foo", rows.first["Unexpected Airbnb Column"]
    assert_includes @log_output.string, "Detected unmapped Airbnb CSV columns"
    assert_includes @log_output.string, "Unexpected Airbnb Column"
  end

  def test_coerces_invalid_dates_and_numbers_to_nil
    csv = <<~CSV
      日付,泊数,金額
      invalid,abc,NaN
    CSV

    rows = @transformer.call(csv)

    assert_nil rows.first["event_date"]
    assert_nil rows.first["number_of_nights"]
    assert_nil rows.first["amount"]
    assert_includes @log_output.string, "column=amount, csv_row=2"
  end

  def test_normalizes_comma_grouped_numeric_values
    csv = <<~CSV
      日付,金額,サービス料
      06/04/2026,"-1,234.50","4,692"
    CSV

    rows = @transformer.call(csv)

    assert_equal BigDecimal("-1234.50"), rows.first["amount"]
    assert_equal BigDecimal("4692"), rows.first["service_fee"]
  end

  def test_rejects_invalid_numeric_grouping_with_a_safe_warning
    csv = <<~CSV
      日付,サービス料
      06/04/2026,"12,34"
    CSV

    rows = @transformer.call(csv)

    assert_nil rows.first["service_fee"]
    assert_includes @log_output.string, "column=service_fee, csv_row=2"
    refute_includes @log_output.string, "12,34"
  end

  def test_equivalent_numeric_formats_produce_the_same_row_id
    ungrouped_csv = <<~CSV
      日付,種別,サービス料
      06/04/2026,予約,4692.00
    CSV
    grouped_csv = <<~CSV
      日付,種別,サービス料
      06/04/2026,予約,"4,692"
    CSV

    ungrouped_row = @transformer.call(ungrouped_csv).first
    grouped_row = @transformer.call(grouped_csv).first

    assert_equal ungrouped_row["service_fee"], grouped_row["service_fee"]
    assert_equal ungrouped_row["row_id"], grouped_row["row_id"]
  end

  def test_strips_bom_and_empty_values
    csv = "\xEF\xBB\xBF日付,詳細,金額\n03/12/2026, ,\n"

    rows = @transformer.call(csv)

    assert_equal Date.new(2026, 3, 12), rows.first["event_date"]
    assert_nil rows.first["details"]
    assert_nil rows.first["amount"]
  end

  def test_maps_new_japanese_tax_column
    csv = <<~CSV
      日付,Airbnbが納税する自動設定された税金,金額
      03/12/2026,12.34,100.00
    CSV

    rows = @transformer.call(csv)

    assert_equal BigDecimal("12.34"), rows.first["airbnb_remitted_tax"]
    assert_equal BigDecimal("100.00"), rows.first["amount"]
  end
end
