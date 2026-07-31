# frozen_string_literal: true

require "simplecov"

SimpleCov.start do
  minimum_coverage 80
  skip "/test/"
end

require "minitest/autorun"
require "minitest/mock"
require "rack/test"

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
$LOAD_PATH.unshift(File.expand_path("..", __dir__))

require_relative "../app"
