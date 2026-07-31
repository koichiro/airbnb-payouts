# frozen_string_literal: true

max_threads_count = ENV.fetch("PUMA_MAX_THREADS", "5").to_i

threads max_threads_count, max_threads_count
port ENV.fetch("PORT", "8080"), "0.0.0.0"
environment ENV.fetch("RACK_ENV", "production")
