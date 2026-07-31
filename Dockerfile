FROM ruby:4.0.6-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

ENV BUNDLE_WITHOUT=development \
    BUNDLE_DEPLOYMENT=1 \
    BUNDLE_PATH=/usr/local/bundle

COPY Gemfile Gemfile.lock ./
RUN bundle install

COPY . .

ENV PORT=8080

CMD ["bundle", "exec", "puma", "-C", "config/puma.rb"]
