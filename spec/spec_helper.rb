# encoding: utf-8

if ENV['COVERAGE'] == 'true'
  require 'simplecov'
  require 'coveralls'

  SimpleCov.formatter = SimpleCov::Formatter::MultiFormatter[
    SimpleCov::Formatter::HTMLFormatter,
    Coveralls::SimpleCov::Formatter
  ]

  SimpleCov.start do
    command_name     'spec:unit'
    add_filter       'config/'
    add_filter       'spec/'
    minimum_coverage 94.19  # FIXME: raise to 100
  end
end

require 'dm-core/spec/shared/adapter_spec'
require 'dm-cassandra-adapter/spec/setup'

require 'devtools/spec_helper'

# Capture Ciql query logging
Ciql.logger = Logger.new(StringIO.new)

# require spec support files and shared behavior
Dir[File.expand_path('../{support,shared}/**/*.rb', __FILE__)].each do |file|
  require file
end

module Helpers
  module Example
    def setup_keyspace
      client = Ciql.client
      client.execute("CREATE KEYSPACE datamapper_default_tests WITH replication = {'class': ?, 'replication_factor': ?}", 'SimpleStrategy', 1)
    rescue Cql::QueryError => exception
      raise unless exception.message.include?('Cannot add existing keyspace')
      client.execute('DROP KEYSPACE datamapper_default_tests')
      retry
    end

    def adapter() DataMapper::Spec.adapter end
    def repository() DataMapper.repository(adapter.name) end
  end
end

RSpec.configure do |config|
  config.expect_with :rspec do |expect_with|
    expect_with.syntax = [:should, :expect]
  end

  config.include Helpers::Example
end
