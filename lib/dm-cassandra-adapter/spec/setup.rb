# encoding: utf-8

require 'dm-cassandra-adapter'
require 'dm-core/spec/setup'

module DataMapper
  module Spec

    def self.require_plugins
      # No-op
    end

    module Adapters
      class CassandraAdapter < Adapter

        def connection_uri
          'cassandra://localhost:9160/datamapper_default_tests'
        end

        def test_connection(adapter)
          # No-op
        end

      end # CassandraAdapter

      use CassandraAdapter

    end # Adapters
  end # Spec
end # DataMapper
