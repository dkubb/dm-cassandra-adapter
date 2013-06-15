# encoding: utf-8

module DataMapper
  module Adapters
    class CassandraAdapter < AbstractAdapter

      # Constants for statement generation
      module Constants
        SEPARATOR = ', '.freeze
        SPACE     = ' '.freeze

        PLACEHOLDER = '?'.freeze
        AND         = 'AND'.freeze
        IN          = 'IN'.freeze
        DESC        = 'DESC'.freeze

        L_PARENTHESIS = '('.freeze
        R_PARENTHESIS = ')'.freeze

        EQUALS_SIGN              = '='.freeze
        GREATER_THAN_SIGN        = '>'.freeze
        LESS_THAN_SIGN           = '<'.freeze
        GREATER_THAN_OR_EQUAL_TO = '>='.freeze
        LESS_THAN_OR_EQUAL_TO    = '<='.freeze

      end # Constants
    end # CassandraAdapter
  end # Adapters
end # DataMapper
