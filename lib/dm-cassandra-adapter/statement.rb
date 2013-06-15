# encoding: utf-8

module DataMapper
  module Adapters
    class CassandraAdapter < AbstractAdapter

      # Base class for command statements
      class Statement
        PLACEHOLDER   = '?'.freeze
        SEPARATOR     = ', '.freeze
        SPACE         = ' '.freeze
        AND           = ' AND '.freeze
        L_PARENTHESIS = '('.freeze
        R_PARENTHESIS = ')'.freeze

      private

        def eql(property)
          "#{property.field} = #{PLACEHOLDER}"
        end

        def parenthesis(*statements)
          "(#{statements.join(SEPARATOR)})"
        end

      end # Statement

    end # CassandraAdapter
  end # Adapters
end # DataMapper
