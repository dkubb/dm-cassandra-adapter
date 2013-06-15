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

      protected

        def and(statements)
          statements.join(AND)
        end

      private

        def eql(property)
          "#{property.field} = #{PLACEHOLDER}"
        end

        def join(statements)
          statements.join(SPACE)
        end

        def list(statements)
          statements.join(SEPARATOR)
        end

        def parenthesis(statements)
          "(#{list(statements)})"
        end

      end # Statement

    end # CassandraAdapter
  end # Adapters
end # DataMapper
