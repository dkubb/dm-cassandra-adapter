# encoding: utf-8

module DataMapper
  module Adapters
    class CassandraAdapter < AbstractAdapter

      # Base class for command statements
      class Statement
        include Constants

        def run(method, consistency = :quorum)
          method.call(to_s, *bind_variables, consistency)
        end

      protected

        def and(statements)
          statements.join(SPACE + AND + SPACE)
        end

      private

        def field(property)
          property.field.dup
        end

        def eql(property)
          "#{field(property)} #{EQUALS_SIGN} #{PLACEHOLDER}"
        end

        def join(statements)
          statements.join
        end

        def list(statements)
          statements.join(SEPARATOR)
        end

        def parenthesis(statements)
          L_PARENTHESIS + list(Array(statements)) + R_PARENTHESIS
        end

      end # Statement
    end # CassandraAdapter
  end # Adapters
end # DataMapper
