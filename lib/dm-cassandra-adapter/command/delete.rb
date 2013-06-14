# encoding: utf-8

module DataMapper
  module Adapters
    class CassandraAdapter < AbstractAdapter
      class Command

        # Delete records in Cassandra
        class Delete < self
          def initialize(adapter, collection)
            @adapter    = adapter
            @collection = collection
          end

          def call
            # TODO: batch the statements
            # TODO: handle bulk deletes with IN() when there is one key
            @collection.each do |resource|
              model = resource.model
              table = model.storage_name(@adapter.name)
              key   = Hash[model.key.zip(resource.key)]

              statement = Statement.new(table, key)
              @adapter.execute(statement.to_s, *statement.bind_variables)
            end
            self
          end

          def count
            @collection.count
          end

          class Statement
            DELETE      = 'DELETE FROM %{table} WHERE %{key}'.freeze
            PLACEHOLDER = '?'.freeze
            AND         = ' AND '.freeze

            def initialize(table, key)
              @table = table
              @key   = key
            end

            def bind_variables
              @key.values
            end

            def to_s
              DELETE % { table: @table, key: key_clause }
            end

          private

            def key_clause
              @key.keys.map { |property| "#{property.field} = #{PLACEHOLDER}" }.
                join(AND)
            end

          end # Statement
        end # Delete
      end # Statement
    end # CassandraAdapter
  end # Adapters
end # DataMapper
