# encoding: utf-8

module DataMapper
  module Adapters
    class CassandraAdapter < AbstractAdapter
      class Command

        # Update records in Cassandra
        class Update < self
          def initialize(adapter, attributes, collection)
            @adapter    = adapter
            @attributes = attributes
            @collection = collection
          end

          def call
            # TODO: batch the statements
            # TODO: handle bulk updates with IN() when there is one key
            @collection.each do |resource|
              model      = resource.model
              table      = model.storage_name(@adapter.name)
              attributes = resource.dirty_attributes
              key        = Hash[model.key.zip(resource.key)]

              statement = Statement.new(table, attributes, key)
              @adapter.execute(statement.to_s, *statement.bind_variables)
            end
            self
          end

          def count
            @collection.count
          end

          class Statement
            UPDATE      = 'UPDATE %{table} SET %{attributes} WHERE %{key}'.freeze
            PLACEHOLDER = '?'.freeze
            SEPARATOR   = ', '.freeze
            AND         = ' AND '.freeze

            def initialize(table, attributes, key)
              @table      = table
              @attributes = attributes
              @key        = key
            end

            def bind_variables
              @attributes.values + @key.values
            end

            def to_s
              UPDATE % {
                table:      @table,
                attributes: attributes_clause,
                key:        key_clause,
              }
            end

          private

            def attributes_clause
              @attributes.keys.map { |property|
                "#{property.field} = #{PLACEHOLDER}"
              }.join(SEPARATOR)
            end

            def key_clause
              @key.keys.map { |property| "#{property.field} = #{PLACEHOLDER}" }.
                join(AND)
            end

          end # Statement
        end # Update
      end # Statement
    end # CassandraAdapter
  end # Adapters
end # DataMapper
