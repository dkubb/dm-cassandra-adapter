# encoding: utf-8

module DataMapper
  module Adapters
    class CassandraAdapter < AbstractAdapter
      module Command

        # Update records in Cassandra
        class Update
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

          class Statement < Statement
            UPDATE = 'UPDATE %{table} SET %{columns} WHERE %{where}'.freeze

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
                table:   @table,
                columns: list(columns),
                where:   self.and(where)
              }
            end

          private

            def columns
              @attributes.keys.map(&method(:eql))
            end

            def where
              @key.keys.map(&method(:eql))
            end

          end # Statement
        end # Update
      end # Statement
    end # CassandraAdapter
  end # Adapters
end # DataMapper
