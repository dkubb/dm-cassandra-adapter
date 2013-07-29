# encoding: utf-8

module DataMapper
  module Adapters
    class CassandraAdapter < AbstractAdapter
      module Command

        # Update records in Cassandra
        class Update < Abstract
          def initialize(adapter, attributes, collection)
            @adapter     = adapter
            @attributes  = attributes
            @collection  = collection
            @model       = @collection.model
            @table       = @model.storage_name(@adapter.name)
            @consistency = consistency_for(@model, :write)
          end

          def call
            # TODO: batch the statements
            # TODO: handle bulk updates with IN() when there is one key
            @collection.each do |resource|
              key       = Hash[@model.key.zip(resource.key)]
              statement = Statement.new(@table, key, resource.dirty_attributes)
              statement.run(@adapter.method(:execute), @consistency)
            end
            self
          end

          def count
            @collection.count
          end

          class Statement < Statement
            UPDATE = 'UPDATE %{table} SET %{columns} WHERE %{where}'.freeze

            def initialize(table, key, attributes)
              @table      = table
              @key        = key
              @attributes = attributes
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
      end # Command
    end # CassandraAdapter
  end # Adapters
end # DataMapper
