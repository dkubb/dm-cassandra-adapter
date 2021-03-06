# encoding: utf-8

module DataMapper
  module Adapters
    class CassandraAdapter < AbstractAdapter
      module Command

        # Delete records in Cassandra
        class Delete < Abstract
          def initialize(adapter, collection)
            @adapter     = adapter
            @collection  = collection
            @model       = @collection.model
            @table       = @model.storage_name(@adapter.name)
            @consistency = consistency_for(@collection.model, :write)
          end

          def call
            # TODO: batch the statements
            # TODO: handle bulk deletes with IN() when there is one key
            @collection.each do |resource|
              key       = Hash[@model.key.zip(resource.key)]
              statement = Statement.new(@table, key)
              statement.run(@adapter.method(:execute), @consistency)
            end
            self
          end

          def count
            @collection.count
          end

          class Statement < Statement
            DELETE = 'DELETE FROM %{table} WHERE %{where}'.freeze

            def initialize(table, key)
              @table = table
              @key   = key
            end

            def bind_variables
              @key.map { |key, value| key.dump(value) }
            end

            def to_s
              DELETE % {
                table: @table,
                where: self.and(where),
              }
            end

          private

            def where
              @key.keys.map(&method(:eql))
            end

          end # Statement
        end # Delete
      end # Command
    end # CassandraAdapter
  end # Adapters
end # DataMapper
