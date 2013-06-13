# encoding: utf-8

module DataMapper
  module Adapters

    # Cassandra DataMapper Adapter
    class CassandraAdapter < AbstractAdapter

      def initialize(name, options = {})
        # TODO: use the options to specify the keyspace
        super
        @client      = Ciql.client
        @consistency = options.fetch(:consistency, :any)
      end

      def create(resources)
        # Execute creation of all resources
        Command::Create.new(self, resources).call.count
      end

      def read(query)
        # Execute the read command then filter the records in-memory
        query.filter_records(Command::Read.new(self, query).to_a)
      end

      def update(attributes, collection)
        # Update each resource in the collection
        Command::Update.new(self, attributes, collection).call.count
      end

      def delete(collection)
        # Delete each resource in the collection
        Command::Delete.new(self, collection).call.count
      end

      def select(statement, *bind_variables)
        @client.execute(statement, *bind_variables).each do |row|
          row.each do |key, value|
            row[key] = SimpleUUID::UUID.new(value.to_s) if value.kind_of?(Cql::Uuid)
          end
        end
      end

      def execute(statement, *bind_variables)
        # TODO: make this return the expected results
        @client.execute(statement, *bind_variables, @consistency)
      end

    end # CassandraAdapter

    const_added :CassandraAdapter

  end # Adapters
end # DataMapper
