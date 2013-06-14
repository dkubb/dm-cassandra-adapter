# encoding: utf-8

module DataMapper
  module Adapters

    # Cassandra DataMapper Adapter
    class CassandraAdapter < AbstractAdapter

      def initialize(*)
        super
        @consistency = options.fetch(:consistency, :any)
        setup_client
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
        @client.execute(statement, *bind_variables).map(&:to_hash)
      end

      def execute(statement, *bind_variables)
        # TODO: make this return the expected results
        @client.execute(statement, *bind_variables, @consistency)
      end

    private

      def setup_client
        @client = Ciql::Client::Thrift.new(
          options.merge(keyspace: options.fetch(:path)[1..-1]).symbolize_keys
        )
      end

    end # CassandraAdapter

    const_added :CassandraAdapter

  end # Adapters
end # DataMapper
