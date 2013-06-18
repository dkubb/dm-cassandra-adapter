# encoding: utf-8

module DataMapper
  module Adapters

    # Cassandra DataMapper Adapter
    class CassandraAdapter < AbstractAdapter

      def initialize(*)
        super
        setup_keyspace
        setup_consistency
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

      def select(*args)
        log(*args) { @client.execute(*args).map(&:to_hash) }
      end

      def execute(*args)
        log(*args) { @client.execute(*args, @consistency) }
        nil
      end

    private

      def setup_keyspace
        @keyspace = options.fetch(:keyspace) { options.fetch(:path)[1..-1] }
      end

      def setup_consistency
        @consistency = options.fetch(:consistency, :any)
      end

      def setup_client
        @client = Ciql::Client::Thrift.new(
          options.merge(keyspace: @keyspace).symbolize_keys
        )
      end

      def log(*args, &block)
        times = Benchmark.measure(&block)
        DataMapper.logger.debug do
          '(%<total>.6fms) %{statement}' % {
            total:     times.real * 10**3,
            statement: sanitize(*args),
          }
        end
      end

      def sanitize(statement, *bind_variables)
        Ciql::Sanitize.sanitize(
          statement.gsub(/\s+/, ' ').strip,
          *bind_variables
        )
      end

    end # CassandraAdapter

    const_added :CassandraAdapter

  end # Adapters
end # DataMapper
