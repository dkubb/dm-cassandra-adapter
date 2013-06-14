# encoding: utf-8

require 'spec_helper'

ENV['ADAPTER']          = 'cassandra'
ENV['ADAPTER_SUPPORTS'] = 'all'

describe 'DataMapper::Adapters::CassandraAdapter' do
  def setup_keyspace
    client = Ciql.client
    client.execute("CREATE KEYSPACE datamapper_default_tests WITH replication = {'class': ?, 'replication_factor': ?}", 'SimpleStrategy', 1)
  rescue CassandraCQL::Error::InvalidRequestException => exception
    raise unless exception.message.include?('Cannot add existing keyspace')
    client.execute('DROP KEYSPACE datamapper_default_tests')
    retry
  end

  def create_table
    adapter.execute('CREATE TABLE heffalumps (id timeuuid PRIMARY KEY, color text, num_spots int, striped boolean)')
  end

  # Use methods to avoid let() in before(:all) warnings
  def adapter() DataMapper::Spec.adapter end
  def repository() DataMapper.repository(adapter.name) end

  # Define a custom model with a UUID PK
  def heffalump_model
    @model ||= Class.new {
      include DataMapper::Resource

      property :id,        DataMapper::Property::SimpleUUID
      property :color,     DataMapper::Property::String
      property :num_spots, DataMapper::Property::Integer
      property :striped,   DataMapper::Property::Boolean

      # This is needed for DataMapper.finalize
      def self.name() 'Heffalump' end
    }.tap { DataMapper.finalize }
  end

  before :all do
    setup_keyspace
    create_table
  end

  it_should_behave_like 'An Adapter'
end
