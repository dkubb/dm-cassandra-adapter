# encoding: utf-8

require 'spec_helper'

ENV['ADAPTER']          = 'cassandra'
ENV['ADAPTER_SUPPORTS'] = 'all'

describe 'DataMapper::Adapters::CassandraAdapter' do

  context 'with a generated UUID key' do
    # Define a table with a UUID key
    def create_table
      adapter.execute('CREATE TABLE heffalumps (id timeuuid PRIMARY KEY, color text, num_spots int, striped boolean)')
    end

    # Define a custom model with a UUID key
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

  context 'with a generated Integer key' do
    # Define a table with an Integer key
    def create_table
      adapter.execute('CREATE TABLE heffalumps (id bigint PRIMARY KEY, color text, num_spots int, striped boolean)')
    end

    # Define a custom model with an Integer key
    def heffalump_model
      @model ||= Class.new {
        include DataMapper::Resource

        property :id,        DataMapper::Property::Serial
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
end
