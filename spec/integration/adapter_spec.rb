# encoding: utf-8

require 'spec_helper'

ENV['ADAPTER']          = 'cassandra'
ENV['ADAPTER_SUPPORTS'] = 'all'

describe 'DataMapper::Adapters::CassandraAdapter' do
  def setup_keyspace
    @adapter.execute('DROP KEYSPACE datamapper_default_tests')
  rescue Cql::QueryError
    # Allow failures when the keyspace does not exist
  ensure
    @adapter.execute("CREATE KEYSPACE datamapper_default_tests WITH replication = {'class': ?, 'replication_factor': ?}", 'SimpleStrategy', 1)
  end

  def use_keyspace
    @adapter.execute('USE datamapper_default_tests')
  end

  def create_table
    @adapter.execute('CREATE TABLE heffalumps (id bigint PRIMARY KEY, color text, num_spots int, striped boolean)')
  end

  before :all do
    @adapter    = DataMapper::Spec.adapter
    @repository = DataMapper.repository(@adapter.name)

    setup_keyspace
    use_keyspace
    create_table
  end

  it_should_behave_like 'An Adapter'
end
