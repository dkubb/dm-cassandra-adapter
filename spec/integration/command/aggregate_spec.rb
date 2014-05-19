# encoding: utf-8

require 'spec_helper'

ENV['ADAPTER'] = 'cassandra'

describe 'aggregate support' do
  before :all do
    setup_keyspace
    adapter.execute("""
      CREATE TABLE heffalumps (
        user varint,
        type varint,
        time timestamp,
        PRIMARY KEY (user, type, time)
      )
    """)
  end

  let(:model) do
    Class.new {
      include DataMapper::Resource
      property :user, Integer,  key: true
      property :type, Integer,  key: true
      property :time, DateTime, key: true
      # This is needed for DataMapper.finalize
      def self.name() 'Heffalump' end
    }.tap { DataMapper.finalize }
  end

  subject { described_class }

  describe '#count' do
    before do
      model.destroy
      model.create(user: 1, type: 1, time: Time.now)
      model.create(user: 1, type: 2, time: Time.now)
      model.create(user: 2, type: 1, time: Time.now)
    end

    it 'returns the number of rows' do
      model.count.should eq 3
    end

    describe 'with conditions' do
      it 'returns the number of matching rows' do
        model.all(user: 1).count.should eq 2
        model.all(user: 1, type: 2).count.should eq 1
      end
    end
  end
end
