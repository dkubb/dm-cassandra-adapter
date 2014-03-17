# encoding: utf-8

require 'spec_helper'

describe DataMapper::Adapters::CassandraAdapter::Command::Read do
  let(:adapter) { DataMapper::Spec.adapter }

  let(:model) do
    Class.new {
      include DataMapper::Resource
      property :id,   Integer, key: true
      property :time, DateTime, key: true
      property :size, Integer
      # This is needed for DataMapper.finalize
      def self.name() 'Model' end
    }.tap { DataMapper.finalize }
  end

  subject { described_class.new(adapter, query) }

  def expect_select(*args)
    adapter.should_receive(:select).with(*args).and_return([])
    subject.to_a
  end

  describe 'with a limit' do
    let(:query) do
      model.all(limit: 5).query
    end

    it 'generates a query with the limit' do
      expect_select(
        'SELECT id, time, size FROM models LIMIT 5', nil
      )
    end
  end

  describe 'with an order' do
    let(:query) do
      model.all(id: 1, order: [:time]).query
    end

    it 'generates a query with the order' do
      expect_select(
        'SELECT id, time, size FROM models WHERE id=? ORDER BY time', 1, nil
      )
    end

    describe 'descending' do
      let(:query) do
        model.all(id: 1, order: :time.desc).query
      end

      it 'generates a query with the order' do
        expect_select(
          'SELECT id, time, size FROM models WHERE id=? ORDER BY time DESC', 1, nil
        )
      end
    end

    describe "that matches the model's key" do
      let(:query) do
        model.all(id: 1, order: [:id, :time]).query
      end

      it 'ignores the order' do
        expect_select(
          'SELECT id, time, size FROM models WHERE id=?', 1, nil
        )
      end
    end
  end
end
