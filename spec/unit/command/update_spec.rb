# encoding: utf-8

require 'spec_helper'
require 'dm-types'

describe DataMapper::Adapters::CassandraAdapter::Command::Update do
  let(:adapter) { DataMapper::Spec.adapter }

  let(:model) do
    Class.new {
      include DataMapper::Resource
      property :type, DataMapper::Property::Enum[:one], key: true
      property :size, Integer
      # This is needed for DataMapper.finalize
      def self.name() 'EnumKey' end
    }.tap { DataMapper.finalize }
  end

  let(:resource) do
    model.new(type: :one, size: 9).tap do |resource|
      resource.persistence_state =
        DataMapper::Resource::PersistenceState::Dirty.new(resource)
      resource.persistence_state.set(model.properties[:size], 10)
    end
  end

  let(:collection) do
    [ resource ].tap do |collection|
      collection.stub(:model).and_return(model)
    end
  end

  subject { described_class.new(adapter, {}, collection) }

  describe '#call' do
    it 'should correctly convert the enum portion of the key' do
      adapter.should_receive(:execute).with(
        'UPDATE enum_keys SET size = ? WHERE type = ?', 10, 1, nil
      )
      subject.call
    end
  end

  describe '#count' do
    describe 'when the collection is loaded' do
      let(:collection) { resource.collection_for_self }

      it 'should not execute a count query' do
        DataMapper::Collection.any_instance.should_not_receive(:count)
        subject.count
      end
    end

    describe 'when the collection is not loaded' do
      let(:collection) { model.all(type: :one) }

      before do
        DataMapper::Collection.any_instance.should_receive(:count).and_return
      end

      it 'should execute a count query' do
        subject.count
      end

      it 'does not load the whole collection' do
        DataMapper::Collection.any_instance.should_not_receive(:lazy_load)
        subject.count
      end
    end
  end
end
