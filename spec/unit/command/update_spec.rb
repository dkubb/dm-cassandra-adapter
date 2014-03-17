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
end
