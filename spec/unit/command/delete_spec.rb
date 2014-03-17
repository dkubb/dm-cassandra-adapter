# encoding: utf-8

require 'spec_helper'
require 'dm-types'

describe DataMapper::Adapters::CassandraAdapter::Command::Delete do
  let(:adapter) { DataMapper::Spec.adapter }

  let(:model) do
    Class.new {
      include DataMapper::Resource
      property :type, DataMapper::Property::Enum[:one], key: true
      # This is needed for DataMapper.finalize
      def self.name() 'EnumKey' end
    }.tap { DataMapper.finalize }
  end

  let(:collection) do
    [ model.new(type: :one) ].tap do |collection|
      collection.stub(:model).and_return(model)
    end
  end

  subject { described_class.new(adapter, collection) }

  describe '#call' do
    it 'should correctly convert the enum portion of the key' do
      adapter.should_receive(:execute).with(
        'DELETE FROM enum_keys WHERE type = ?', 1, nil
      )
      subject.call
    end
  end
end
