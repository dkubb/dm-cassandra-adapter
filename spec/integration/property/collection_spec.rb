# encoding: utf-8

require 'spec_helper'

ENV['ADAPTER'] = 'cassandra'

describe 'collection support' do
  def create_table(type)
    adapter.execute("""
      CREATE TABLE heffalumps (
        id varint,
        content #{type},
        PRIMARY KEY (id)
      )
    """)
  end

  def define_model(type)
    Class.new do
      include DataMapper::Resource

      property :id,      DataMapper::Property::Serial
      property :content, type

      # This is needed for DataMapper.finalize
      def self.name() 'Heffalump' end
    end.tap { DataMapper.finalize }
  end

  shared_examples 'a collection property' do
    let(:model) { define_model(property) }
    let(:empty_example) { property.primitive.new }

    subject { model.new }

    context 'a new model' do
      it 'has an empty collection' do
        subject.content.should eq empty_example
      end

      it 'the collection is frozen' do
        subject.content.should be_frozen
      end

      it 'persists an empty collection' do
        subject.save.should be true
        subject.reload.content.should eq empty_example
      end

      it 'different models should not share the same default instance' do
        other = model.new
        subject.content.should_not be other.content
        other.content = first_example
        subject.content.should eq empty_example
      end
    end

    context 'saving and reloading a model' do
      before do
        subject.content = combined_example
        subject.save.should be true
        subject.reload
      end

      it 'persists the collection' do
        subject.content.should eq reloaded.call(combined_example)
      end

      it 'the reloaded collection is frozen' do
        subject.content.should be_frozen
      end
    end

    context 'updating a model' do
      it 'persists a new collection' do
        subject.content = first_example
        subject.save.should be true
        subject.content = second_example
        subject.save.should be true
        subject.reload.content.should eq reloaded.call(second_example)
      end
    end
  end

  describe 'Map' do
    before :all do
      setup_keyspace
      create_table('map<varint,varchar>')
    end

    let(:property) { DataMapper::Property::Map }

    let(:first_example)  { { 123 => 'abc' } }
    let(:second_example) { { 456 => 'xyz' } }

    let(:combined_example) { first_example.merge(second_example) }

    let(:reloaded) { lambda { |c| c } }

    it_behaves_like 'a collection property'
  end

  describe 'List' do
    before :all do
      setup_keyspace
      create_table('list<varchar>')
    end

    let(:property) { DataMapper::Property::List }

    let(:first_example)  { ['abc'] }
    let(:second_example) { ['xyz'] }

    let(:combined_example) { first_example + second_example }

    let(:reloaded) { lambda { |c| c } }

    it_behaves_like 'a collection property'
  end

  describe 'Set' do
    before :all do
      setup_keyspace
      create_table('set<timestamp>')
    end

    let(:property) { DataMapper::Property::Set }

    let(:first_example)  { [Date.today    ].to_set }
    let(:second_example) { [Date.today - 1].to_set }

    let(:combined_example) { first_example + second_example }

    let(:reloaded) { lambda { |c| c.map(&:to_time).to_set } }

    it_behaves_like 'a collection property'
  end
end
