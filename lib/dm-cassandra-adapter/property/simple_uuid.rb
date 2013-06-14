# encoding: utf-8

require 'dm-core'
require 'simple_uuid'

module DataMapper
  class Property

    # Store a SimpleUUID uuid
    class SimpleUUID < Object
      accept_options :serial

      primitive ::SimpleUUID::UUID
      serial    true  # An adapter generated default
      unique    true

      def typecast(object)
        primitive.new(object) unless object.nil?
      end

      alias load typecast
      alias dump typecast

    end # SimpleUUID
  end # Property
end # DataMapper
