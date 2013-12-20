# encoding: utf-8

require 'dm-core'
require 'simple_uuid'
require 'cql/uuid'
require 'cql/time_uuid'

# Make Cql::Uuid instances comparable
module Cql
  class Uuid
    include Comparable

    def <=>(other)
      to_s <=> other.to_s
    end
  end

  class TimeUuid
    def <=>(other)
      result = to_time <=> other.to_time
      result == 0 ? super : result
    end
  end
end

module DataMapper
  class Property

    # Store a SimpleUUID uuid
    class SimpleUUID < Object
      accept_options :serial

      primitive ::SimpleUUID::UUID
      serial    true  # An adapter generated default
      unique    true

      def typecast(object)
        primitive.new(raw_value(object)) unless object.nil?
      end

      alias load typecast
      alias dump typecast

    private

      def raw_value(object)
        case object
        when Cql::Uuid then object.to_s
        else object
        end
      end
    end # SimpleUUID
  end # Property
end # DataMapper
