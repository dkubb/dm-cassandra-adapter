# encoding: utf-8

require 'dm-core'
require 'set'

module DataMapper
  class Property

    class Collection < Object
      default lambda { |resource, property| property.primitive.new.freeze }

      def typecast(object)
        object.freeze or primitive.new.freeze
      end

      alias load typecast
      alias dump typecast
    end

    class Map < Collection
      primitive ::Hash
    end

    class List < Collection
      primitive ::Array
    end

    class Set < Collection
      primitive ::Set
    end
  end
end
