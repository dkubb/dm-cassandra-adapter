# encoding: utf-8

module DataMapper
  module Adapters
    class CassandraAdapter < AbstractAdapter
      module Command

        # Read record in Cassandra
        class Read < Abstract
          include Enumerable

          def initialize(adapter, query)
            @adapter     = adapter
            @query       = query
            @table       = @query.model.storage_name(@adapter.name)
            @consistency = consistency_for(@query.model, :read)
          end

          def each(&block)
            return to_enum unless block
            rows = statement.run(@adapter.method(:select), @consistency)
            rows.each(&block)
            self
          end

        protected

          def fields
            @query.fields.map(&:field)
          end

          def statement
            conditions = @query.conditions
            order      = @query.order
            limit      = @query.limit if @query.offset.zero?
            Statement.new(@table, fields, conditions, order, limit)
          end

          class Statement < Statement
            SELECT = 'SELECT %{columns} FROM %{table}'.freeze
            WHERE  = ' WHERE %s'.freeze
            ORDER  = ' ORDER BY %s'.freeze
            LIMIT  = ' LIMIT %d'.freeze

            attr_reader :bind_variables

            def initialize(table, fields, conditions, order, limit)
              @table          = table
              @columns        = list(fields)
              @conditions     = conditions
              @order          = []
              @where          = []
              @bind_variables = []
              visit_conditions
#              visit_order(order) if matches_key?

              # Only set the limit if the order is defined, otherwise we must
              # retrieve all rows and sort/limit in-memory.
              @limit = limit unless @order.empty?
            end

            def to_s
              statement = SELECT % { columns: @columns, table: @table }
              statement << WHERE % join(@where) unless @where.empty?
              statement << ORDER % list(@order) unless @order.empty?
              statement << LIMIT % @limit       unless @limit.nil?
              statement
            end

          private

            def visit_conditions(conditions = @conditions)
              case conditions
              when Query::Conditions::AndOperation
                visit_conjunction(conditions)
              when Query::Conditions::AbstractComparison
                visit_comparison(conditions)
              else
                # Skip unknown conditions
              end
            end

            def visit_conjunction(conjunction)
              head, *tail = conjunction.to_a
              visit_conditions(head)
              tail.each do |operand|
                @where << SPACE
                @where << AND
                @where << SPACE
                visit_conditions(operand)
              end
            end

            def visit_comparison(comparison)
              if comparison.relationship?
                visit_conditions(comparison.foreign_key_mapping)
              else
                method = "visit_#{comparison.slug}"
                return unless respond_to?(method, true)
                send(method, comparison.subject, comparison.value)
              end
            end

            def visit_eql(*args)
              visit_binary_relation(*args, EQUALS_SIGN)
            end

            def visit_gt(*args)
              return unless matches_key?
              visit_binary_relation(*args, GREATER_THAN_SIGN)
            end

            def visit_lt(*args)
              return unless matches_key?
              visit_binary_relation(*args, LESS_THAN_SIGN)
            end

            def visit_gte(*args)
              return unless matches_key?
              visit_binary_relation(*args, GREATER_THAN_OR_EQUAL_TO)
            end

            def visit_lte(*args)
              return unless matches_key?
              visit_binary_relation(*args, LESS_THAN_OR_EQUAL_TO)
            end

            def visit_binary_relation(subject, value, relation)
              @where          << field(subject) << relation << PLACEHOLDER
              @bind_variables << value
            end

            def visit_in(subject, value)
              return unless subject.key?
              @where          << field(subject) << IN << parenthesis(PLACEHOLDER)
              @bind_variables << value
            end

            def visit_order(order)
              @order = order.map do |direction|
                field(direction.target).tap do |statement|
                  statement << SPACE << DESC if direction.operator == :desc
                end
              end
            end

            def matches_key?
              @conditions.kind_of?(Query::Conditions::AndOperation) &&
              @conditions.any? do |condition|
                condition.respond_to?(:subject) &&
                condition.subject.key?          &&
                [ :eql, :in ].include?(condition.slug)
              end
            end

          end # Statement
        end # Read
      end # Command
    end # CassandraAdapter
  end # Adapters
end # DataMapper
