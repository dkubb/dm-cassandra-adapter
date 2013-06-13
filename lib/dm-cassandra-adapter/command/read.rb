# encoding: utf-8

module DataMapper
  module Adapters
    class CassandraAdapter < AbstractAdapter
      class Command

        # Read record in Cassandra
        class Read < self
          include Enumerable

          def initialize(adapter, query)
            @adapter = adapter
            @query   = query
            @table   = query.model.storage_name(@adapter.name)
          end

          def each(&block)
            return to_enum unless block
            statement = self.statement
            rows = @adapter.select(statement.to_s, *statement.bind_variables)
            rows.each(&block)
            self
          end

        protected

          def statement
            fields     = @query.fields.map(&:field)
            conditions = @query.conditions
            order      = @query.order
            limit      = @query.limit if @query.offset.zero?
            Statement.new(@table, fields, conditions, order, limit)
          end

          class Statement
            SELECT        = 'SELECT %{columns} FROM %{table}'.freeze
            WHERE         = 'WHERE %s'.freeze
            ORDER         = 'ORDER BY %s'.freeze
            LIMIT         = 'LIMIT %d'.freeze
            AND           = 'AND'.freeze
            L_PARENTHESIS = '('.freeze
            R_PARENTHESIS = ')'.freeze
            SEPARATOR     = ', '.freeze
            SPACE         = ' '.freeze
            PLACEHOLDER   = '?'.freeze

            attr_reader :bind_variables

            def initialize(table, fields, conditions, order, limit)
              @table          = table 
              @columns        = fields.join(SEPARATOR)
              @conditions     = conditions
              @where          = []
              @bind_variables = []
              visit_conditions
#              visit_order(order)

              # Only set the limit if the order is defined, otherwise we must
              # retrieve all rows and sort/limit in-memory.
#              @limit = limit if @order
            end

            def to_s
              statement = [ SELECT % { columns: @columns, table: @table } ]
              statement << WHERE % @where.join(SPACE)     if @where.any?
              statement << ORDER % @order.join(SEPARATOR) if @order
              statement << LIMIT % @limit                 if @limit
              statement.join(' ')
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
                @where << AND
                @where << L_PARENTHESIS
                visit_conditions(operand)
                @where << R_PARENTHESIS
              end
            end

            def visit_comparison(comparison)
              method = "visit_#{comparison.slug}"
              return unless respond_to?(method, true)
              send(method, comparison.subject, comparison.value)
            end

            def visit_eql(*args)
              visit_binary_relation(*args, '=')
            end

            def visit_gt(*args)
              visit_binary_relation(*args, '>') if equals_key_condition?
            end

            def visit_lt(*args)
              visit_binary_relation(*args, '<') if equals_key_condition?
            end

            def visit_gte(*args)
              visit_binary_relation(*args, '>=') if equals_key_condition?
            end

            def visit_lte(*args)
              visit_binary_relation(*args, '<=') if equals_key_condition?
            end

            def visit_binary_relation(subject, value, relation)
              @where          << subject.field << relation << PLACEHOLDER
              @bind_variables << value 
            end

            def visit_in(subject, value)
              return unless subject.key?
              @where          << subject.field << 'IN' << "(#{PLACEHOLDER})"
              @bind_variables << value 
            end

            def visit_order(order)
              @order = if equals_key_condition?
                order.map do |direction|
                  "#{direction.target.field} #{direction.operator.to_s.upcase}"
                end
              end
            end

            def equals_key_condition?
              @conditions.kind_of?(Query::Conditions::AndOperation) &&
              @conditions.any? do |condition|
                condition.respond_to?(:subject) &&
                condition.subject.key?          &&
                [ :eql, :in ].include?(condition.slug)
              end
            end

          end # Statement
        end # Read
      end # Statement
    end # CassandraAdapter
  end # Adapters
end # DataMapper
