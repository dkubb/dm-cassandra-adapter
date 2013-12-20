# encoding: utf-8

Gem::Specification.new do |gem|
  gem.name        = 'dm-cassandra-adapter'
  gem.version     = '0.0.1'
  gem.authors     = ['Dan Kubb']
  gem.email       = 'dan.kubb@gmail.com'
  gem.description = 'Cassandra DataMapper Adapter'
  gem.summary     = gem.description
  gem.homepage    = 'https://github.com/dkubb/dm-cassandra-adapter'
  gem.licenses    = %w[MIT]

  gem.require_paths    = %w[lib]
  gem.files            = `git ls-files`.split($/)
  gem.test_files       = `git ls-files -- spec/{unit,integration}`.split($/)
  gem.extra_rdoc_files = %w[LICENSE README.md CONTRIBUTING.md TODO]

  gem.required_ruby_version = '>= 1.9.3'

  gem.add_runtime_dependency('ciql',            '~> 0.3.1')
  gem.add_runtime_dependency('connection_pool', '~> 1.2.0')
  gem.add_runtime_dependency('dm-core',         '~> 1.2.1')
  gem.add_runtime_dependency('simple_uuid',     '~> 0.3')

  gem.add_development_dependency('bundler', '~> 1.3', '>= 1.3.5')
end
