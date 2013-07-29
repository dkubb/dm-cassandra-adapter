# encoding: utf-8

source 'https://rubygems.org'

gemspec

gem 'ciql',          :git => 'https://github.com/Nulu/ciql.git'
gem 'cassandra-cql', :git => 'https://github.com/kreynolds/cassandra-cql.git'
gem 'dm-core',       :git => 'https://github.com/datamapper/dm-core.git', :branch => 'release-1.2'

group :development, :test do
  gem 'devtools', git: 'https://github.com/rom-rb/devtools.git'
end

eval_gemfile 'Gemfile.devtools'
