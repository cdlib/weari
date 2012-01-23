# -*- ruby -*-
require 'rake/testtask'
require 'rdoc/task'

require 'bundler'
include Rake::DSL
Bundler::GemHelper.install_tasks

task :default => [:test]
Rake::TestTask.new(:test) do |t|
  t.libs << 'lib' << 'test'
  t.pattern = 'test/**/test_*.rb'
  t.verbose = true
end

RDoc::Task.new do |rd|
  rd.title = "Web ARchiving Indexing - Ruby"
  rd.rdoc_files.include("README", "lib/**/*.rb")

  rd.options += ['-f', 'darkfish',]
end
