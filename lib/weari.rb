# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

module Weari
  autoload :Builder, "weari/builder"
  autoload :Parser,  "weari/parser"
  autoload :Pig,     "weari/pig"
  autoload :PigJob,  "weari/pigjob"

  module Solr
    autoload :CachingEnumerable,  "weari/solr/caching_enumerable"
    autoload :DocumentEnumerable, "weari/solr/document_enumerable"
    autoload :Indexer,            "weari/solr/indexer"
    autoload :Merger,             "weari/solr/merger"
  end
end
