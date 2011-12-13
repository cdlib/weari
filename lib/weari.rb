# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

module Weari
  %W(Builder Parser Pig PigJob Solr).each do |n|
    autoload n.to_sym, "weari/#{n.downcase}"
  end
  
  VERSION = "0.0.1"
end
