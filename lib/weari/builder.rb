# Author::    Erik Hetzner  (mailto:erik.hetzner@ucop.edu)
# Copyright:: Copyright (c) 2011, Regents of the University of California

require 'rubygems'
require 'nokogiri'

module Weari
  class Builder < ::Nokogiri::XML::Builder
    # Strip control characters from a string
    def strip_control(s)
      return s.gsub(/[[:cntrl:]]/, '')
    end

    def initialize(options={}, root=nil, &block)
      # fix problems with this method in superclass because we are in a
      # different namespace
      
      if root
        @doc    = root.document
        @parent = root
      else
        @doc          = Nokogiri::XML::Document.new
        @parent       = @doc
      end
      
      @context  = nil
      @arity    = nil
      @ns       = nil
      
      options.each do |k,v|
        @doc.send(:"#{k}=", v)
      end
      
      return unless block_given?
      
      @arity = block.arity
      if @arity <= 0
        @context = eval('self', block.binding)
        instance_eval(&block)
      else
        yield self
      end
      
      @parent = @doc
    end

    # sorry, method_missing hacks are stupid
    undef method_missing

    def mk(*args, &block)
      insert(@doc.create_element(*args), &block)
    end
  end
end
