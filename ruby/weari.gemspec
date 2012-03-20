# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)

Gem::Specification.new do |s|
  s.name        = "weari"
  s.version     = "0.0.1"
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Erik Hetzner"]
  s.email       = ["erik.hetzner@ucop.edu"]
  s.homepage    = ""
  s.summary     = %q{WEb ARchive Indexer}

  s.add_dependency "rsolr", ">=1.0.6"

  s.rubyforge_project = "weari"

  s.files         = `hg locate "ruby/**"`.split("\n")
  s.test_files    = `hg locate "ruby/**" --include '{spec,features}'`.split("\n")
  s.executables   = `hg locate "ruby/**" --include bin`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end
