# for making thrift stuff
all: ruby/lib/weari/thrift/server.rb ruby/lib/weari/thrift/weari_constants.rb thrift-java

thrift-rb: weari.thrift
	thrift --gen rb -out ruby/lib/weari/thrift weari.thrift

thrift-java: weari.thrift
	thrift --gen java -out src/main/java weari.thrift

ruby/lib/weari/thrift/server.rb: thrift-rb
	sed -i "s/^require 'weari_types'/require 'weari\/thrift\/weari_types'/" ruby/lib/weari/thrift/server.rb

ruby/lib/weari/thrift/weari_constants.rb: thrift-rb
	sed -i "s/^require 'weari_types'/require 'weari\/thrift\/weari_types'/" ruby/lib/weari/thrift/weari_constants.rb
