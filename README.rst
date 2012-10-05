==============================
 weari: a WEb ARchive Indexer
==============================

Build
-----

weari is built with sbt and maven. Tests are run in sbt, builds happen
in maven.

Running
-------

First, you will need a hadoop cluster.

After building (in maven) you should have a
``target/weari-0.1-SNAPSHOT-bin.zip`` file. Unzip this.

You need to ensure that the ``weari`` script in the directory you just
unpacked points at your hadoop conf directory. Check the script.

To start, run::

  sh weari start

This will start weari in the background, writing to the ``weari.log``
file.

You can use the ``weari-client`` script to manually



Rebalancing
-----------

Upon adding a solr shard to your cluster, you may find that you need
to “rebalance” your cluster.

Glossary
--------

merge group: a collection of documents which web resources should be
merged into, eliminating duplicates. Each merge group should be stored
on a single solr shard.
