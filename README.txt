
Cassandra README
================

Cassandra is a P2P based distributed storage system 
that provides structured storage.

Cassandra is distributed under the Apache License, Version 2.0

Installation
------------
* Please use jdk 1.7; Cassandra will run with 1.6 but 
  frequently core dumps on quad-core machines
* Unpack the tar ball:
        tar xvzf cassandra-<version>.tar.gz
* cd into the new cassandra-<version> directory
* Run "ant jar"

Congratulations, Cassandra is now installed.

Setup
------
* Create the logging directory, set in conf/logging.props.
  The default directory is /var/cassandra/logs
* For information on the configuration parameters, 
  see http://code.google.com/p/the-cassandra-project/wiki/ConfReference


Starting the server
-------------------
* cd into the cassandra-<version> directory
* ensure you have write privileges on the data directories
* start the server:
        ./bin/start-server &

Ensure the server is running
----------------------------

* Check if the process exists, e.g.:
	ps -ef | grep cassandra
* Open a web browser, and point it at:
	http://<node on which the server is running>:7002
* Open up jconsole and observe the process is running.

Stopping the server
-------------------
* cd into the cassandra-<version> directory
* stop the server:
        ./bin/stop-server

