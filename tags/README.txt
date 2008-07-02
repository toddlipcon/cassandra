
Cassandra README
================

Cassandra is a P2P based distributed storage system
that provides structured storage.

Cassandra is distributed under the Apache License, Version 2.0

Installation
------------
* Unpack the tar ball in the misc directory the /var directory:
        cd /var; tar jxvf cassandra.tar.bz2
* The scripts expect jdk 1.7 in /usr/local. This is also provided
  in the misc directory:
        tar jxvf misc/jdk1.7-drop.tar.bz2 /usr/local/
* When you build, make sure to use the java 1.7 compiler
Congratulations, Cassandra is now installed.

Setup
------
* cd into the cassandra directory
        cd /var/cassandra
* The default data directories are in /mnt/d1, mnt/d2, mnt/d3. The
  commitlog directory is /mnt/d4/commitlog. This is assumed in
  the conf file provided. If you dont care to change the conf do
  the following:
        mkdir -p /mnt/d1
        mkdir -p /mnt/d2/logs
        mkdir /mnt/d3
        mkdir /mnt/d4
* Edit conf/storage-conf.xml and set meaningful values for the following:

   |  XML Tag           |      Default             |           Comments
---------------------------------------------------------------------------------------
 1 | ClusterName        | CASSANDRA INSTANCE       | Cluster name used for membership
 2 | GangliaServer      | GANGLIA.SERVER.NAME:PORT | Ganglis reporting, remove if unused
 3 | ColumnFamily       | SIMPLECOLUMNFAMILY       | Sample simple column family, add/remove as needed
 4 | ColumnFamily       | SUPERCOLUMNFAMILY        | Sample sure column family, add/remove as needed
 5 | Seed               | SEED.MACHINE.1           | Seed node from cluster, edit/remove as needed
 6 | Seed               | SEED.MACHINE.2           | Seed node from cluster, edit/remove as needed

Optional setup:

 7 | DataFileDirectory  | /mnt/d1/data             | Data directory, edit/reomve as needed
 8 | DataFileDirectory  | /mnt/d2/data             | Data directory, edit/reomve as needed
 9 | DataFileDirectory  | /mnt/d3/data             | Data directory, edit/reomve as needed
10 | DataFileDirectory  | /mnt/d3/bootstrap        | Bootstrap directory
11 | DataFileDirectory  | /mnt/d3/staging          | Staging directory
12 | DataFileDirectory  | /mnt/d4/commitlog        | commitlog directory


Starting the server
-------------------
All you have to do is:
cd /var/cassandra
./bin/start-server&

To check if the server is started, there are 2 ways:

* Check if the process exists:
	ps aux | grep peer
* Open firefox, and point it at:
	http://<node on which the server is running>:7002
* Open up jconsole and observe the process is running.

Stopping the server
-------------------
cd /var/cassandra
./bin/stop-server

