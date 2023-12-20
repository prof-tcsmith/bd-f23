#!/bin/bash
#
# cfstats is a command that provides statistics about the Cassandra column families (keyspaces).
# This script will display all the column families in the cluster.
#
# If we want to look at just one, add the keyspace name to the command, like this:
# docker exec -it cassandra-1 nodetool cfstats system_distributed
#
echo "###############################"
echo "cassandra-1"
echo "###############################"
docker exec -it cassandra-1 nodetool cfstats

echo "###############################"
echo "cassandra-2"
echo "###############################"
docker exec -it cassandra-2 nodetool cfstats

echo "###############################"
echo "cassandra-3"
echo "###############################"
docker exec -it cassandra-3 nodetool cfstats

