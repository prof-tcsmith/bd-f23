#!/bin/bash

echo "###############################"
echo "cassandra-1"
echo "###############################"
docker exec -it cassandra-1 nodetool status

echo "###############################"
echo "cassandra-2"
echo "###############################"
docker exec -it cassandra-2 nodetool status

echo "###############################"
echo "cassandra-3"
echo "###############################"
docker exec -it cassandra-3 nodetool status

