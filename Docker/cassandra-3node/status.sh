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

echo "###############################"
echo "cassandra-4"
echo "###############################"
docker exec -it cassandra-4 nodetool status

echo "###############################"
echo "cassandra-5"
echo "###############################"
docker exec -it cassandra-5 nodetool status
