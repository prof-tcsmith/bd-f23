#!/bin/bash

echo "###############################"
echo "cassandra-1"
echo "###############################"
docker exec -it cassandra-1 cqlsh

echo "###############################"
echo "cassandra-2"
echo "###############################"
docker exec -it cassandra-2 cqlsh

echo "###############################"
echo "cassandra-3"
echo "###############################"
docker exec -it cassandra-3 cqlsh

echo "###############################"
echo "cassandra-4"
echo "###############################"
docker exec -it cassandra-4 cqlsh

echo "###############################"
echo "cassandra-5"
echo "###############################"
docker exec -it cassandra-5 cqlsh

