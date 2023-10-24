#!/bin/bash

docker container stop cassandra-1
docker container stop cassandra-2
docker container stop cassandra-3

docker container rm cassandra-1
docker container rm cassandra-2
docker container rm cassandra-3

docker volume rm cassandra-3node_cassandra-node-1
docker volume rm cassandra-3node_cassandra-node-2
docker volume rm cassandra-3node_cassandra-node-3
docker network rm cassandra-net

rm -rf ./cassandra-node-1/*
rm -rf ./cassandra-node-2/*
rm -rf ./cassandra-node-3/*

docker rmi cassandra

