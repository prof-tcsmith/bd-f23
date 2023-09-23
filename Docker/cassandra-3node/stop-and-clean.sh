#1/bin/bash

docker-compose stop 

docker rm cassandra-1
docker rm cassandra-2
docker rm cassandra-3

docker rmi cassandra

rm -rf ./cassandra-node-1/*
rm -rf ./cassandra-node-2/*
rm -rf ./cassandra-node-3/*

docker container prune
docker volume prune
docker network prune