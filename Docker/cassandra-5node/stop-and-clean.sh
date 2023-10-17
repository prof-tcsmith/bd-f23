#1/bin/bash

export INTERVAL=20s
export START_PERIOD=20s
export RETRIES=3

docker-compose stop 

docker rm cassandra-1
docker rm cassandra-2
docker rm cassandra-3
docker rm cassandra-4
docker rm cassandra-5

docker rmi cassandra

rm -rf ./cassandra-node-1/*
rm -rf ./cassandra-node-2/*
rm -rf ./cassandra-node-3/*
rm -rf ./cassandra-node-4/*
rm -rf ./cassandra-node-5/*

docker container prune
docker volume prune
docker network prune