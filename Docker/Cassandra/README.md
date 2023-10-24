# Cassandra 3-Node Cluster 

Prior to starting the cluster you must create 6 folders. 
cassandra-node-1
cassandra-node-2
cassandra-node-3
data1
data2
data3

The cassandra node folders are to make the Cassandra database persistent. The Data folders are shared with the containers, and thus will allow you to share files between the host system and the cassandra nodes.

To start the cluster, run the following command:
```bash start.sh```

To stop the cluser, run the folloowing command (be sure you stop the cluster after your session):
```bash stop.sh```

If your cluster becomes corrupted (due to starting it twice without shutting down, or other potential issues), the following command will stop and reset all our cluster folders.
```bash stop-and-clean.sh```