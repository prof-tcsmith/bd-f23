DROP KEYSPACE mid;

CREATE KEYSPACE mid WITH REPLICATION = { 
		  'class' : 'SimpleStrategy', 
		  'replication_factor' : 3 
}; 