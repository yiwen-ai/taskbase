CREATE KEYSPACE IF NOT EXISTS taskbase
WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': '3' };
USE taskbase;