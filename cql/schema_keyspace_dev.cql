CREATE KEYSPACE IF NOT EXISTS taskbase
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };
USE taskbase;