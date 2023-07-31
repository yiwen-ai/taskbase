CREATE KEYSPACE IF NOT EXISTS taskbase_test
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };
USE taskbase_test;
