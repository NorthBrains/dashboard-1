-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS products_data WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

-- Create 'data' table in 'products_data' keyspace
CREATE TABLE IF NOT EXISTS products_data.data(
    id int primary key);
