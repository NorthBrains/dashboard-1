-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS products_data WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

-- Create 'data' table in 'products_data' keyspace
CREATE TABLE IF NOT EXISTS products_data.data (
    record_id uuid PRIMARY KEY,
    id int,
    category text,
    subcategories text,
    product text,
    quantity int,
    timestamp text
);