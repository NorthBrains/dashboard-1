-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS products_data WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

-- Create 'shopping_trends' table in 'products_data' keyspace
CREATE TABLE IF NOT EXISTS shopping_trends.products_data (
    customer_id int PRIMARY KEY,
    age int,
    gender text,
    item_purchased text,
    category text,
    purchase_amount_usd float,
    location text,
    size text,
    color text,
    season text,
    review_rating float,
    subscription_status text,
    shipping_type text,
    discount_applied text,
    promo_code_used text,
    previous_purchases int,
    payment_method text,
    frequency_of_purchases text
);
