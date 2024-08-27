-- Create keyspace com
CREATE KEYSPACE IF NOT EXISTS company_one WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS company_one.warehouse_data (
    warehouse_id int,
    product_id int,
    product_name text,
    category text,
    quantity_in_stock float,
    location text,
    supplier text,
    restock_frequency text,
    average_delivery_time float,
    stock_value float,
    storage_temperature float,
    shelf_life float,
    hazardous_material text,
    reorder_level float,
    daily_sales float,
    daily_restock float,
    stock_level int,
    sales_forecast int,
    latitude float,
    longitude float,
    state text,
    timestamp text,
    PRIMARY KEY (warehouse_id, product_id)
);

CREATE TABLE IF NOT EXISTS company_one.sales_data (
    customer_id int,
    product_id int,
    age int,
    gender text,
    item_purchased text,
    category text,
    purchase_amount float,
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
    frequency_of_purchases text,
    timestamp text,
    PRIMARY KEY (customer_id, item_purchased)
);

