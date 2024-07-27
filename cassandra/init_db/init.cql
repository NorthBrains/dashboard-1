-- Create keyspace com
CREATE KEYSPACE IF NOT EXISTS company_one WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

-- Create 'warehouse_data' table in 'company_one' keyspace
CREATE TABLE company_one.warehouse_data (
    "Warehouse ID" int,
    "Product ID" int,
    "Product Name" text,
    "Category" text,
    "Quantity in Stock" double,
    "Location" text,
    "Supplier" text,
    "Restock Frequency" text,
    "Average Delivery Time (days)" double,
    "Stock Value (USD)" double,
    "Storage Temperature (C)" double,
    "Shelf Life (days)" double,
    "Hazardous Material" text,
    "Reorder Level" double,
    "Daily Sales (units)" double,
    "Daily Restock (units)" double,
    "Stock Level (units)" int,
    "Sales Forecast (units)" int,
    "Latitude" double,
    "Longitude" double,
    "State" text,
    "timestamp" timestamp,
    PRIMARY KEY ("Warehouse ID", "Product ID")
);

-- Create 'sales_data' table in 'company_one' keyspace
CREATE TABLE company_one.sales_data (
    "Customer ID" int,
    "Age" int,
    "Gender" text,
    "Item Purchased" text,
    "Category" text,
    "Purchase Amount (USD)" double,
    "Location" text,
    "Size" text,
    "Color" text,
    "Season" text,
    "Review Rating" double,
    "Subscription Status" text,
    "Shipping Type" text,
    "Discount Applied" text,
    "Promo Code Used" text,
    "Previous Purchases" int,
    "Payment Method" text,
    "Frequency of Purchases" text,
    PRIMARY KEY ("Customer ID", "Item Purchased")
);
