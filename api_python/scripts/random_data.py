from confluent_kafka import Producer
import random
from datetime import datetime
import json
import time

products = [
    {"category": "Beverages", "subcategories": [{"name": "Juices", "products": [{"name": "Orange Juice", "id": 1}, {"name": "Apple Juice", "id": 2}, {"name": "Multi-Vegetable Juice", "id": 3}]}, {"name": "Water", "products": [{"name": "Mineral Water", "id": 4}, {"name": "Spring Water", "id": 5}, {"name": "Carbonated Water", "id": 6}]}]},
    {"category": "Fruits and Vegetables", "subcategories": [{"name": "Fruits", "products": [{"name": "Apples", "id": 7}, {"name": "Bananas", "id": 8}, {"name": "Oranges", "id": 9}]}, {"name": "Vegetables", "products": [{"name": "Tomatoes", "id": 10}, {"name": "Cucumbers", "id": 11}, {"name": "Peppers", "id": 12}]}]},
    {"category": "Meat and Fish", "subcategories": [{"name": "Meat", "products": [{"name": "Chicken", "id": 13}, {"name": "Beef", "id": 14}, {"name": "Pork", "id": 15}]}, {"name": "Fish", "products": [{"name": "Salmon", "id": 16}, {"name": "Trout", "id": 17}, {"name": "Mackerel", "id": 18}]}]},
    {"category": "Dairy Products", "subcategories": [{"name": "Milk and Yogurts", "products": [{"name": "Milk", "id": 19}, {"name": "Plain Yogurt", "id": 20}, {"name": "Fruit Yogurt", "id": 21}]}, {"name": "Cheeses", "products": [{"name": "Yellow Cheese", "id": 22}, {"name": "White Cheese", "id": 23}, {"name": "Blue Cheese", "id": 24}]}]}
]

def create_producer():
    conf = {'bootstrap.servers': "172.30.0.5:9092"}
    producer = Producer(**conf)
    return producer

producer = create_producer()

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def generate_messages(products):
    enriched_products = []
    for category in products:
        for subcategory in category["subcategories"]:
            for product in subcategory["products"]:
                enriched_product = {
                    "category": category["category"],
                    "subcategories": subcategory["name"],
                    "product": product["name"],
                    "id": product["id"],
                    "quantity": random.randint(10, 100),
                    "timestamp": datetime.now().isoformat()
                }
                enriched_products.append(enriched_product)
                producer.produce('products_data', key=str(product['id']), value=json.dumps(enriched_product), callback=delivery_report)
                producer.poll(0)
    producer.flush()
    return enriched_products

while True:
    enriched_products = generate_messages(products)
    print("Batch of products processed.")
    time.sleep(3)  